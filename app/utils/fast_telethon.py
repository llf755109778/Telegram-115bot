import asyncio
import os
import logging
from telethon import TelegramClient, utils, functions
from telethon.sessions import StringSession
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputFileLocation, InputDocumentFileLocation, upload

logger = logging.getLogger(__name__)

# --- 全局分身客户端缓存 ---
dc1_client: TelegramClient | None = None
dc1_lock = asyncio.Lock()  # 用于初始化时的线程安全

# 1. 定义各 DC 的中转 IP (这是固定资产，别写错)
DC_MAP = {
    1: "149.154.175.50",  # 美国 弗吉尼亚
    2: "149.154.167.50",  # 荷兰 阿姆斯特丹
    3: "149.154.175.100",  # 美国 迈阿密
    4: "149.154.167.91",  # 荷兰 阿姆斯特丹 (备份)
    5: "149.154.171.5",  # 新加坡 (你的主 DC)
}

# 2. 全局缓存池与锁池
proxy_clients = {}  # {dc_id: client_instance}
proxy_locks = {}  # {dc_id: asyncio.Lock()}


async def get_proxy_client(main_client, target_dc):
    """
    通用 DC 分身获取器：支持 1, 2, 3, 4, 5
    """
    global proxy_clients, proxy_locks

    if target_dc not in DC_MAP:
        logger.warning(f"⚠️ 未知的 DC ID: {target_dc}，回退到主客户端")
        return main_client

    # 3. 初始化该 DC 的专属锁（确保 5 个程序并发时不会重复创建）
    if target_dc not in proxy_locks:
        proxy_locks[target_dc] = asyncio.Lock()

    # 4. 锁外快速检测：已存在且连接中直接返回
    client = proxy_clients.get(target_dc)
    if client and client.is_connected():
        return client

    async with proxy_locks[target_dc]:
        # 5. 锁内双重检查
        client = proxy_clients.get(target_dc)
        if client and client.is_connected():
            return client

        try:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass

            logger.info(f"🚀 正在为 DC{target_dc} 创建专属加速分身 (机器: LA, 账号: DC5)...")

            # 6. 向主客户端申请该 DC 的授权（Export）
            # 注意：即便你是 DC5 账号，也可以 Export 到任何 DC
            export_auth = await main_client(functions.auth.ExportAuthorizationRequest(dc_id=target_dc))

            # 7. 创建新的分身实例
            new_client = TelegramClient(
                StringSession(),
                main_client.api_id,
                main_client.api_hash,
                # 可以在这里根据需要添加 proxy 参数
            )

            # 8. 核心：强制重定向到目标 DC 的物理 IP
            new_client.session.set_dc(target_dc, DC_MAP[target_dc], 443)
            await new_client.connect()

            # 9. 导入授权（Import）
            await new_client(functions.auth.ImportAuthorizationRequest(
                id=export_auth.id,
                bytes=export_auth.bytes
            ))

            proxy_clients[target_dc] = new_client
            logger.info(f"✅ DC{target_dc} 分身授权成功并已缓存")
            return new_client
        except Exception as e:

            logger.error(f"❌ 创建 DC{target_dc} 分身失败: {e}")
            try:

                temp_client = proxy_clients.get(target_dc)
                if temp_client:
                    # 使用 create_task 异步断开，不阻塞当前的错误返回
                    asyncio.create_task(temp_client.disconnect())
            except:
                pass
                # 关键：手动清理缓存，让下一次请求能重新触发创建逻辑
            proxy_clients[target_dc] = None
            return None


async def download_file_parallel(client: TelegramClient, message, file_path, progress_callback=None, threads=4,
                                 cancel_event=None):
    """
    使用多线程分片下载 Telegram 文件
    """
    try:
        message = await client.get_messages(message.peer_id, ids=message.id)
        media = message.media
        document = getattr(media, 'document', None)

        # 如果不是文档类型，或者文件太小（小于10MB），使用默认下载
        if not document or document.size < 10 * 1024 * 1024:
            # 默认下载不支持 cancel_event，这里简单处理
            return await client.download_media(message, file=file_path, progress_callback=progress_callback)

        file_size = document.size

        # 检查 DC ID，如果文件在不同 DC，回退到默认下载（处理跨 DC 比较复杂）
        if hasattr(document, 'dc_id') and document.dc_id != client.session.dc_id:
            if document.dc_id == 1 or document.dc_id == 3:
                dc_proxy = await get_proxy_client(client, document.dc_id)
                if dc_proxy:
                    logger.info("⚡ 检测到文件在 DC1，正在通过专线分身下载...")
                    client = dc_proxy
                    message = await client.get_messages(message.peer_id, ids=message.id)
                    await asyncio.sleep(1)
            logger.info(f"文件在 DC {document.dc_id}，当前在 DC {client.session.dc_id}，回退到单线程下载")
            return await client.download_media(message, file=file_path, progress_callback=progress_callback)

        # 获取 input_location，明确传入 document
        # input_location = utils.get_input_location(document)
        from telethon.tl.types import InputDocumentFileLocation

        input_location = InputDocumentFileLocation(
            id=document.id,
            access_hash=document.access_hash,
            file_reference=document.file_reference,
            thumb_size=''  # 下载原文件必须传空字符串
        )
        # 确保 input_location 是有效的 TLObject
        if not input_location:
            logger.warning("无法获取有效的 input_location，回退到单线程下载")
            return await client.download_media(message, file=file_path, progress_callback=progress_callback)

        # 分片大小 512KB
        part_size = 1024 * 512

        # 确保目录存在
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 初始化文件（预分配空间）
        with open(file_path, 'wb') as f:
            f.truncate(file_size)

        downloaded = 0
        progress_lock = asyncio.Lock()
        sem = asyncio.Semaphore(threads)

        # 错误标记，如果任何一个分片失败，停止所有任务
        failed = False

        async def download_chunk(offset):
            nonlocal downloaded, failed
            if failed:
                return

            # 检查取消信号
            if cancel_event and cancel_event.is_set():
                failed = True
                return

            retries = 5
            while retries > 0 and not failed:
                if cancel_event and cancel_event.is_set():
                    failed = True
                    return

                try:
                    async with sem:
                        current_part_size = part_size
                        # if offset + current_part_size > file_size:
                        #     current_part_size = file_size - offset

                        result = await client(GetFileRequest(
                            location=input_location,
                            offset=offset,
                            limit=current_part_size
                        ))
                        if isinstance(result, upload.File):
                            chunk_data = result.bytes
                        elif isinstance(result, upload.FileCdnRedirect):
                            logger.warning(f"检测到 CDN 重定向，并行下载不支持 CDN，触发回退...")
                            failed = True
                            return
                        else:
                            # 这就是报错 "expected but found something else" 的根源
                            raise TypeError(f"收到非预期 TL 对象: {type(result)}")

                        with open(file_path, 'r+b') as f:
                            f.seek(offset)
                            f.write(chunk_data)

                        async with progress_lock:
                            downloaded += len(chunk_data)
                            if progress_callback:
                                if asyncio.iscoroutinefunction(progress_callback):
                                    await progress_callback(downloaded, file_size)
                                else:
                                    progress_callback(downloaded, file_size)
                        return
                except Exception as e:
                    retries -= 1
                    if retries == 0:
                        logger.error(f"分片下载失败 offset={offset}: {e}")
                        failed = True
                        raise e
                    await asyncio.sleep(1)

        tasks = []
        for offset in range(0, file_size, part_size):
            tasks.append(asyncio.create_task(download_chunk(offset)))

        # 使用 asyncio.wait 监控任务和取消事件
        if cancel_event:
            cancel_waiter = asyncio.create_task(cancel_event.wait())
            download_future = asyncio.gather(*tasks)

            done, pending = await asyncio.wait(
                [download_future, cancel_waiter],
                return_when=asyncio.FIRST_COMPLETED
            )

            if cancel_waiter in done:
                logger.info("检测到取消信号，立即停止所有下载任务")
                # 取消所有下载任务
                download_future.cancel()
                for t in tasks:
                    if not t.done():
                        t.cancel()
                # 等待任务清理完成
                try:
                    await download_future
                except asyncio.CancelledError:
                    pass
                raise asyncio.CancelledError("用户取消下载")
            else:
                # 下载完成（或失败）
                cancel_waiter.cancel()
                await download_future
        else:
            await asyncio.gather(*tasks)

        if failed:
            if cancel_event and cancel_event.is_set():
                raise asyncio.CancelledError("用户取消下载")
            raise Exception("多线程下载中有分片失败")

        return file_path

    except asyncio.CancelledError:
        logger.info("下载已取消")
        # 确保清理文件
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        raise

    except Exception as e:
        if cancel_event and cancel_event.is_set():
            raise asyncio.CancelledError("用户取消下载")

        logger.error(f"多线程下载遇到错误: {e}，正在回退到单线程下载...")
        # 如果多线程下载失败，回退到原生下载
        # 确保文件被重置或覆盖
        return await client.download_media(message, file=file_path, progress_callback=progress_callback)
