import asyncio
import os
import logging
from telethon import TelegramClient, errors, functions
from telethon.sessions import StringSession
from telethon.tl.functions.help import GetConfigRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation
from telethon.tl.functions.auth import ImportAuthorizationRequest

logger = logging.getLogger(__name__)

# --------------------------
# DC 客户端缓存
# --------------------------
_dc_clients = {}  # dc_id -> TelegramClient
_dc_lock = asyncio.Lock()


async def get_best_dc_nodes(client):
    # 1. 获取配置
    config = await client(functions.help.GetConfigRequest())

    # 2. 定义存放结果的字典 (用 dc_id 做 key 自动去重)
    best_nodes = {}

    # 3. 遍历所有选项进行筛选
    for option in config.dc_options:
        # 过滤掉 CDN 节点（我们只需要核心 DC）
        if option.cdn:
            continue

        # 优先级逻辑：
        # 如果这个 DC 还没记录，或者当前这个是 IPv4 且之前记录的是 IPv6，则替换
        if option.id not in best_nodes:
            best_nodes[option.id] = option
        else:
            # 偏好非隐藏节点（this_port_only 为 False）且 端口为 443 的 IPv4
            current_best = best_nodes[option.id]
            if not option.ipv6 and current_best.ipv6:
                best_nodes[option.id] = option
            elif option.port == 443 and current_best.port != 443:
                best_nodes[option.id] = option

    # 4. 排序并取前五个 (DC 1, 2, 3, 4, 5)
    sorted_dcs = sorted(best_nodes.values(), key=lambda x: x.id)

    # 5. 格式化输出
    result = {}
    for dc in sorted_dcs[:5]:
        result[dc.id] = dc.ip_address
        print(f"✅ DC {dc.id}: {dc.ip_address}:{dc.port} (区域: {dc.static or 'Default'})")

    return result


# 全局变量
GLOBAL_DC_MAP = {}


async def init_dc_map(main_client):
    global GLOBAL_DC_MAP
    if not GLOBAL_DC_MAP:
        logger.info("正在获取 Telegram 官方 DC 节点列表...")
        GLOBAL_DC_MAP = await get_best_dc_nodes(main_client)


async def get_dc_client(main_client: TelegramClient, document):
    """
    为目标 DC 获取独立客户端
    """
    global GLOBAL_DC_MAP
    target_dc = document.dc_id
    async with _dc_lock:
        client = _dc_clients.get(target_dc)
        if client:
            try:
                # 快速检查，不使用 get_me()
                if client.is_connected():
                    return client
                await client.connect()
                return client
            except:
                _dc_clients.pop(target_dc, None)
                logger.error(f"正在销毁失效 DC {target_dc} ")
                client = None

        if client:
            return client

        if not GLOBAL_DC_MAP:
            GLOBAL_DC_MAP = await get_best_dc_nodes(main_client)

        logger.info(f"🚀 正在为 DC {target_dc} 创建新的分身...")

        # 创建新客户端（临时 session）
        new_client = TelegramClient(StringSession(), main_client.api_id, main_client.api_hash)

        # 强制重定向到目标 DC
        new_client.session.set_dc(target_dc, GLOBAL_DC_MAP[target_dc], 443)
        try:
            # 增加超时控制，防止死等
            await asyncio.wait_for(new_client.connect(), timeout=15)

            # 关键：导出授权
            export_auth = await main_client(functions.auth.ExportAuthorizationRequest(target_dc))
            await new_client(ImportAuthorizationRequest(id=export_auth.id, bytes=export_auth.bytes))

            # 3. 只有成功了才存入缓存
            _dc_clients[target_dc] = new_client
            logger.info(f"✨ DC {target_dc} 分身建立成功并已加入缓存")
            return new_client
        except Exception as e:
            logger.error(f"❌ 建立 DC {target_dc} 分身失败: {e}")
            # 失败了也要断开，防止资源泄漏
            await new_client.disconnect()
            raise e


# --------------------------
# 异步分片下载
# --------------------------
async def download_file_parallel(main_client: TelegramClient, message, file_path,
                                 progress_callback=None, threads=4,
                                 cancel_event=None):
    """
    多 DC + 异步分片下载文件
    """
    try:
        # 获取最新消息
        message = await main_client.get_messages(message.peer_id, ids=message.id)
        media = getattr(message, 'media', None)
        document = getattr(media, 'document', None)

        if not document:
            # 普通小文件直接下载
            return await main_client.download_media(message, file=file_path, progress_callback=progress_callback)

        # 判断是否跨 DC
        client = main_client
        if hasattr(document, 'dc_id') and document.dc_id != main_client.session.dc_id:
            client = await get_dc_client(main_client, document)
            logger.info(f"分身创建成功")
        logger.info(f"document dc_id={document.dc_id} client dc_id={client.session.dc_id}")
        file_size = document.size

        # 对于小文件直接用默认下载
        if file_size < 10 * 1024 * 1024:
            return await client.download_media(message, file=file_path, progress_callback=progress_callback)

        # 构造 InputDocumentFileLocation
        input_location = InputDocumentFileLocation(
            id=document.id,
            access_hash=document.access_hash,
            file_reference=document.file_reference,
            thumb_size=''  # 下载原文件必须传空字符串
        )

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 初始化文件（预分配空间）
        with open(file_path, 'wb') as f:
            f.truncate(file_size)

        part_size = 1024 * 1024 * 1  # 1MB
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

                        if not hasattr(result, 'bytes'):
                            logger.warning(f"收到非预期 TL 对象: {type(result)}, 回退单线程")
                            failed = True
                            return
                        await asyncio.sleep(0.02)  # 轻微限速防止 Flood
                        chunk_data = result.bytes
                        with open(file_path, 'r+b') as f:
                            f.seek(offset)
                            f.write(chunk_data[:file_size - offset])
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
                    await asyncio.sleep(1)
                    if retries == 0:
                        logger.error(f"分片下载失败 offset={offset}: {e}")
                        failed = True
                        raise e

        # 创建所有分片任务
        tasks = [asyncio.create_task(download_chunk(offset)) for offset in range(0, file_size, part_size)]

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
    except errors.AuthKeyUnregisteredError:
        # 发现授权确实失效了
        logger.error(f"⚠️ DC {client.session.dc_id} 授权已失效，正在清理缓存...")
        async with _dc_lock:
            if client.session.dc_id in _dc_clients:
                await _dc_clients[client.session.dc_id].disconnect()
                del _dc_clients[client.session.dc_id]
        return await main_client.download_media(message, file=file_path, progress_callback=progress_callback)
    except Exception as e:
        logger.error(f"下载遇到错误: {e}，回退单线程下载...")
        return await main_client.download_media(message, file=file_path, progress_callback=progress_callback)
