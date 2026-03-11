import asyncio
import os
import logging
from telethon import TelegramClient, utils, functions, errors
from telethon.sessions import StringSession
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputFileLocation, InputDocumentFileLocation, upload

logger = logging.getLogger(__name__)


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
        part_size = 1024 * 1024

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
        message = await client.get_messages(message.peer_id, ids=message.id)
        return await client.download_media(message, file=file_path, progress_callback=progress_callback)
