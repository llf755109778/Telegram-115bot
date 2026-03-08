import asyncio
import os
import re
import json
import time
from collections import OrderedDict
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

import init

from app.core.open_115 import calculate_sha1

# --- 存储和配置 ---
CONFIG_FILE = "/config/sync_config.json"
caption_cache = OrderedDict()
MAX_CACHE_SIZE = 10000 # 1000 个消息组通常足够了
download_queue = asyncio.Queue()


def load_progress():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f).get("last_id", 0)
        except:
            return 0
    return 0


def save_progress(last_id):
    with open(CONFIG_FILE, 'w') as f:
        json.dump({"last_id": last_id}, f)


def sanitize_filename(name):
    if not name: return ""
    # 过滤网盘和系统敏感字符
    name = re.sub(r'[\\/:*?"<>|#%&{}]', '', name)
    return name.replace('\n', ' ').strip()[:80]

def get_ext(msg):
    ext = None
    if msg.file and msg.file.ext:
        ext = msg.file.ext  # 这会自动带上点，如 .mp4
    else:
        # 2. 如果没有后缀，根据 MIME 类型猜测
        mime = msg.file.mime_type if msg.file else None
        if mime:
            import mimetypes
            ext = mimetypes.guess_extension(mime)

    # 3. 最后的兜底
    if not ext:
        ext = ".mp4" if msg.video else ".jpg"

    # 确保 ext 是带点的格式（有时候 guess_extension 返回的不稳定）
    if ext and not ext.startswith('.'):
        ext = "." + ext
    return ext


# --- 后台 Worker 进程 ---
async def download_worker(bot):
    """
    独立后台 Worker：负责从队列取任务，并处理下载/上传
    """
    init.logger.info("🚀 chatDown 后台 Worker 已就绪，等待任务中...")

    while True:
        task = await download_queue.get()
        msg = task['msg']
        target_chat = task['target_chat']
        user_chat_id = task['chat_id']

        start_time = time.time()
        last_update_time = 0  # 用于控制 10s 更新频率

        try:
            # 1. 核心：Caption 补全逻辑 (恢复 caption_cache)
            group_id = msg.grouped_id

            # 2. 构造唯一的 Key (频道ID + 组ID)，防止不同频道串词
            cache_key = f"{target_chat}_{msg.grouped_id}" if msg.grouped_id else None

            raw_cap = msg.text or ""
            if cache_key:
                if raw_cap:
                    # 只有当当前消息有文字时，才更新缓存
                    caption_cache[cache_key] = sanitize_filename(raw_cap)
                    # 将最新的 Key 移到末尾（保持 LRU 顺序）
                    caption_cache.move_to_end(cache_key)
                else:
                    # 如果当前消息没文字，尝试从缓存中提取
                    raw_cap = caption_cache.get(cache_key, "")

            clean_cap = sanitize_filename(raw_cap) or "Untitled"

            # 2. 命名与路径
            chat_tag = sanitize_filename(str(target_chat).replace("-100", ""))
            ext = get_ext(msg)
            # 文件名：[频道名]_标题_消息ID.后缀
            file_name = f"[{chat_tag}]_{clean_cap}_{msg.id}{ext}"
            local_path = os.path.join(init.TEMP, file_name)

            # 发送状态初始化消息
            file_size_mb = round(msg.file.size / 1024 / 1024, 2) if msg.file else 0
            status_header = f"🎬 **任务开始**\n\n📢 来源: `{chat_tag}`\n📄 文件: `{file_name}`\n📦 大小: `{file_size_mb} MB`"
            status_msg = await bot.send_message(chat_id=user_chat_id, text=status_header, parse_mode="Markdown")

            # 3. 频率控制的进度回调 (10s 一次)
            async def progress_callback(current, total):
                nonlocal last_update_time
                now = time.time()
                if now - last_update_time > 10:
                    percent = current / total * 100
                    speed = (current / 1024 / 1024) / (now - start_time + 0.1)  # MB/s
                    bar = f"{'█' * int(percent // 10)}{'░' * (10 - int(percent // 10))}"

                    progress_text = (
                        f"{status_header}\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📥 进度: `{bar}` {percent:.1f}%\n"
                        f"🚀 速度: `{speed:.2f} MB/s`\n"
                        f"⏰ 已耗时: `{int(now - start_time)}s`"
                    )
                    try:
                        await status_msg.edit_text(progress_text, parse_mode="Markdown")
                        last_update_time = now
                    except:
                        pass

            # 4. 下载
            init.logger.info(f"⬇️ 下载中: {file_name}")
            await init.tg_user_client.download_media(msg, file=local_path, progress_callback=progress_callback)

            # 5. 上传至 115
            await status_msg.edit_text(f"{status_header}\n\n✅ 下载完成！正在同步到 115 网盘...", parse_mode="Markdown")

            date_folder = msg.date.strftime("%Y-%m")
            remote_target = f"/AV/Telegram_Sync/{chat_tag}/{date_folder}"

            success, result = await process_upload(local_path, remote_target)

            # 6. 结果结算
            total_duration = round(time.time() - start_time, 1)
            if success:
                if os.path.exists(local_path): os.remove(local_path)
                save_progress(msg.id)
                final_text = (
                    f"✨ **同步成功！**\n\n"
                    f"📁 目录: `{remote_target}`\n"
                    f"📝 文件: `{file_name}`\n"
                    f"⏱️ 总耗时: `{total_duration}s`"
                )
                await status_msg.edit_text(final_text, parse_mode="Markdown")
                init.logger.info(f"✅ 完成: {file_name}")
            else:
                await status_msg.edit_text(f"❌ **上传失败**\n错误: {result}")

        except Exception as e:
            init.logger.error(f"❌ Worker 报错: {str(e)}")
        finally:
            download_queue.task_done()
            # 清理过期的 cache，防止内存堆积（如果 cache 太多可以加个清理机制）
            while len(caption_cache) > MAX_CACHE_SIZE:
                caption_cache.popitem(last=False)

                # 优化 2: 动态休眠
                # 如果 115 上传或 TG 下载太频繁，建议保留一小段间隔，
                # 但 1秒 太久，建议 0.1 ~ 0.2 秒即可，或者仅在连续失败时增加休眠。
            await asyncio.sleep(0.2)


# --- 异步上传封装 ---
async def process_upload(file_path, save_dir):
    loop = asyncio.get_running_loop()

    def sync_task():
        try:
            file_name = Path(file_path).name
            sha1 = calculate_sha1(file_path)
            init.openapi_115.create_dir_recursive(save_dir)
            res = init.openapi_115.upload_file(
                target=save_dir, file_name=file_name,
                file_size=os.path.getsize(file_path), fileid=sha1,
                file_path=file_path, request_times=1
            )
            return True, res
        except Exception as e:
            return False, str(e)

    return await loop.run_in_executor(None, sync_task)


# --- 指令入口 ---
async def chatDown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ 用法: `/chatDown 频道链接`")
        return

    target_chat = context.args[0]
    msg_status = await update.message.reply_text("🔍 正在扫描并加入队列...")

    last_id = load_progress()
    count = 0

    try:
        if not init.tg_user_client.is_connected():
            await init.tg_user_client.connect()

        # 扫描时保持顺序，确保 grouped_id 的第一条消息先进入队列被处理
        items = []
        async for msg in init.tg_user_client.iter_messages(target_chat, min_id=last_id):
            if msg.photo or msg.video or msg.document:
                items.append(msg)

        items.reverse()  # 从旧到新

        for m in items:
            await download_queue.put({
                'msg': m,
                'target_chat': target_chat,
                'chat_id': update.effective_chat.id
            })
            count += 1

        if count > 0:
            await msg_status.edit_text(f"✅ 已添加 `{count}` 个任务到后台队列。")
        else:
            await msg_status.edit_text(f"☕ 已经是最新进度: {last_id}")

    except Exception as e:
        await msg_status.edit_text(f"❌ 错误: {e}")


def register_chatDown_handlers(application):
    application.add_handler(CommandHandler("chatDown", chatDown))
    asyncio.get_event_loop().create_task(download_worker(application.bot))
    if hasattr(init, 'logger') and init.logger:
        init.logger.info("✅ chatDown 异步系统(含标题缓存)已就绪")