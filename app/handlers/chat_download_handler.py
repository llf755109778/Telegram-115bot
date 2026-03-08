import asyncio
import os
import re
import json
import time
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

import init

# --- 存储和配置 ---
CONFIG_FILE = "/config/sync_config.json"
caption_cache = {}
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
            # 1. 命名与路径预设
            chat_tag = sanitize_filename(str(target_chat).replace("-100", ""))
            clean_cap = sanitize_filename(msg.text or "") or "Untitled"
            ext = get_ext(msg)
            file_name = f"[{chat_tag}]_{clean_cap}_{msg.id}{ext}"
            local_path = os.path.join(init.TEMP, file_name)

            # 发送状态初始化消息
            file_size_mb = round(msg.file.size / 1024 / 1024, 2) if msg.file else 0
            status_header = f"🎬 **任务开始**\n\n📢 来源: `{chat_tag}`\n📄 文件: `{file_name}`\n📦 大小: `{file_size_mb} MB`"
            status_msg = await bot.send_message(chat_id=user_chat_id, text=status_header, parse_mode="Markdown")

            # 2. 定义带频率控制的下载进度回调
            async def progress_callback(current, total):
                nonlocal last_update_time
                now = time.time()
                # 核心逻辑：每 10 秒更新一次回显
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

            # 3. 开始下载
            init.logger.info(f"开始搬运消息 {msg.id} -> {file_name}")
            await init.tg_user_client.download_media(msg, file=local_path, progress_callback=progress_callback)

            # 4. 同步至 115
            await status_msg.edit_text(f"{status_header}\n\n✅ 下载完成！正在同步到 115 网盘...", parse_mode="Markdown")

            date_folder = msg.date.strftime("%Y-%m")
            remote_target = f"/AV/Telegram_Sync/{chat_tag}/{date_folder}"

            success, result = await process_upload(local_path, remote_target)

            # 5. 结果结算
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
                init.logger.info(f"搬运完成: {file_name}，耗时 {total_duration}s")
            else:
                await status_msg.edit_text(f"❌ **115 上传失败**\n文件: `{file_name}`\n错误: {result}")

        except Exception as e:
            init.logger.error(f"Worker 异常: {str(e)}")
            await bot.send_message(chat_id=user_chat_id, text=f"⚠️ 搬运 ID {msg.id} 时出错: {str(e)}")
        finally:
            download_queue.task_done()
            await asyncio.sleep(2)  # 给系统留点喘息时间


# --- 异步上传到 115 ---
async def process_upload(file_path, save_dir):
    loop = asyncio.get_running_loop()

    def sync_task():
        try:
            file_name = Path(file_path).name
            sha1 = init.openapi_115.calculate_sha1(file_path)
            init.openapi_115.create_dir_recursive(save_dir)
            res = init.openapi_115.upload_file(
                target=save_dir,
                file_name=file_name,
                file_size=os.path.getsize(file_path),
                fileid=sha1,
                file_path=file_path,
                request_times=1
            )
            return True, res
        except Exception as e:
            return False, str(e)

    return await loop.run_in_executor(None, sync_task)


# --- 指令入口 ---
async def chatDown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "❌ 请输入频道链接或 ID。\n用法: `/chatDown https://t.me/xxx` 或 `/chatDown -100xxx`")
        return

    target_chat = context.args[0]
    msg_status = await update.message.reply_text("🔍 正在扫描新资源并准备加入队列...")

    last_id = load_progress()
    count = 0

    try:
        if not init.tg_user_client.is_connected():
            await init.tg_user_client.connect()

        async for msg in init.tg_user_client.iter_messages(target_chat, min_id=last_id):
            if msg.photo or msg.video or msg.document:
                # 放入队列
                await download_queue.put({
                    'msg': msg,
                    'target_chat': target_chat,
                    'chat_id': update.effective_chat.id
                })
                count += 1

        if count > 0:
            await msg_status.edit_text(
                f"✅ 扫描完成！\n已将 `{count}` 个新媒体加入后台队列。\n我会逐个处理并在此显示详细进度。")
        else:
            await msg_status.edit_text(f"☕ 频道已经是最新的了（上次进度: {last_id}）。")

    except Exception as e:
        init.logger.error(f"扫描频道出错: {e}")
        await msg_status.edit_text(f"❌ 扫描频道失败: {e}")


# --- 注册函数 ---
def register_chatDown_handlers(application):
    application.add_handler(CommandHandler("chatDown", chatDown))

    # 启动后台协程，并传入 bot 实例
    asyncio.get_event_loop().create_task(download_worker(application.bot))

    if hasattr(init, 'logger') and init.logger:
        init.logger.info("✅ chatDown 异步回显系统已就绪")