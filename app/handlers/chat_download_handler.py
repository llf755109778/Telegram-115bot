import asyncio
import os
import re
import json
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

import init

# --- 存储和命名辅助 ---
CONFIG_FILE = "/config/sync_config.json"
caption_cache = {}


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

# --- 主指令函数 ---
async def chatDown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    指令回调函数：/chatDown [可选群组ID]
    """
    # 1. 初始反馈
    msg_status = await update.message.reply_text("🔎 正在初始化搬运任务...")

    # 2. 确定目标频道/群组
    # 默认使用配置中的 CHAT_ID，或者从指令参数获取
    target_chat = None
    if context.args:
        arg = context.args[0]
        # 如果是链接，Telethon 会自动处理，如果是数字 ID 则转为 int
        if arg.startswith("https://t.me/") or arg.startswith("t.me/"):
            target_chat = arg
        elif arg.startswith("-100") or arg.isdigit():
            target_chat = int(arg)
        else:
            target_chat = arg  # 处理 @username

    last_id = load_progress()
    items = []

    # 检查和建立 Telegram 用户客户端连接
    try:
        if not init.tg_user_client.is_connected():
            init.logger.info("🔄 正在验证 Telegram 用户客户端连接...")
            await init.tg_user_client.connect()

        if not await init.tg_user_client.is_user_authorized():
            return

    except Exception as e:
        init.logger.error(f"Telegram 用户客户端连接失败: {e}")
        return

    # 3. 使用 Telethon 客户端扫描新消息
    # 注意：这里的 'client' 必须是你已经 start() 过的 Telethon TelegramClient 实例
    async for msg in init.tg_user_client.iter_messages(target_chat, min_id=last_id):
        if msg.photo or msg.video or msg.document:
            items.append(msg)

    if not items:
        await msg_status.edit_text(f"☕ 已经是最新的了（上次进度: {last_id}）。")
        return

    # 反转列表：从旧到新处理，确保 Album 的第一张带文字的消息先被处理
    items.reverse()
    total = len(items)
    await msg_status.edit_text(f"📦 发现 {total} 个新媒体，开始搬运并同步到 115...")

    success_count = 0

    # 4. 循环处理
    for msg in items:
        # --- Caption 补全 ---
        group_id = msg.grouped_id
        raw_cap = msg.text or ""
        if group_id:
            if raw_cap:
                caption_cache[group_id] = sanitize_filename(raw_cap)
            else:
                raw_cap = caption_cache.get(group_id, "")

        clean_cap = sanitize_filename(raw_cap) or "Untitled"

        # --- 后缀与命名 ---
        ext = get_ext(msg) or ".mp4"
        gid_str = group_id if group_id else f"msg{msg.id}"
        file_name = f"{clean_cap}_{gid_str}_{msg.id}{ext}"
        local_path = os.path.join(init.TEMP, file_name)

        # --- 下载 ---
        # 更新机器人状态（每 5 个更新一次，避免被 Telegram 限制频率）
        if success_count % 5 == 0:
            await msg_status.edit_text(f"⏳ 正在搬运: {success_count}/{total}\n当前: {file_name[:20]}...")

        try:
            # 使用 Telethon 下载到本地临时目录
            await init.tg_user_client.download_media(msg, file=local_path)

            # --- 上传到 115 (核心逻辑) ---
            # 这个 process_upload 是你之前写的：包含 run_in_executor 那个
            # 它能保证在上传几 GB 的大文件时，主线程（机器人）依然能工作
            chat_tag = sanitize_filename(str(target_chat).replace("-100", ""))
            date_folder = msg.date.strftime("%Y-%m")
            remote_target = f"/AV/Telegram_Sync/{chat_tag}/{date_folder}"
            success, bingo = await process_upload(local_path, remote_target)

            if success:
                # 成功后：删除本地 + 更新进度
                if os.path.exists(local_path):
                    os.remove(local_path)
                save_progress(msg.id)
                success_count += 1
            else:
                await update.message.reply_text(f"⚠️ 上传 115 失败: {file_name}")

        except Exception as e:
            await update.message.reply_text(f"❌ 处理消息 {msg.id} 时出错: {str(e)}")

    # 5. 完成总结
    await msg_status.edit_text(f"✅ 同步任务圆满完成！\n共同步资源: {success_count} 个\n最新 ID 记录: {last_id}")
    caption_cache.clear()

# --- 在主程序中注册 ---
def register_chatDown_handlers(application):
    application.add_handler(CommandHandler("chatDown", chatDown))
    init.logger.info("✅ chatDown处理器已注册")



# --- 补全 process_upload (之前讨论的 115 上传逻辑) ---
async def process_upload(file_path, save_dir):
    """
    使用线程池执行 115 上传，防止阻塞机器人主线程
    """
    loop = asyncio.get_running_loop()

    # 这里的 init.openapi_115 对应你之前的 115 SDK 实例
    def sync_task():
        file_size = os.path.getsize(file_path)
        file_name = Path(file_path).name
        # 假设你的 init.openapi_115 已经初始化
        sha1 = init.openapi_115.calculate_sha1(file_path)
        init.openapi_115.create_dir_recursive(save_dir)

        return init.openapi_115.upload_file(
            target=save_dir,
            file_name=file_name,
            file_size=file_size,
            fileid=sha1,
            file_path=file_path,
            request_times=1
        )

    # 解决 process_upload 红线
    return await loop.run_in_executor(None, sync_task)