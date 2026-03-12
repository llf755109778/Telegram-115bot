import asyncio
import os
import random
import re
import json
import time
from collections import OrderedDict
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

import init

from app.core.open_115 import calculate_sha1
from app.utils.fast_telethon import download_file_parallel
from app.utils.utils import sanitize_filename, get_ext

# --- 存储和配置 ---
CONFIG_FILE = "/config/sync_config.json"
caption_cache = OrderedDict()
MAX_CACHE_SIZE = 50000  # 1000 个消息组通常足够了
download_queue = asyncio.Queue()


def load_config():
    """加载完整配置文件"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"channels": {}}
    return {"channels": {}}


config = load_config()


def update_channel_data(channel_id, last_id=None, new_range=None):
    # 1. 先加载完整的配置
    global config

    # 2. 确保 "channels" 节点存在
    if "channels" not in config:
        config["channels"] = {}

    # 3. 获取或初始化该频道的数据
    chid_str = str(channel_id)
    if chid_str not in config["channels"]:
        config["channels"][chid_str] = {"last_id": 0, "ranges": [], "name": ""}

    # 4. 根据传入参数修改具体字段
    if last_id is not None:
        config["channels"][chid_str]["last_id"] = last_id

    if new_range is not None:
        # 假设 new_range 是一个列表 [start, end]
        config["channels"][chid_str]["ranges"].append(new_range)

    # 5. 保存回文件
    save_progress(config)


def save_progress(config):
    """保存配置文件"""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)


def get_channel_progress(channel_id):
    """获取特定频道的进度"""
    global config
    channel_data = config.get("channels", {}).get(str(channel_id), {})
    # 返回上一次同步的 ID 和 自定义区间
    return channel_data.get("last_id", 0), channel_data.get("ranges", [])


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
            # --- 修复 1: 解决 File Reference 过期 ---
            # 下载前重新获取消息对象，刷新 File Reference
            msg = await init.tg_user_client.get_messages(target_chat, ids=msg.id)
            if not msg or not (msg.photo or msg.video or msg.document):
                init.logger.warning(f"消息 {msg.id} 已失效或不再包含媒体")
                continue

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
            # await init.tg_user_client.download_media(msg, file=local_path, progress_callback=progress_callback)

            # 执行下载
            await download_file_parallel(
                init.tg_user_client,
                msg,
                file_path=local_path,
                progress_callback=progress_callback,
                threads=2
            )
            # 5. 上传至 115
            await status_msg.edit_text(f"{status_header}\n\n✅ 下载完成！正在同步到 115 网盘...", parse_mode="Markdown")
            await asyncio.sleep(1 + 3 * random.random())  # 随机延迟 1 秒
            date_folder = msg.date.strftime("%Y-%m")
            remote_target = f"/AV/Telegram_Sync/{chat_tag}/{date_folder}"
            # 假设这部分代码在一个 async def 处理函数中
            max_retries = 45  # 可选：设置最大重试次数，如果想无限循环可以去掉
            retry_count = 0
            while retry_count <= max_retries:
                success, result = await process_upload(local_path, remote_target)

                # 6. 结果结算
                total_duration = round(time.time() - start_time, 1)
                if success:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                    update_channel_data(target_chat, msg.id, None)
                    final_text = (
                        f"✨ **同步成功！**\n\n"
                        f"📁 目录: `{remote_target}`\n"
                        f"📝 文件: `{file_name}`\n"
                        f"⏱️ 总耗时: `{total_duration}s`"
                    )
                    await status_msg.edit_text(final_text, parse_mode="Markdown")
                    await asyncio.sleep(1 + 3 * random.random())  # 随机延迟 1 秒
                    init.logger.info(f"✅ 完成: {file_name}")
                    break
                else:
                    init.logger.error(f"❌ 上传失败: {result}，准备倒计时重试")
                    await status_msg.edit_text(f"❌ **上传失败**\n 31分钟以后重试"
                                               f"📁 目录: `{remote_target}`\n"
                                               f"📝 文件: `{file_name}`\n")

                    # 倒计时 31 分钟 (31 * 60 = 1860 秒)
                    retry_seconds = 3 * 60

                    # 动态倒计时显示
                    for i in range(retry_seconds, 0, -60):  # 每分钟更新一次状态，节省 API 请求
                        minutes_left = i // 60
                        retry_msg = (
                            f"❌ **上传失败**\n"
                            f"📁 目录: `{remote_target}`\n"
                            f"📝 文件: `{file_name}`\n"
                            f"⏳ 将在 **{minutes_left}** 分钟后自动重试..."
                        )
                        try:
                            await status_msg.edit_text(retry_msg, parse_mode="Markdown")
                        except Exception:
                            pass  # 防止编辑消息太快被 Telegram 限制

                        await asyncio.sleep(60)  # 等待一分钟
                # 倒计时结束，重置开始时间并重新进入 while 循环
                start_time = time.time()
                retry_count += 1
                init.logger.info(f"🔄 开始第 {retry_count} 次重试...")

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
            is_upload, bingo = init.openapi_115.upload_file(
                target=save_dir, file_name=file_name,
                file_size=os.path.getsize(file_path), fileid=sha1,
                file_path=file_path, request_times=1
            )
            init.logger.info(f"📤 上传结果: {is_upload}, {bingo}")
            return is_upload, bingo
        except Exception as e:
            return False, False

    return await loop.run_in_executor(None, sync_task)


async def chatDown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ 用法: `/chatDown 频道链接`")
        return

    target_chat = context.args[0]
    # 1. 先发一条初始消息，拿到 msg_id
    msg_status = await update.message.reply_text("🔍 正在初始化流式扫描...")

    # 2. 【核心】启动一个后台协程去干重活，但不 await 它
    # 把所有需要的参数传进去
    asyncio.create_task(
        run_scan_and_update_status(
            target_chat,
            msg_status,
            update.effective_chat.id
        )
    )

    # 3. 主函数直接结束，机器人立刻恢复响应能力
    init.logger.info(f"已为频道 {target_chat} 开启后台扫描追踪")


async def run_scan_and_update_status(target_chat, msg_status, chat_id):
    """这个函数负责苦力活：扫描 + 更新进度"""
    count = 0
    scanned_total = 0
    global config
    last_id, ranges = get_channel_progress(target_chat)
    try:
        if not init.tg_user_client.is_connected():
            await init.tg_user_client.connect()

        # 核心改进：直接流式遍历，不存入 items 列表
        # reverse=True 表示从旧到新拉取，这样 grouped_id 的标题逻辑依然生效
        async for msg in init.tg_user_client.iter_messages(
                target_chat,
                min_id=min(last_id, max(1, last_id - 20)),
                reverse=True,  # 关键：从旧到新拉取，不需要再执行 .reverse()
                limit=None  # 虽然是 None，但它是流式获取的
        ):
            scanned_total += 1
            # 2. 构造唯一的 Key (频道ID + 组ID)，防止不同频道串词
            cache_key = f"{target_chat}_{msg.grouped_id}" if msg.grouped_id else None

            raw_cap = msg.text or ""
            if cache_key:
                if raw_cap:
                    # 只有当当前消息有文字时，才更新缓存
                    caption_cache[cache_key] = sanitize_filename(raw_cap)
                    # 将最新的 Key 移到末尾（保持 LRU 顺序）
                    caption_cache.move_to_end(cache_key)
            if msg.id <= last_id:
                continue
            if msg.photo or msg.video or msg.document:
                await download_queue.put({
                    'msg': msg,
                    'target_chat': target_chat,
                    'chat_id': chat_id
                })
                count += 1

            # 每扫描 100 条消息（无论是不是媒体），让出一次控制权，防止卡死
            if scanned_total % 100 == 0:
                await asyncio.sleep(0.01)

            # 每发现 50 个媒体，更新一次进度条，让用户爽到
            if count > 0 and count % 100 == 0:
                try:
                    # 此时 chatDown 早已结束，但这个任务依然持有 msg_status 对象
                    await msg_status.edit_text(
                        f"🚀 正在后台扫描...\n"
                        f"📂 已入队: `{count}` 个媒体\n"
                        f"📡 已扫描: `{scanned_total}` 条消息"
                    )
                except Exception:
                    pass  # 防止 Telegram 限制编辑频率导致的报错
                await asyncio.sleep(1.5)
        # 扫描结束
        await msg_status.edit_text(f"✅ 扫描完成！共计入队 `{count}` 个任务。")

    except Exception as e:
        init.logger.error(f"扫描中断: {e}")
        await msg_status.edit_text(f"❌ 扫描异常中断: {e}")


def register_chatDown_handlers(application):
    application.add_handler(CommandHandler("chatdown", chatDown))
    asyncio.get_event_loop().create_task(download_worker(application.bot))
    if hasattr(init, 'logger') and init.logger:
        init.logger.info("✅ chatDown 异步系统(含标题缓存)已就绪")
