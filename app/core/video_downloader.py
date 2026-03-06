# -*- coding: utf-8 -*-
import asyncio
import os
import hashlib
import math
import re
from datetime import datetime
from pathlib import Path
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import init
from app.utils.fast_telethon import download_file_parallel

class VideoDownloadManager:
    def __init__(self):
        # 任务队列
        self.queue = asyncio.Queue()
        # 正在进行的任务 {task_id: task_info}
        self.active_tasks = {}
        # 最大并发数
        self.max_concurrent_tasks = 2
        # 当前并发数
        self.current_tasks = 0
        # 任务锁
        self.lock = asyncio.Lock()

    def get_queue_status(self):
        """获取当前队列状态摘要"""
        # 获取正在运行的任务名
        active_list = [task['file_name'] for task in self.active_tasks.values()]

        # 获取排队中的任务数量 (asyncio.Queue 不直接支持遍历，但可以获取大小)
        waiting_count = self.queue.qsize()

        return active_list, waiting_count
    async def add_task(self, task_info):
        """添加下载任务"""
        await self.queue.put(task_info)
        init.logger.info(f"任务已添加到队列: {task_info['file_name']}")
        # 尝试启动任务处理循环（如果尚未启动）
        asyncio.create_task(self._process_queue())

    async def cancel_task(self, task_id):
        """取消任务"""
        async with self.lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                task['cancel_event'].set()
                init.logger.info(f"正在取消任务: {task_id}")
                return True
        return False

    async def _process_queue(self):
        """处理队列中的任务"""
        while True:
            async with self.lock:
                # init.logger.info(f"_process_queue 正在运行的任务：{self.current_tasks}")
                if self.current_tasks >= self.max_concurrent_tasks:
                    # 达到最大并发数，等待
                    break
                
                if self.queue.empty():
                    # 队列为空，退出循环
                    break
                
                # 获取下一个任务
                task_info = await self.queue.get()
                self.current_tasks += 1
                # init.logger.info(f"_process_queue  self.current_tasks += 1 正在运行的任务：{self.current_tasks}")
                self.active_tasks[task_info['task_id']] = task_info
                
            # 启动任务
            # init.logger.info(f"_process_queue**********asyncio.create_task(self._run_task(task_info))")
            asyncio.create_task(self._run_task(task_info))

    async def _run_task(self, task_info):
        """执行单个下载任务"""
        task_id = task_info['task_id']
        file_name = task_info['file_name']
        # init.logger.info(f"_run_task 下载 {task_id}————{file_name}")

        try:

            file_size = task_info['file_size']
            save_path = task_info['save_path']
            message = task_info['message']
            context = task_info['context']
            chat_id = task_info['chat_id']
            message_id = task_info['message_id']

            temp_file_path = f"{init.TEMP}/{file_name}"
            cancel_event = asyncio.Event()
            task_info['cancel_event'] = cancel_event
            # 更新状态：开始下载
            await self._update_status(context, chat_id, message_id, 
                                    f"⬇️ 正在下载: {file_name}\n等待队列...", 
                                    task_id, show_cancel=True)

            # 进度回调
            last_update_time = datetime.now()
            
            async def progress_callback(current, total):
                nonlocal last_update_time
                if cancel_event.is_set():
                    raise asyncio.CancelledError("用户取消下载")
                
                now = datetime.now()
                if (now - last_update_time).total_seconds() >= 4 * video_manager.max_concurrent_tasks + 4:
                    percentage = (current / total) * 100 if total > 0 else 0
                    progress_bar = self._create_progress_bar(percentage)
                    text = (f"⬇️ 正在下载: {file_name}\n"
                           f"📊 进度: {progress_bar}\n"
                           f"📦 大小: {self._format_size(current)} / {self._format_size(total)}")
                    await self._update_status(context, chat_id, message_id, text, task_id, show_cancel=True)
                    last_update_time = now

            # 执行下载
            saved_path = await download_file_parallel(
                init.tg_user_client,
                message,
                file_path=temp_file_path,
                progress_callback=progress_callback,
                threads=8,
                cancel_event=cancel_event
            )
            # init.logger.info(f"_run_task 下载完成 {task_id}————{file_name}")

            if not saved_path:
                if cancel_event.is_set():
                    raise asyncio.CancelledError("用户取消下载")
                raise Exception("下载失败")

            # 格式转换与重命名
            if cancel_event.is_set():
                raise asyncio.CancelledError("用户取消下载")
                
            await self._update_status(context, chat_id, message_id, "🔄 正在处理文件...", task_id)
            final_path = self._process_file(saved_path)
            
            # 上传到115
            if cancel_event.is_set():
                raise asyncio.CancelledError("用户取消下载")

            await self._update_status(context, chat_id, message_id, f"☁️ 正在上传到115: {Path(final_path).name}", task_id)
            await self._upload_to_115(final_path, save_path, context, chat_id, message_id, task_id)
            # init.logger.info(f"_run_task 上传完成 {task_id}————{file_name}")

        except asyncio.CancelledError:
            init.logger.info(f"任务 {task_id} 已取消")
            await self._update_status(context, chat_id, message_id, "🛑 下载已取消", task_id, show_cancel=False)
            self._cleanup(temp_file_path)
        except Exception as e:
            init.logger.error(f"任务失败 {task_id}: {e}")
            await self._update_status(context, chat_id, message_id, f"❌ 失败: {str(e)}", task_id, show_cancel=False)
            self._cleanup(temp_file_path)
        finally:
            async with self.lock:
                # init.logger.info(f"_run_task current_tasks {self.current_tasks}")
                self.current_tasks -= 1
                # init.logger.info(f"_run_task current_tasks--- {self.current_tasks}")

                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]
            # 继续处理队列
            asyncio.create_task(self._process_queue())

    @staticmethod
    def is_date_directory(save_dir):
        if not save_dir:
            return False

        # 提取最后一层目录名
        # 例如: /home/user/2024-05 -> last_part = "2024-05"
        last_part = Path(save_dir).name

        # 正则解释：
        # ^\d{4}            : 以4位数字开头（年份）
        # ([-_ /]?\d{2})?   : 可选的月份（前面带可选的分隔符 - _ / 或空格）
        # ([-_ /]?\d{2})?   : 可选的日期
        # $                 : 结尾
        date_pattern = r"^\d{4}([-_ /]?\d{2})?([-_ /]?\d{2})?$"

        if re.match(date_pattern, last_part):
            return True
        return False

    async def _upload_to_115(self, file_path, save_dir: str, context, chat_id, message_id, task_id):
        """上传文件到115"""
        try:
            file_size = os.path.getsize(file_path)
            file_name = Path(file_path).name
            current_date = datetime.now().strftime("%Y%m%d")
            if save_dir and not self.is_date_directory(save_dir):
                # 确保路径拼接正确（处理末尾是否有斜杠的情况）
                save_dir = str(Path(save_dir) / current_date)
            elif not save_dir:
                save_dir = str(Path('/AV/短片') / current_date)

            loop = asyncio.get_running_loop()

            def sync_task():
                sha1 = self._calculate_sha1(file_path)  # 假设这是同步计算
                init.openapi_115.create_dir_recursive(save_dir)  # 假设这是同步请求

                # 确保目录存在
                init.openapi_115.create_dir_recursive(save_dir)
                return sha1
            sha1 = await loop.run_in_executor(None, sync_task)

            # 上传
            is_upload, bingo = await loop.run_in_executor(
                None,
                    lambda: init.openapi_115.upload_file(
                    target=save_dir,
                    file_name=file_name,
                    file_size=file_size,
                    fileid=sha1,
                    file_path=file_path,
                    request_times=1
                )
            )
            if is_upload:
                status = "⚡ 秒传成功" if bingo else "✅ 上传成功"
                text = (f"{status}\n"
                       f"📄 文件: {file_name}\n"
                       f"📂 目录: {save_dir}")
                await self._update_status(context, chat_id, message_id, text, task_id, show_cancel=False)
            else:
                await self._update_status(context, chat_id, message_id, "❌ 上传失败", task_id, show_cancel=False)

        finally:
            self._cleanup(file_path)

    def _process_file(self, file_path):
        """处理文件格式"""
        format_name = self._detect_video_format(file_path)
        new_path = file_path[:-3] + format_name
        if file_path != new_path:
            Path(file_path).rename(new_path)
            return new_path
        return file_path

    def _cleanup(self, file_path):
        """清理临时文件"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            init.logger.warn(f"清理文件失败: {e}")

    async def _update_status(self, context, chat_id, message_id, text, task_id, show_cancel=False):
        """更新消息状态"""
        try:
            reply_markup = None
            if show_cancel:
                # 使用 v_cancel_ 前缀避免与其他处理器的 cancel_ 冲突
                keyboard = [[InlineKeyboardButton("❌ 取消下载", callback_data=f"v_cancel_{task_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
            
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup
            )
        except Exception as e:
            init.logger.error(f"_update_status : {e}")
            pass

    def _format_size(self, size):
        if size == 0: return "0 B"
        names = ["B", "KB", "MB", "GB", "TB"]
        i = int(math.floor(math.log(size, 1024)))
        p = math.pow(1024, i)
        return f"{round(size/p, 2)} {names[i]}"

    def _create_progress_bar(self, percentage):
        filled = int(percentage // 5)
        return "█" * filled + "░" * (20 - filled) + f" {percentage:.1f}%"

    def _calculate_sha1(self, file_path):
        with open(file_path, 'rb') as f:
            return hashlib.sha1(f.read()).hexdigest()

    def _detect_video_format(self, file_path):
        # 复用原有的格式检测逻辑
        try:
            with open(file_path, "rb") as f:
                header = f.read(260)
        except:
            return "mp4"
            
        if len(header) < 4: return "mp4"
        
        if len(header) >= 12 and header[4:8] == b'ftyp':
            major = header[8:12]
            if major == b'qt  ': return 'mov'
            if major.startswith(b'3g'): return '3gp'
            return 'mp4'
            
        if header.startswith(b'\x1A\x45\xDF\xA3'):
            return 'mkv'
        if header.startswith(b'RIFF') and header[8:12] == b'AVI ':
            return 'avi'
        if header.startswith(b'\x30\x26\xB2\x75\x8E\x66\xCF\x11'):
            return 'wmv'
        if header.startswith(b'FLV'):
            return 'flv'
            
        return "mp4"

# 全局单例
video_manager = VideoDownloadManager()
