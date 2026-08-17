#!/usr/bin/env python3
"""
Telegram Drive - Store and retrieve files via Telegram.
Drop files into telegram_drive/ -> auto-uploaded to Telegram.
Send files to the bot -> saved into telegram_drive/.

BEGILE FIX: #9290
"""

import os
import sys
import json
import time
import uuid
import logging
import threading
from pathlib import Path
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import telebot
from telebot import apihelper
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ─────────────── Settings ───────────────
BASE_DIR = Path(__file__).parent.resolve()
DRIVE_DIR = BASE_DIR / "telegram_drive"
CONFIG_FILE = BASE_DIR / "config.json"
SENT_INDEX = BASE_DIR / ".sent_files.json"
LOG_FILE = BASE_DIR / "bot.log"

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB Telegram limit

telebot.logger.setLevel(logging.CRITICAL)
apihelper.CONNECT_TIMEOUT = 30
apihelper.READ_TIMEOUT = 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("TelegramDrive")

HTTP_SESSION = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    raise_on_status=False,
)
HTTP_SESSION.mount("https://", HTTPAdapter(max_retries=retries))


def human_size(size):
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


# ═══════════════════════════════════════════════
#  Progress Bar - Single Line
# ═══════════════════════════════════════════════
def human_size(size):
    """تبدیل بایت به فرمت خوانا (KB, MB, GB)"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=30, fill='█'):
    """
    نمایش درصد پیشرفت در یک خط جدید.
    """
    if total == 0:
        percent = "100.0"
    else:
        percent = f"{100 * (iteration / float(total)):.{decimals}f}"

    sys.stdout.write(f'{prefix}: {percent}% {suffix}\n')
    sys.stdout.flush()


class ProgressFileReader:
    """کلاسی برای رصد خواندن فایل و فراخوانی نوار پیشرفت هنگام آپلود"""
    def __init__(self, file, total_size, filename):
        self.file = file
        self.total_size = total_size
        self.filename = filename
        self.uploaded = 0
        self.last_update = 0

    def read(self, size=-1):
        chunk = self.file.read(size)
        if chunk:
            self.uploaded += len(chunk)
            current_time = time.time()
            # آپدیت پروگرس بار هر 0.1 ثانیه یکبار تا از پرش بیش از حد ترمینال جلوگیری شود
            if current_time - self.last_update > 0.1 or self.uploaded == self.total_size:
                print_progress_bar(
                    self.uploaded,
                    self.total_size,
                    prefix=f"📤 {self.filename}",
                    suffix=f"({human_size(self.uploaded)}/{human_size(self.total_size)})",
                    length=25
                )
                self.last_update = current_time
        return chunk

    def seek(self, *args):
        self.file.seek(*args)
        # اگر آپلود از اول شروع شد (مثلا برای retry)، پروگرس بار را صفر کن
        if args[0] == 0:
            self.uploaded = 0
            self.last_update = 0

    def close(self):
        self.file.close()


# ═══════════════════════════════════════════════
#  Config
# ═══════════════════════════════════════════════
def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_config(token, owner_ids):
    config = {
        "bot_token": token,
        "owner_ids": owner_ids,
        "created_at": datetime.now().isoformat(),
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    log.info(f"Config saved to {CONFIG_FILE}")
    return config


def setup_config():
    print("\n" + "=" * 50)
    print("  Telegram Drive - Initial Setup")
    print("=" * 50)

    old = load_config()
    if old:
        print("\nExisting config found:")
        print(f"  Token: ...{old['bot_token'][-8:]}")
        print(f"  Owners: {old['owner_ids']}")
        choice = input("\nUse these settings? (y/n): ").strip().lower()
        if choice in ("y", "yes", ""):
            return old

    print("\nEnter your details:")
    token = input("\nBot Token: ").strip()
    if not token:
        sys.exit(1)

    ids_input = input("Authorized User IDs (comma-separated): ").strip()
    try:
        owner_ids = [int(i.strip()) for i in ids_input.split(",") if i.strip()]
    except ValueError:
        sys.exit(1)

    config = save_config(token, owner_ids)
    DRIVE_DIR.mkdir(exist_ok=True)
    return config


# ═══════════════════════════════════════════════
#  Download Tracker
# ═══════════════════════════════════════════════
class DownloadTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._recently_saved = set()
        self._timestamps = {}

    def mark_downloaded(self, filepath):
        with self._lock:
            path_str = str(Path(filepath).resolve())
            # Remove old entry so later files with same basename don't use stale timestamps
            self._recently_saved.discard(path_str)
            self._timestamps[path_str] = time.time()
            self._recently_saved.add(path_str)

    def was_downloaded(self, filepath):
        with self._lock:
            path_str = str(Path(filepath).resolve())
            if path_str in self._recently_saved:
                if time.time() - self._timestamps.get(path_str, 0) > 10:
                    self._recently_saved.discard(path_str)
                    self._timestamps.pop(path_str, None)
                    return False
                return True
            return False


# ═══════════════════════════════════════════════
#  Sent Index
# ═══════════════════════════════════════════════
class SentIndex:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self):
        if SENT_INDEX.exists():
            try:
                with open(SENT_INDEX, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}

    def _save(self):
        with open(SENT_INDEX, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)

    def is_sent(self, filepath):
        with self._lock:
            return str(Path(filepath).resolve()) in self._data

    def mark_sent(self, filepath, message_id):
        with self._lock:
            self._data[str(Path(filepath).resolve())] = {
                "message_id": message_id,
                "sent_at": datetime.now().isoformat(),
            }
            self._save()

    def remove(self, filepath):
        with self._lock:
            self._data.pop(str(Path(filepath).resolve()), None)
            self._save()

    def rename(self, old_path, new_path):
        with self._lock:
            old_res = str(Path(old_path).resolve())
            new_res = str(Path(new_path).resolve())
            if old_res in self._data:
                self._data[new_res] = self._data.pop(old_res)
                self._save()


# ═══════════════════════════════════════════════
#  Upload to Telegram
# ═══════════════════════════════════════════════
def _upload_to_telegram_with_resume(bot_token, filepath, chat_id, caption=None):
    filepath = Path(filepath)
    filename = filepath.name
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    file_size = os.path.getsize(filepath)

    read_timeout = max(60, (file_size // (1024 * 1024)) * 10)
    max_retries = 5

    log.info(f"⏳ Start uploading: {filename} ({human_size(file_size)}) - Timeout: {read_timeout}s")

    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
    headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}

    body_parts = []
    body_parts.append(f"--{boundary}\r\n".encode())
    body_parts.append(f'Content-Disposition: form-data; name="chat_id"\r\n\r\n'.encode())
    body_parts.append(f"{chat_id}\r\n".encode())

    body_parts.append(f"--{boundary}\r\n".encode())
    body_parts.append(f'Content-Disposition: form-data; name="caption"\r\n\r\n'.encode())
    body_parts.append(f"{caption or filename}\r\n".encode())

    body_parts.append(f"--{boundary}\r\n".encode())
    body_parts.append(f'Content-Disposition: form-data; name="parse_mode"\r\n\r\n'.encode())
    body_parts.append("Markdown\r\n".encode())

    body_parts.append(f"--{boundary}\r\n".encode())
    body_parts.append(f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'.encode())
    body_parts.append(b"Content-Type: application/octet-stream\r\n\r\n")

    body_end = f"\r\n--{boundary}--\r\n".encode()

    try:
        with open(filepath, "rb") as f:
            progress_reader = ProgressFileReader(f, file_size, filename)

            def data_generator():
                for part in body_parts:
                    yield part
                while True:
                    chunk = progress_reader.read(64 * 1024)
                    if not chunk:
                        break
                    yield chunk
                yield body_end

            for attempt in range(1, max_retries + 1):
                try:
                    response = HTTP_SESSION.post(url, data=data_generator(), headers=headers, timeout=(30, read_timeout))
                    result = response.json()

                    if response.status_code == 200 and result.get("ok"):
                        print_progress_bar(file_size, file_size, prefix=f"📤 {filename}", suffix="(Done)", length=25)
                        return True, result["result"]["message_id"]
                    else:
                        log.error(f"Attempt {attempt}/{max_retries} - API Error: {result.get('description')}")

                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    log.warning(f"Attempt {attempt}/{max_retries} - Network error. Retrying in {attempt*2}s...")
                    time.sleep(attempt * 2)
                except Exception as e:
                    log.error(f"Attempt {attempt}/{max_retries} - Unexpected error: {e}")
                    time.sleep(attempt * 2)

        log.error(f"❌ Failed to upload {filename} after {max_retries} retries.")
        return False, None

    except Exception as e:
        log.error(f"❌ File read error: {e}")
        return False, None


def upload_to_telegram(bot_token, filepath, chat_id, caption=None):
    return _upload_to_telegram_with_resume(bot_token, filepath, chat_id, caption)


# ═══════════════════════════════════════════════
#  Folder Watcher
# ═══════════════════════════════════════════════
class DriveWatcher(FileSystemEventHandler):
    def __init__(self, bot, token, owner_ids, sent_index, download_tracker):
        self.bot = bot
        self.token = token
        self.owner_ids = owner_ids
        self.sent_index = sent_index
        self.download_tracker = download_tracker

    def _wait_for_stable(self, path, timeout=10):
        prev_size = -1
        elapsed = 0
        while elapsed < timeout:
            try:
                curr_size = os.path.getsize(path)
                if curr_size == prev_size and curr_size > 0:
                    return True
                prev_size = curr_size
            except OSError:
                return False
            time.sleep(1)
            elapsed += 1
        return False

    def on_created(self, event):
        if not event.is_directory:
            self._upload_file(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._upload_file(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        if Path(event.dest_path).parent.resolve() == DRIVE_DIR:
            self.sent_index.rename(event.src_path, event.dest_path)
            self._upload_file(event.dest_path)

    def _upload_file(self, filepath):
        filepath = Path(filepath)

        if self.download_tracker.was_downloaded(filepath):
            log.info(f"Skip (just downloaded from TG): {filepath.name}")
            return

        if filepath.name.startswith(".") or filepath.name.endswith(".tmp"):
            return

        if self.sent_index.is_sent(filepath):
            return

        if not self._wait_for_stable(str(filepath)):
            return

        try:
            size = filepath.stat().st_size
        except OSError:
            return

        if size == 0 or size > MAX_FILE_SIZE:
            log.warning(f"Skipping {filepath.name}: Size 0 or > {MAX_FILE_SIZE//1024//1024}MB")
            return

        for owner_id in self.owner_ids:
            success, msg_id = upload_to_telegram(
                self.token,
                filepath,
                owner_id,
                caption=f"**{filepath.name}**\nSize: {human_size(size)}",
            )
            if success and msg_id:
                self.sent_index.mark_sent(filepath, msg_id)
                log.info(f"✅ Upload OK: {filepath.name}")


# ═══════════════════════════════════════════════
#  Telegram Bot
# ═══════════════════════════════════════════════
class TelegramDriveBot:
    def __init__(self, config, download_tracker):
        self.config = config
        self.token = config["bot_token"]
        self.owner_ids = set(config["owner_ids"])
        self.bot = telebot.TeleBot(self.token)
        self.sent_index = SentIndex()
        self.download_tracker = download_tracker
        self._setup_handlers()

    def _is_authorized(self, user_id):
        return user_id in self.owner_ids

    def _download_with_progress(self, url, filepath, filename):
        with HTTP_SESSION.get(url, stream=True, timeout=(20, 600)) as r:
            r.raise_for_status()
            downloaded = 0
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
        return downloaded

    def _setup_handlers(self):
        @self.bot.message_handler(commands=["start", "help"])
        def cmd_start(msg):
            if not self._is_authorized(msg.from_user.id):
                self.bot.reply_to(msg, "Access denied!")
                return
            self.bot.reply_to(
                msg,
                "Cloud **Telegram Drive** is active!\n\n"
                "Drop files here to save them\n"
                "Files in `telegram_drive/` are auto-uploaded\n\n"
                "/list /delete <name> /info",
                parse_mode="Markdown",
            )

        # [BUG] Command handlers temporarily disabled due to stability issues
        # @self.bot.message_handler(commands=["list"])
        # def cmd_list(msg):
        #     if not self._is_authorized(msg.from_user.id):
        #         return
        #     files = list(DRIVE_DIR.iterdir()) if DRIVE_DIR.exists() else []
        #     files = [f for f in files if f.is_file() and not f.name.startswith(".")]
        #     if not files:
        #         self.bot.reply_to(msg, "Drive is empty!")
        #         return
        #     lines = [f"* `{f.name}` - {human_size(f.stat().st_size)}" for f in sorted(files, key=lambda x: x.name)]
        #     self.bot.reply_to(msg, f"**Files ({len(files)}):**\n\n" + "\n".join(lines), parse_mode="Markdown")
        #
        # @self.bot.message_handler(commands=["info"])
        # def cmd_info(msg):
        #     if not self._is_authorized(msg.from_user.id):
        #         return
        #     files = list(DRIVE_DIR.iterdir()) if DRIVE_DIR.exists() else []
        #     files = [f for f in files if f.is_file() and not f.name.startswith(".")]
        #     total = sum(f.stat().st_size for f in files)
        #     self.bot.reply_to(
        #         msg,
        #         f"**Telegram Drive**\n\nPath: `{DRIVE_DIR}`\nFiles: {len(files)}\nTotal: {human_size(total)}\nUsers: {len(self.owner_ids)}",
        #         parse_mode="Markdown",
        #     )
        #
        # @self.bot.message_handler(commands=["delete"])
        # def cmd_delete(msg):
        #     if not self._is_authorized(msg.from_user.id):
        #         return
        #     parts = msg.text.split(maxsplit=1)
        #     if len(parts) < 2:
        #         self.bot.reply_to(msg, "Usage: /delete filename.txt")
        #         return
        #     filename = parts[1].strip()
        #     filepath = DRIVE_DIR / filename
        #     if filepath.exists() and filepath.is_file():
        #         filepath.unlink()
        #         self.sent_index.remove(filepath)
        #         self.bot.reply_to(msg, f"`{filename}` deleted!")
        #     else:
        #         self.bot.reply_to(msg, f"`{filename}` not found!")

        @self.bot.message_handler(content_types=["document"])
        def handle_document(msg):
            if not self._is_authorized(msg.from_user.id):
                self.bot.reply_to(msg, "Access denied!")
                return

            doc = msg.document
            filename = doc.file_name or f"file_{doc.file_id}"
            filepath = DRIVE_DIR / filename

            counter = 1
            stem, suffix = filepath.stem, filepath.suffix
            while filepath.exists():
                filepath = DRIVE_DIR / f"{stem}_{counter}{suffix}"
                counter += 1

            try:
                log.info(f"Downloading: {filename}")

                # Mark BEFORE writing to prevent watcher re-upload
                self.download_tracker.mark_downloaded(filepath)

                # Get file URL and download with progress
                resp = HTTP_SESSION.get(
                    f"https://api.telegram.org/bot{self.token}/getFile",
                    params={"file_id": doc.file_id},
                    timeout=(20, 60),
                )
                file_path_remote = resp.json()["result"]["file_path"]
                dl_url = f"https://api.telegram.org/file/bot{self.token}/{file_path_remote}"

                downloaded = self._download_with_progress(dl_url, filepath, filename)

                saved_size = human_size(downloaded)
                self.bot.reply_to(
                    msg,
                    f"**Saved!**\n`{filepath.name}`\nSize: {saved_size}",
                    parse_mode="Markdown",
                )
                log.info(f"Saved: {filepath.name} ({saved_size})")

            except Exception as e:
                if filepath.exists():
                    filepath.unlink()
                self.bot.reply_to(msg, f"Download error:\n`{e}`", parse_mode="Markdown")
                log.error(f"Download error {filename}: {e}")

        @self.bot.message_handler(content_types=["photo"])
        def handle_photo(msg):
            if not self._is_authorized(msg.from_user.id):
                return
            self._save_media(msg.photo[-1].file_id, msg.photo[-1].file_unique_id, "jpg", msg)

        @self.bot.message_handler(content_types=["video"])
        def handle_video(msg):
            if not self._is_authorized(msg.from_user.id):
                return
            self._save_media(msg.video.file_id, msg.video.file_unique_id, "mp4", msg)

        @self.bot.message_handler(content_types=["audio"])
        def handle_audio(msg):
            if not self._is_authorized(msg.from_user.id):
                return
            ext = "mp3" if msg.audio.mime_type == "audio/mpeg" else "ogg"
            self._save_media(msg.audio.file_id, msg.audio.file_unique_id, ext, msg)

        @self.bot.message_handler(content_types=["voice"])
        def handle_voice(msg):
            if not self._is_authorized(msg.from_user.id):
                return
            self._save_media(msg.voice.file_id, msg.voice.file_unique_id, "ogg", msg)

        @self.bot.message_handler(content_types=["animation"])
        def handle_animation(msg):
            if not self._is_authorized(msg.from_user.id):
                return
            self._save_media(msg.animation.file_id, msg.animation.file_unique_id, "gif", msg)

        @self.bot.message_handler(content_types=["sticker"])
        def handle_sticker(msg):
            if not self._is_authorized(msg.from_user.id):
                return
            if msg.sticker.is_video:
                self._save_media(msg.sticker.file_id, msg.sticker.file_unique_id, "webm", msg)
            elif not msg.sticker.is_animated:
                self._save_media(msg.sticker.file_id, msg.sticker.file_unique_id, "webp", msg)

    def _save_media(self, file_id, file_unique_id, ext, msg):
        filepath = DRIVE_DIR / f"{file_unique_id}.{ext}"
        try:
            self.download_tracker.mark_downloaded(filepath)

            resp = HTTP_SESSION.get(
                f"https://api.telegram.org/bot{self.token}/getFile",
                params={"file_id": file_id},
                timeout=(20, 60),
            )
            file_path_remote = resp.json()["result"]["file_path"]
            dl_url = f"https://api.telegram.org/file/bot{self.token}/{file_path_remote}"

            downloaded = self._download_with_progress(dl_url, filepath)

            self.bot.reply_to(msg, f"**Saved!**\n`{filepath.name}`\nSize: {human_size(downloaded)}", parse_mode="Markdown")
            log.info(f"Media saved: {filepath.name} ({human_size(downloaded)})")
        except Exception as e:
            if filepath.exists():
                filepath.unlink()
            log.error(f"Media save error: {e}")

    def start_polling(self):
        self.bot.infinity_polling(timeout=20, long_polling_timeout=20, skip_pending=True)


# ═══════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════
def main():
    config = load_config()
    if config is None:
        config = setup_config()
        print("\nStarting bot...")
        time.sleep(1)
    else:
        DRIVE_DIR.mkdir(exist_ok=True)

    download_tracker = DownloadTracker()
    drive = TelegramDriveBot(config, download_tracker)

    watcher = DriveWatcher(drive.bot, drive.token, config["owner_ids"], drive.sent_index, download_tracker)
    observer = Observer()
    observer.schedule(watcher, str(DRIVE_DIR), recursive=False)
    observer.start()
    log.info(f"Watcher active: {DRIVE_DIR}")

    existing = [f for f in DRIVE_DIR.iterdir() if f.is_file() and not f.name.startswith(".")] if DRIVE_DIR.exists() else []
    if existing:
        log.info(f"{len(existing)} files in drive:")
        for f in existing:
            log.info(f"  * {f.name}")

    log.info("Bot started!")
    log.info(f"Drive: {DRIVE_DIR}")
    log.info(f"Owners: {drive.owner_ids}")

    try:
        drive.start_polling()
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
    except Exception as e:
        log.critical(f"Fatal error: {e}")
    finally:
        observer.stop()
        observer.join()
        HTTP_SESSION.close()


if __name__ == "__main__":
    main()