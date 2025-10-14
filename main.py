# main.py
import os
import json
import asyncio
import logging
from threading import Thread
from queue import Queue
from datetime import datetime
import re
import mimetypes
from pathlib import Path

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeSticker,
)
from flask import Flask

# Google API
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from google.oauth2.credentials import Credentials

# -----------------------------
# Flask keep-alive
# -----------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "Telegram listener is alive and syncing to Google Drive."

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

def keep_alive():
    t = Thread(target=run_flask, daemon=True)
    t.start()

# -----------------------------
# Config from environment
# -----------------------------
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # если используем бота
SESSION_NAME = os.environ.get("SESSION_NAME", "live_session")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "token.json")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")

if not API_ID or not API_HASH:
    raise ValueError("API_ID and API_HASH must be set.")
if not TOKEN_FILE or not DRIVE_FOLDER_ID:
    raise ValueError("TOKEN_FILE and DRIVE_FOLDER_ID must be set.")

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("listener.log", encoding="utf-8")]
)
logger = logging.getLogger("tg-listener")

# -----------------------------
# Telethon client
# -----------------------------
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# -----------------------------
# Google Drive client (OAuth)
# -----------------------------
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def create_drive_service_from_token(token_file=TOKEN_FILE):
    creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

drive_service = create_drive_service_from_token(TOKEN_FILE)

# -----------------------------
# Helpers
# -----------------------------
def sanitize_filename(name: str, max_len: int = 150) -> str:
    name = name or "unknown"
    name = re.sub(r'[^\w\s\-_.()]', '_', name)
    name = re.sub(r'\s+', '_', name).strip('_')
    return name[:max_len]

def get_sender_display(sender):
    if not sender:
        return "unknown"
    if getattr(sender, "username", None):
        return sanitize_filename(str(sender.username))
    first = getattr(sender, "first_name", "") or ""
    last = getattr(sender, "last_name", "") or ""
    full = (first + " " + last).strip()
    return sanitize_filename(full) if full else f"user_{sender.id}"

def get_original_filename(msg):
    try:
        if msg.file and getattr(msg.file, "name", None):
            return sanitize_filename(msg.file.name)
        if getattr(msg.media, "document", None):
            for attr in msg.media.document.attributes:
                if getattr(attr, "file_name", None):
                    return sanitize_filename(attr.file_name)
    except Exception:
        pass
    return None

def ts_for_name(dt):
    return dt.astimezone().isoformat().replace(':', '-')

def get_file_extension(mime_type: str) -> str:
    """Получить расширение по mime_type с расширенными fallback."""
    if not mime_type:
        return ".bin"
    ext = mimetypes.guess_extension(mime_type)
    if ext in (None, ".jpe"):
        # .jpe часто возвращается для jpeg
        mapping = {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "image/webp": ".webp",
            "video/mp4": ".mp4",
            "video/quicktime": ".mov",
            "audio/ogg": ".ogg",
            "audio/mpeg": ".mp3",
            "audio/x-wav": ".wav",
            "audio/webm": ".webm",
            "video/webm": ".webm",
            "application/pdf": ".pdf",
            "application/zip": ".zip",
            "application/x-rar-compressed": ".rar",
            "application/json": ".json",
        }
        ext = mapping.get(mime_type, None)
    return ext or ".bin"

def sniff_extension(data: bytes) -> str:
    """Грубое определение расширения по сигнатуре, если mime неизвестен."""
    if not data or len(data) < 4:
        return ".bin"
    head = data[:16]
    if head.startswith(b"\xFF\xD8\xFF"):
        return ".jpg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
        return ".gif"
    if head.startswith(b"%PDF"):
        return ".pdf"
    if head.startswith(b"PK\x03\x04"):
        return ".zip"  # или docx/xlsx, но оставим zip
    if head[:4] == b"\x1f\x8b\x08\x00":
        return ".gz"
    if head.startswith(b"OggS"):
        return ".ogg"
    if head[4:8] == b"ftyp":  # mp4 / mov container
        return ".mp4"
    return ".bin"

def extract_document_filename(doc) -> str | None:
    try:
        for attr in getattr(doc, 'attributes', []) or []:
            if isinstance(attr, DocumentAttributeFilename):
                return sanitize_filename(attr.file_name)
    except Exception:
        pass
    return None

def build_media_filename(msg, sender_name: str, data: bytes) -> str:
    """
    Формирует осмысленное имя файла с расширением:
    <sender>__<timestamp>__<base><ext>
    Приоритеты: исходное имя -> mime_type -> сигнатура -> .bin
    """
    ts_part = ts_for_name(msg.date)
    base = f"msg{msg.id}"
    ext = ".bin"

    media = msg.media
    mime_type = None

    if isinstance(media, MessageMediaPhoto):
        # Фото: Telethon не даёт прямое имя; считаем jpeg
        ext = ".jpg"
        base = f"photo_msg{msg.id}"
    elif isinstance(media, MessageMediaDocument) and getattr(media, 'document', None):
        doc = media.document
        mime_type = getattr(doc, 'mime_type', None)
        orig_name = extract_document_filename(doc)
        if orig_name:
            p = Path(orig_name)
            base = sanitize_filename(p.stem) or base
            if p.suffix:
                ext = p.suffix
        # если нет orig_name или нет расширения
        if ext == ".bin" or len(ext) > 10:
            guessed = get_file_extension(mime_type) if mime_type else None
            if guessed and guessed != ".bin":
                ext = guessed
        # атрибуты видео/аудио могут помочь
        if ext == ".bin":
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    ext = ".mp4"
                    break
                if isinstance(attr, DocumentAttributeAudio):
                    # voice / music
                    if getattr(attr, 'voice', False):
                        ext = ".ogg"
                    else:
                        ext = ".mp3"
                    break
                if isinstance(attr, DocumentAttributeSticker):
                    ext = ".webp"
                    break
    else:
        mime_type = getattr(media, 'mime_type', None)
        if mime_type:
            ext = get_file_extension(mime_type)

    # Если все ещё .bin — попробуем сигнатуру
    if ext == ".bin" and data:
        ext = sniff_extension(data)

    filename = f"{sender_name}__{ts_part}__{base}{ext}"
    return sanitize_filename(filename, max_len=200)

def format_meta(msg):
    ts = msg.date.astimezone().isoformat()
    return f"[{ts}] msg_id={msg.id} from={msg.sender_id} chat={msg.chat_id}"

# -----------------------------
# Upload helpers
# -----------------------------
def upload_bytes_to_drive(data: bytes, filename: str, folder_id: str):
    try:
        media = MediaInMemoryUpload(data, mimetype="application/octet-stream", resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        file_id = file.get("id")
        logger.info("Uploaded bytes to Drive: %s (id=%s)", filename, file_id)
        return file_id
    except Exception as e:
        logger.exception("Drive upload bytes failed for %s: %s", filename, e)
        return None

def upload_message_json(message_dict: dict, folder_id: str):
    try:
        json_bytes = json.dumps(message_dict, ensure_ascii=False, indent=2).encode("utf-8")
        ts = message_dict.get('ts', '').replace(':', '-')
        filename = f"{message_dict.get('sender_name','unknown')}__{ts}.json"
        filename = sanitize_filename(filename, max_len=200)
        media = MediaInMemoryUpload(json_bytes, mimetype="application/json", resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        file_id = file.get("id")
        logger.info("Uploaded message JSON: %s (id=%s)", filename, file_id)
        return file_id
    except Exception as e:
        logger.exception("Drive upload JSON failed: %s", e)
        return None

async def fetch_media_bytes(message, retries: int = 3):
    for attempt in range(1, retries + 1):
        try:
            data = await message.download_media(file=bytes)
            return data if data else None
        except FloodWaitError as e:
            wait = getattr(e, "seconds", 10)
            logger.warning("FloodWaitError: waiting %s seconds", wait)
            await asyncio.sleep(wait + 1)
        except Exception as e:
            logger.exception("Download attempt %s failed: %s", attempt, e)
            await asyncio.sleep(2)
    return None

# -----------------------------
# Async Queue for uploads
# -----------------------------
upload_queue = asyncio.Queue()

async def upload_worker():
    while True:
        task = await upload_queue.get()
        try:
            if task["type"] == "media":
                upload_bytes_to_drive(task["data"], task["filename"], DRIVE_FOLDER_ID)
            elif task["type"] == "json":
                upload_message_json(task["data"], DRIVE_FOLDER_ID)
        except Exception as e:
            logger.exception("Upload worker error: %s", e)
        finally:
            upload_queue.task_done()

# -----------------------------
# Event handler
# -----------------------------
@client.on(events.NewMessage(incoming=True))
async def handler(event):
    try:
        msg = event.message
        if not event.is_private:
            return

        sender = await event.get_sender()
        if not sender:
            return

        sender_name = get_sender_display(sender)
        meta = format_meta(msg)
        logger.info("New message from %s: %s", sender_name, meta)

        text = msg.message or msg.raw_text or ""
        if text.strip():
            message_dict = {
                "id": msg.id,
                "chat_id": msg.chat_id,
                "sender_id": msg.sender_id,
                "sender_name": sender_name,
                "ts": msg.date.astimezone().isoformat(),
                "text": text
            }
            await upload_queue.put({"type": "json", "data": message_dict})

        if msg.media:
            media_bytes = await fetch_media_bytes(msg)
            if media_bytes:
                filename = build_media_filename(msg, sender_name, media_bytes)
                await upload_queue.put({"type": "media", "data": media_bytes, "filename": filename})
            else:
                logger.warning("Failed to download media msg_id=%s from %s", msg.id, sender_name)

    except RPCError as e:
        logger.exception("RPCError while handling message: %s", e)
    except Exception as e:
        logger.exception("Error in handler: %s", e)

# -----------------------------
# Main
# -----------------------------
async def main():
    # старт worker-ов
    for _ in range(3):
        asyncio.create_task(upload_worker())

    if BOT_TOKEN:
        await client.start(bot_token=BOT_TOKEN)
        logger.info("Started bot with BOT_TOKEN")
    else:
        await client.start()
        me = await client.get_me()
        logger.info("Authorized as %s (%s)", me.first_name, me.id)

    logger.info("Listening for incoming private messages...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
