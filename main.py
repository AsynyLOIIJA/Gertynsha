# main.py
import os
import json
import asyncio
import logging
import time
import threading
from threading import Thread
from queue import Queue
from datetime import datetime
import re
import mimetypes
from pathlib import Path
from collections import deque
from threading import Lock

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
from flask import request, Response

# Google API
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from google.oauth2.credentials import Credentials
import requests

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
# Self-ping (Render keep-alive)
# -----------------------------
PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL")
KEEPALIVE_INTERVAL_SEC = int(os.environ.get("KEEPALIVE_INTERVAL_SEC", "60"))

def _self_ping_loop():
    if not PUBLIC_URL:
        logging.info("No PUBLIC_URL/RENDER_EXTERNAL_URL set; self-ping disabled.")
        return
    logging.info("Self-ping enabled: %s (interval=%ss)", PUBLIC_URL, KEEPALIVE_INTERVAL_SEC)
    session = requests.Session()
    while True:
        try:
            resp = session.get(PUBLIC_URL, timeout=10)
            logging.debug("Self-ping %s -> %s", PUBLIC_URL, resp.status_code)
        except Exception as e:
            logging.warning("Self-ping error: %s", e)
        time.sleep(KEEPALIVE_INTERVAL_SEC)

def start_self_ping():
    th = threading.Thread(target=_self_ping_loop, daemon=True)
    th.start()

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
# Backfill settings (edit if needed)
# -----------------------------
# Максимум сообщений на диалог при бэкфилле (None = без лимита)
BACKFILL_LIMIT_PER_DIALOG = None

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

# -----------------------------
# In-memory recent messages storage for HTML rendering
# -----------------------------
RECENT_MAX = int(os.environ.get("RECENT_MAX", 2000))
recent_messages = deque(maxlen=RECENT_MAX)
recent_lock = Lock()
media_file_ids = {}  # filename -> drive file id
media_ids_lock = Lock()

async def upload_worker():
    while True:
        task = await upload_queue.get()
        try:
            if task["type"] == "media":
                file_id = upload_bytes_to_drive(task["data"], task["filename"], DRIVE_FOLDER_ID)
                if file_id:
                    with media_ids_lock:
                        media_file_ids[task["filename"]] = file_id
            elif task["type"] == "json":
                upload_message_json(task["data"], DRIVE_FOLDER_ID)
        except Exception as e:
            logger.exception("Upload worker error: %s", e)
        finally:
            upload_queue.task_done()

# -----------------------------
# Unified message processing
# -----------------------------
async def process_incoming_message(msg):
    try:
        sender = await msg.get_sender()
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
                "text": text,
            }
            await upload_queue.put({"type": "json", "data": message_dict})

        media_filenames = []
        if msg.media:
            media_bytes = await fetch_media_bytes(msg)
            if media_bytes:
                filename = build_media_filename(msg, sender_name, media_bytes)
                await upload_queue.put({"type": "media", "data": media_bytes, "filename": filename})
                media_filenames = [filename]
            else:
                logger.warning("Failed to download media msg_id=%s from %s", msg.id, sender_name)

        # Store in HTML buffer
        with recent_lock:
            recent_messages.append({
                "id": msg.id,
                "sender": sender_name,
                "ts": msg.date.astimezone().isoformat(),
                "text": (msg.message or msg.raw_text or "").strip(),
                "media": media_filenames,
            })

    except RPCError as e:
        logger.exception("RPCError while handling message: %s", e)
    except Exception as e:
        logger.exception("Error in process_incoming_message: %s", e)


# -----------------------------
# Backfill unread on startup
# -----------------------------
async def backfill_unread_private():
    try:
        dialogs = await client.get_dialogs(limit=None)
        total_dialogs = 0
        total_msgs = 0
        for d in dialogs:
            # Интересуют только приватные чаты (пользователи/боты)
            if not getattr(d, "is_user", False):
                continue
            unread = getattr(d, "unread_count", 0) or 0
            if unread <= 0:
                continue
            total_dialogs += 1
            # raw dialog содержит read_inbox_max_id
            read_max = 0
            try:
                if getattr(d, "dialog", None) and getattr(d.dialog, "read_inbox_max_id", None):
                    read_max = int(d.dialog.read_inbox_max_id or 0)
            except Exception:
                read_max = 0

            logger.info("Backfill dialog %s: unread=%s, read_inbox_max_id=%s", getattr(d.entity, 'username', getattr(d.entity, 'id', 'user')), unread, read_max)

            fetched = 0
            async for msg in client.iter_messages(d.entity, reverse=True, min_id=read_max):
                await process_incoming_message(msg)
                fetched += 1
                total_msgs += 1
                if BACKFILL_LIMIT_PER_DIALOG and fetched >= BACKFILL_LIMIT_PER_DIALOG:
                    break
                # лёгкая уступка циклу во избежание блокировок
                await asyncio.sleep(0)
        if total_dialogs:
            logger.info("Backfill finished: dialogs=%s, messages=%s", total_dialogs, total_msgs)
        else:
            logger.info("Backfill finished: no unread private dialogs")
    except RPCError as e:
        logger.exception("RPCError during backfill: %s", e)
    except Exception as e:
        logger.exception("Backfill error: %s", e)

# -----------------------------
# Flask routes for HTML / JSON rendering of recent messages
# -----------------------------

@app.route("/messages")
def messages_html():
    """Вернуть HTML страницу с сообщениями, сгруппированными по пользователю.
    Параметры query:
      limit (int) - максимум сообщений (по умолчанию 200)
      sender (str) - фильтр по имени отправителя
    """
    try:
        limit = int(request.args.get("limit", 200))
    except ValueError:
        limit = 200
    sender_filter = request.args.get("sender")
    with recent_lock:
        data = list(recent_messages)[-limit:]
    if sender_filter:
        data = [m for m in data if m["sender"].lower() == sender_filter.lower()]
    # Группировка по sender с сохранением порядка появления
    grouped = []  # list of (sender, [messages])
    index = {}
    for m in data:
        s = m["sender"]
        if s not in index:
            index[s] = len(grouped)
            grouped.append((s, [m]))
        else:
            grouped[index[s]][1].append(m)

    # Построить HTML
    parts = ["<html><head><meta charset='utf-8'>",
             "<title>Recent Telegram Messages</title>",
             "<style>body{font-family:Arial, sans-serif; background:#fafafa; margin:20px;}\n",
             "h2{border-bottom:1px solid #ccc; padding-bottom:4px;}\n",
             ".msg{background:#fff;border:1px solid #ddd;border-radius:6px;padding:8px;margin:6px 0;}\n",
             ".meta{color:#555;font-size:12px;margin-bottom:4px;}\n",
             ".media a{margin-right:8px;font-size:12px;}\n",
             "</style></head><body>"]
    parts.append(f"<h1>Messages (last {len(data)})</h1>")
    parts.append("<p>Generated at: " + datetime.utcnow().isoformat() + "Z</p>")
    if sender_filter:
        parts.append(f"<p>Filter sender = {sender_filter}</p>")
    with media_ids_lock:
        media_ids_snapshot = dict(media_file_ids)
    for sender, msgs in grouped:
        parts.append(f"<h2>{sender} <span style='font-size:12px;color:#888'>({len(msgs)})</span></h2>")
        for m in msgs:
            parts.append("<div class='msg'>")
            parts.append(f"<div class='meta'>id={m['id']} | ts={m['ts']}</div>")
            if m['text']:
                safe_text = (m['text']
                             .replace('&', '&amp;')
                             .replace('<', '&lt;')
                             .replace('>', '&gt;'))
                parts.append(f"<div class='text'>{safe_text}</div>")
            if m['media']:
                links = []
                for fname in m['media']:
                    fid = media_ids_snapshot.get(fname)
                    if fid:
                        url = f"https://drive.google.com/file/d/{fid}/view?usp=drivesdk"
                        links.append(f"<a href='{url}' target='_blank'>{fname}</a>")
                    else:
                        links.append(f"<span>{fname} (uploading...)</span>")
                parts.append("<div class='media'>" + " ".join(links) + "</div>")
            parts.append("</div>")
    parts.append("</body></html>")
    html = "".join(parts)
    return Response(html, mimetype="text/html")

@app.route("/messages.json")
def messages_json():
    try:
        limit = int(request.args.get("limit", 200))
    except ValueError:
        limit = 200
    sender_filter = request.args.get("sender")
    with recent_lock:
        data = list(recent_messages)[-limit:]
    if sender_filter:
        data = [m for m in data if m["sender"].lower() == sender_filter.lower()]
    # Добавляем drive_file_ids если уже есть
    with media_ids_lock:
        media_ids_snapshot = dict(media_file_ids)
    enriched = []
    for m in data:
        mm = dict(m)
        if m['media']:
            mm['media_links'] = []
            for fname in m['media']:
                fid = media_ids_snapshot.get(fname)
                if fid:
                    mm['media_links'].append({"filename": fname, "drive_file_id": fid})
        enriched.append(mm)
    return Response(json.dumps(enriched, ensure_ascii=False, indent=2), mimetype="application/json")

# -----------------------------
# Event handler
# -----------------------------
@client.on(events.NewMessage(incoming=True))
async def handler(event):
    msg = event.message
    if not event.is_private:
        return
    await process_incoming_message(msg)

@client.on(events.NewMessage(outgoing=True))
async def handler_out(event):
    msg = event.message
    if not event.is_private:
        return
    await process_incoming_message(msg)

# -----------------------------
# Main
# -----------------------------
async def main():
    # старт worker-ов
    for _ in range(1):  # единичный воркер сохраняет порядок загрузок
        asyncio.create_task(upload_worker())

    if BOT_TOKEN:
        await client.start(bot_token=BOT_TOKEN)
        logger.info("Started bot with BOT_TOKEN")
    else:
        await client.start()
        me = await client.get_me()
        logger.info("Authorized as %s (%s)", me.first_name, me.id)

    # Бэкфилл непрочитанных приватных сообщений
    await backfill_unread_private()

    logger.info("Listening for incoming private messages...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    start_self_ping()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
