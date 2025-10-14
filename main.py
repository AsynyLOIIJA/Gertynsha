# main.pyук
import os
import json
import asyncio
import logging
from pathlib import Path  # может использоваться для генерации имён, но папки больше не создаём
from threading import Thread

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from flask import Flask

# Google API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

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
TARGET = os.environ.get("TARGET", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "live_session")
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON", "")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")

if not API_ID or not API_HASH or not TARGET:
    raise ValueError("API_ID, API_HASH and TARGET must be set in environment variables.")

if not SERVICE_ACCOUNT_JSON or not DRIVE_FOLDER_ID:
    raise ValueError("SERVICE_ACCOUNT_JSON and DRIVE_FOLDER_ID must be set in environment variables.")

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
# Больше не сохраняем локально: ни директорий, ни лог-файла.
# Все сообщения и медиа будут сразу уходить в Google Drive.
# -----------------------------

# -----------------------------
# Telethon client
# -----------------------------
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# -----------------------------
# Google Drive client
# -----------------------------
SCOPES = ["https://www.googleapis.com/auth/drive"]

def create_drive_service_from_sa(sa_json_str: str):
    sa_info = json.loads(sa_json_str)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return drive_service

drive_service = create_drive_service_from_sa(SERVICE_ACCOUNT_JSON)

def upload_bytes_to_drive(data: bytes, filename: str, folder_id: str):
    """Загрузка бинарных данных напрямую в Google Drive."""
    try:
        from googleapiclient.http import MediaInMemoryUpload
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
    """Загрузить JSON представление сообщения (текст и метаданные) в Google Drive."""
    try:
        from googleapiclient.http import MediaInMemoryUpload
        json_bytes = json.dumps(message_dict, ensure_ascii=False, indent=2).encode("utf-8")
        # Формируем имя файла: msg_<id>_<timestamp>.json (заменяем двоеточия)
        ts = message_dict.get('ts', '').replace(':', '-')
        filename = f"msg_{message_dict.get('id')}_{ts}.json"
        media = MediaInMemoryUpload(json_bytes, mimetype="application/json", resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        file_id = file.get("id")
        logger.info("Uploaded message JSON: %s (id=%s)", filename, file_id)
        return file_id
    except Exception as e:
        logger.exception("Drive upload JSON failed: %s", e)
        return None

# -----------------------------
# Helpers
# -----------------------------
def format_meta(msg):
    ts = msg.date.astimezone().isoformat()
    return f"[{ts}] msg_id={msg.id} from={msg.sender_id} chat={msg.chat_id}"

def build_message_dict(msg, meta: str, text: str, media_entries: list):
    return {
        "id": msg.id,
        "chat_id": msg.chat_id,
        "sender_id": msg.sender_id,
        "ts": msg.date.astimezone().isoformat(),
        "meta": meta,
        "text": text,
        "media": media_entries,
    }

async def fetch_media_bytes(message, retries: int = 3):
    """Получить медиа как bytes. Использует download_media(file=bytes)."""
    for attempt in range(1, retries + 1):
        try:
            data = await message.download_media(file=bytes)  # Telethon вернёт bytes
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
# Event handler
# -----------------------------
@client.on(events.NewMessage(incoming=True))
async def handler(event):
    try:
        msg = event.message
        sender = await event.get_sender()
        if not sender:
            return

        is_target = (hasattr(sender, "username") and str(sender.username) == str(TARGET)) or (str(sender.id) == str(TARGET))
        if not is_target:
            return

        meta = format_meta(msg)
        logger.info("New message from target: %s", meta)
        text = msg.message or msg.raw_text or ""
        media_entries = []
        if msg.media:
            media_bytes = await fetch_media_bytes(msg)
            if media_bytes:
                filename = f"media_msg{msg.id}_{len(media_bytes)}.bin"
                file_id = upload_bytes_to_drive(media_bytes, filename, DRIVE_FOLDER_ID)
                media_entries.append({
                    "filename": filename,
                    "drive_file_id": file_id,
                    "size": len(media_bytes),
                    "status": "uploaded" if file_id else "failed"
                })
            else:
                media_entries.append({"status": "download_failed"})

        message_dict = build_message_dict(msg, meta, text, media_entries)
        json_drive_id = upload_message_json(message_dict, DRIVE_FOLDER_ID)
        if json_drive_id:
            logger.info("Message JSON uploaded (drive id=%s) msg_id=%s", json_drive_id, msg.id)
        else:
            logger.warning("Failed to upload message JSON msg_id=%s", msg.id)
    except RPCError as e:
        logger.exception("RPCError while handling message: %s", e)
    except Exception as e:
        logger.exception("Error in handler: %s", e)

# -----------------------------
# Main
# -----------------------------
async def main():
    if BOT_TOKEN:
        await client.start(bot_token=BOT_TOKEN)
        logger.info("Started bot with BOT_TOKEN")
    else:
        await client.start()
        me = await client.get_me()
        logger.info("Authorized as %s (%s)", me.first_name, me.id)

    logger.info("Listening for messages from: %s", TARGET)
    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
