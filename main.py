# main.py
import os
import json
import asyncio
import logging
import mimetypes
from threading import Thread

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
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
BOT_TOKEN = os.environ.get("BOT_TOKEN")
TARGET = os.environ.get("TARGET", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "live_session")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "token.json")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")

if not API_ID or not API_HASH or not TARGET:
    raise ValueError("API_ID, API_HASH and TARGET must be set in environment variables.")
if not TOKEN_FILE or not DRIVE_FOLDER_ID:
    raise ValueError("TOKEN_FILE and DRIVE_FOLDER_ID must be set in environment variables.")

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
def get_file_extension(mime_type: str):
    """Определяем расширение по MIME-type. Если не определено — возвращаем bin."""
    ext = mimetypes.guess_extension(mime_type)
    if ext:
        return ext
    # Специальные случаи Telethon (иногда mime_type пустой)
    mapping = {
        "image": ".jpg",
        "video": ".mp4",
        "audio": ".mp3",
        "document": ".pdf",
    }
    for k, v in mapping.items():
        if k in (mime_type or "").lower():
            return v
    return ".bin"

def format_meta(msg):
    ts = msg.date.astimezone().isoformat()
    return f"[{ts}] msg_id={msg.id} from={msg.sender_id} chat={msg.chat_id}"

def build_message_dict(msg, meta: str, text: str):
    return {
        "id": msg.id,
        "chat_id": msg.chat_id,
        "sender_id": msg.sender_id,
        "ts": msg.date.astimezone().isoformat(),
        "meta": meta,
        "text": text
    }

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
# Google Drive upload helpers
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

        # --- JSON только для текста ---
        if text.strip():
            message_dict = build_message_dict(msg, meta, text)
            upload_message_json(message_dict, DRIVE_FOLDER_ID)

        # --- Медиа загружаем отдельно ---
        if msg.media:
            media_bytes = await fetch_media_bytes(msg)
            if media_bytes:
                mime_type = getattr(msg.media, "mime_type", None) or "application/octet-stream"
                ext = get_file_extension(mime_type)
                filename = f"media_msg{msg.id}_{len(media_bytes)}{ext}"
                upload_bytes_to_drive(media_bytes, filename, DRIVE_FOLDER_ID)
            else:
                logger.warning("Failed to download media msg_id=%s", msg.id)

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
