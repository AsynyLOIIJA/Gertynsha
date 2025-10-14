# main.py
import os
import json
import asyncio
import logging
import mimetypes
from threading import Thread
from queue import Queue

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
    return "✅ Telegram listener is alive and syncing to Google Drive."

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

def keep_alive():
    t = Thread(target=run_flask, daemon=True)
    t.start()

# -----------------------------
# Config
# -----------------------------
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "live_session")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "token.json")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")

if not API_ID or not API_HASH:
    raise ValueError("API_ID and API_HASH must be set.")
if not DRIVE_FOLDER_ID:
    raise ValueError("DRIVE_FOLDER_ID must be set.")
if not os.path.exists(TOKEN_FILE):
    raise ValueError(f"{TOKEN_FILE} not found. Please run the OAuth auth script first.")

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
# Google Drive client
# -----------------------------
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def create_drive_service(token_file=TOKEN_FILE):
    creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

drive_service = create_drive_service()

# -----------------------------
# Upload helpers
# -----------------------------
async def upload_bytes_to_drive(data: bytes, filename: str, folder_id: str):
    try:
        media = MediaInMemoryUpload(data, mimetype="application/octet-stream", resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        loop = asyncio.get_event_loop()
        file = await loop.run_in_executor(None, lambda: drive_service.files().create(
            body=file_metadata, media_body=media, fields="id").execute()
        )
        logger.info(f"Uploaded file: {filename} -> Drive ID: {file.get('id')}")
    except Exception as e:
        logger.exception(f"Failed to upload file {filename}: {e}")

async def upload_message_json(message_dict: dict, folder_id: str):
    try:
        json_bytes = json.dumps(message_dict, ensure_ascii=False, indent=2).encode("utf-8")
        ts = message_dict.get('ts', '').replace(':', '-')
        filename = f"{message_dict.get('chat_name', 'chat')}_msg{message_dict.get('id')}_{ts}.json"
        media = MediaInMemoryUpload(json_bytes, mimetype="application/json", resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        loop = asyncio.get_event_loop()
        file = await loop.run_in_executor(None, lambda: drive_service.files().create(
            body=file_metadata, media_body=media, fields="id").execute()
        )
        logger.info(f"Uploaded message JSON: {filename}")
    except Exception as e:
        logger.exception(f"Failed to upload JSON: {e}")

# -----------------------------
# Telethon setup
# -----------------------------
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# -----------------------------
# Async upload queue
# -----------------------------
upload_queue = Queue()

async def upload_worker():
    while True:
        func, args = await asyncio.get_event_loop().run_in_executor(None, upload_queue.get)
        try:
            await func(*args)
        except Exception as e:
            logger.exception(f"Error in upload task: {e}")
        upload_queue.task_done()

def enqueue_upload(func, *args):
    upload_queue.put((func, args))

# -----------------------------
# Message handling
# -----------------------------
async def fetch_media_bytes(message, retries: int = 3):
    for attempt in range(1, retries + 1):
        try:
            data = await message.download_media(file=bytes)
            if data:
                return data
        except FloodWaitError as e:
            wait = getattr(e, "seconds", 10)
            logger.warning("FloodWaitError: waiting %s seconds", wait)
            await asyncio.sleep(wait + 1)
        except Exception as e:
            logger.warning("Download attempt %s failed: %s", attempt, e)
            await asyncio.sleep(2)
    return None

def build_message_dict(msg, chat_name: str, text: str):
    return {
        "id": msg.id,
        "chat_id": msg.chat_id,
        "sender_id": msg.sender_id,
        "chat_name": chat_name,
        "ts": msg.date.astimezone().isoformat(),
        "text": text.strip()
    }

@client.on(events.NewMessage(incoming=True))
async def handler(event):
    try:
        msg = event.message
        chat = await event.get_chat()
        chat_name = (
            getattr(chat, 'title', None)
            or getattr(chat, 'first_name', None)
            or "Saved_Messages"
        )
        chat_name = chat_name.replace(" ", "_")

        text = msg.message or msg.raw_text or ""
        if text.strip():
            message_dict = build_message_dict(msg, chat_name, text)
            enqueue_upload(upload_message_json, message_dict, DRIVE_FOLDER_ID)

        if msg.media:
            media_bytes = await fetch_media_bytes(msg)
            if media_bytes:
                mime = getattr(msg.file, "mime_type", None)
                ext = mimetypes.guess_extension(mime) or ".bin"
                filename = f"{chat_name}_msg{msg.id}{ext}"
                enqueue_upload(upload_bytes_to_drive, media_bytes, filename, DRIVE_FOLDER_ID)

        logger.info(f"Processed message from {chat_name}: {msg.id}")
    except Exception as e:
        logger.exception(f"Handler error: {e}")

# -----------------------------
# Main
# -----------------------------
async def main():
    if BOT_TOKEN:
        await client.start(bot_token=BOT_TOKEN)
        logger.info("Started as bot.")
    else:
        await client.start()
        me = await client.get_me()
        logger.info(f"Authorized as {me.first_name} ({me.id})")

    asyncio.create_task(upload_worker())
    logger.info("Listening for ALL private messages (including Saved Messages)...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped manually.")
