# main.pyук
import os
import json
import asyncio
import logging
from pathlib import Path
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
# Paths
# -----------------------------
BASE_DIR = Path("live_dump") / str(TARGET)
MEDIA_DIR = BASE_DIR / "media"
LOG_FILE = BASE_DIR / "live_messages.txt"
BASE_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

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

def upload_file_to_drive(local_path: Path, folder_id: str):
    try:
        file_metadata = {"name": local_path.name, "parents": [folder_id]}
        media = MediaFileUpload(str(local_path), resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        file_id = file.get("id")
        logger.info("Uploaded to Drive: %s (id=%s)", local_path.name, file_id)
        return file_id
    except Exception as e:
        logger.exception("Drive upload failed for %s: %s", local_path, e)
        return None

# -----------------------------
# Helpers
# -----------------------------
def format_meta(msg):
    ts = msg.date.astimezone().isoformat()
    return f"[{ts}] msg_id={msg.id} from={msg.sender_id} chat={msg.chat_id}"

def write_log_line(line: str):
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

async def safe_download(message, dest_folder: Path, retries: int = 3):
    for attempt in range(1, retries + 1):
        try:
            path = await message.download_media(file=dest_folder)
            return Path(path) if path else None
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
        write_log_line(meta)
        if text:
            write_log_line(text)

        if msg.media:
            saved_path = await safe_download(msg, MEDIA_DIR)
            if saved_path:
                write_log_line(f"[Media saved: {saved_path.name}]")
                logger.info("Media saved locally: %s", saved_path)
                # upload to drive
                file_id = upload_file_to_drive(saved_path, DRIVE_FOLDER_ID)
                if file_id:
                    write_log_line(f"[Uploaded to Drive: {saved_path.name} (id={file_id})]")
                else:
                    write_log_line("[Upload to Drive: FAILED]")
            else:
                write_log_line("[Media saved: FAILED]")
                logger.warning("Couldn't save media for msg_id=%s", msg.id)

        write_log_line("")  # separator
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
