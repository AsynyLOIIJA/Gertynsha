Telegram realtime listener -> Google Drive (service account)

Env vars required:
- API_ID
- API_HASH
- TARGET
- SESSION_NAME (optional)
- SERVICE_ACCOUNT_JSON (entire JSON string from service account key file)
- DRIVE_FOLDER_ID (ID of shared Drive folder)

Steps:
1. Enable Drive API.
2. Create service account and JSON key.
3. Create folder in your Drive, share it with service account email (editor).
4. Put SERVICE_ACCOUNT_JSON and DRIVE_FOLDER_ID in Railway variables.
5. Deploy repository to Railway and run.
