# Telegram Video + Audio Merger Bot

A **Bot API** Telegram bot that merges a video + audio without re-encoding (ffmpeg stream copy). You can send a video first, then an audio file, or upload a `links.txt` batch file with multiple pairs. Progress is shown in a **single message** with periodic edits.

## Features
- Video + audio merge without quality loss (`-c copy`).
- Guided `/batch` mode: Audio -> Video -> Output Name loop until `/done`.
- Batch jobs via `links.txt`.
- Serial processing queue (one merge job at a time).
- Live progress for download/merge/upload.
- Uses **yt-dlp** and prefers **aria2c** if installed.

## Requirements
- Python 3.10+
- ffmpeg + ffprobe in PATH
- (Optional) aria2c for faster downloads

## Install
```bash
pip install -r requirements.txt
```

## Configure
Create a `.env` file (see `.env.example`):
```
BOT_TOKEN=123456:ABCDEF
```

Optional:
- `WORK_DIR=data`
- `DOWNLOAD_CONCURRENCY=4`
- `MERGE_CONCURRENCY=2`
- `KEEP_FILES=0`

## Run
This repo is configured to run as a **webhook webservice** (not polling).

For local testing you need a public HTTPS URL (e.g. Cloudflare Tunnel / ngrok) and set `WEBHOOK_URL`.

```bash
python bot.py
```

## Deploy on Koyeb (installs ffmpeg)
Koyeb needs `ffmpeg` available at runtime. The simplest way is deploying as a **Docker** service and installing ffmpeg inside the image.

This repo includes a `Dockerfile` that installs ffmpeg via `apt-get`.

### Steps
1. Push this project to GitHub.
2. In Koyeb, create a new **App** from your GitHub repo.
3. Choose **Dockerfile** build.
4. Set environment variables:
   - `BOT_TOKEN` = your bot token
   - `WEBHOOK_URL` = your public base URL (example: `https://<your-app>.koyeb.app`)
   - `WEBHOOK_PATH` = `/webhook` (optional)
   - `WEBHOOK_SECRET` = any random string (optional but recommended)
   - (Optional) `WORK_DIR`, `DOWNLOAD_CONCURRENCY`, `MERGE_CONCURRENCY`, `KEEP_FILES`
5. Deploy.

Notes:
- This runs as a webservice and listens on `PORT` (default 8000).
- The bot sets the Telegram webhook automatically on startup to `WEBHOOK_URL + WEBHOOK_PATH`.

## Usage
### Single merge
1. Send a **video** file.
2. Send an **audio** file.

The bot downloads both, merges without re-encoding, and uploads the result.

### Guided batch merge (`/batch`)
1. Send `/batch`.
2. Repeat this for each item: Audio -> Video -> Output Name.
3. Send `/done`.

All queued items are processed one-by-one in order.

### Batch merge (`links.txt`)
Send `/links`, then upload a text file formatted like:
```
audio - https://example.com/audio.mp3
video - https://example.com/video.mp4
name  - my_merge.mp4

audio1 - https://example.com/audio2.mp3
video1 - https://example.com/video2.mp4
name1  - my_merge_2.mp4
```

You can also paste the same text directly in chat.

## Notes
- Merging uses `ffmpeg -c copy -shortest` to avoid re-encoding.
- If a link requires special handling, yt-dlp will attempt to extract it.
