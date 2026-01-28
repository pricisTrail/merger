# Telegram Video + Audio Merger Bot

A **Bot API** Telegram bot that merges a video + audio without re-encoding (ffmpeg stream copy). You can send a video first, then an audio file, or upload a `links.txt` batch file with multiple pairs. Progress is shown in a **single message** with periodic edits.

## Features
- Video + audio merge without quality loss (`-c copy`).
- Batch jobs via `links.txt`.
- Parallel downloads with per-job pairing guarantees.
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
   - (Optional) `WORK_DIR`, `DOWNLOAD_CONCURRENCY`, `MERGE_CONCURRENCY`, `KEEP_FILES`
5. Deploy.

Notes:
- This is a long-running worker (polling). You don't need an HTTP port.
- If Koyeb asks for a port anyway, set an arbitrary `PORT` env var; the bot ignores it.

## Usage
### Single merge
1. Send a **video** file.
2. Send an **audio** file.

The bot downloads both, merges without re-encoding, and uploads the result.

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
