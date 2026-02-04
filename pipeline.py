"""Download + merge pipeline utilities."""
from __future__ import annotations

import asyncio
import logging
import re
import shutil
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional

import aiofiles
import aiohttp
import os
from aiogram import Bot
from aiogram.types import FSInputFile
from yt_dlp import YoutubeDL
from pyrogram import Client

from utils import format_bytes, format_duration, format_speed

DOWNLOAD_CHUNK_SIZE = 512 * 1024
LOGGER = logging.getLogger(__name__)

ProgressCallback = Callable[[str], Awaitable[None]]
SyncProgressCallback = Optional[Callable[[str], None]]


class ToolMissingError(RuntimeError):
    pass


def ensure_dependencies() -> None:
    if not shutil.which("ffmpeg"):
        raise ToolMissingError("ffmpeg not found in PATH")
    if not shutil.which("ffprobe"):
        raise ToolMissingError("ffprobe not found in PATH")


def format_progress(action: str, current: int, total: Optional[int], speed: Optional[float]) -> str:
    percent = "?"
    eta = None
    total_str = "?"
    if total and total > 0:
        percent = f"{(current / total) * 100:.1f}%"
        total_str = format_bytes(total)
        if speed and speed > 0:
            eta = max((total - current) / speed, 0)
    parts = [
        action,
        percent,
        f"{format_bytes(current)}/{total_str}",
        format_speed(speed) if speed else "",
    ]
    if eta is not None:
        parts.append(f"ETA {format_duration(eta)}")
    return " ".join(p for p in parts if p)


class ByteProgress:
    def __init__(self, action: str, total: Optional[int], callback: Optional[ProgressCallback]) -> None:
        self.action = action
        self.total = total
        self.callback = callback
        self.start = time.monotonic()
        self.last_emit = 0.0

    async def emit(self, current: int, force: bool = False) -> None:
        if not self.callback:
            return
        now = time.monotonic()
        if not force and now - self.last_emit < 0.8:
            return
        speed = current / max(now - self.start, 0.001)
        text = format_progress(self.action, current, self.total, speed)
        await self.callback(text)
        self.last_emit = now

    async def done(self) -> None:
        if self.callback:
            await self.callback("done")


# Single shared Pyrogram client - stays connected, supports concurrent downloads
_pyrogram_client: Optional[Client] = None
_pyrogram_lock = asyncio.Lock()


async def get_pyrogram_client(bot_token: str) -> Optional[Client]:
    """Get or create a shared Pyrogram client that stays connected."""
    global _pyrogram_client
    
    api_id = os.getenv("API_ID")
    api_hash = os.getenv("API_HASH")
    
    if not api_id or not api_hash:
        return None
    
    async with _pyrogram_lock:
        if _pyrogram_client is None:
            _pyrogram_client = Client(
                name="merger_bot",
                api_id=int(api_id),
                api_hash=api_hash,
                bot_token=bot_token,
                in_memory=True,
            )
            await _pyrogram_client.start()
            LOGGER.info("Shared Pyrogram client started")
        elif not _pyrogram_client.is_connected:
            try:
                await _pyrogram_client.start()
                LOGGER.info("Shared Pyrogram client reconnected")
            except Exception as e:
                LOGGER.warning("Failed to reconnect, creating new client: %s", e)
                _pyrogram_client = Client(
                    name="merger_bot",
                    api_id=int(api_id),
                    api_hash=api_hash,
                    bot_token=bot_token,
                    in_memory=True,
                )
                await _pyrogram_client.start()
    
    return _pyrogram_client


async def download_telegram_file(
    bot: Bot,
    file_id: str,
    dest_path: Path,
    semaphore: asyncio.Semaphore,
    progress_cb: Optional[ProgressCallback] = None,
    max_retries: int = 3,
) -> None:
    """Download file from Telegram using Pyrogram (supports up to 2GB).
    Falls back to Bot API for small files if Pyrogram credentials missing.
    """
    async with semaphore:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Try Pyrogram first (supports files up to 2GB)
        app = await get_pyrogram_client(bot.token)
        if app:
            last_error = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    if progress_cb:
                        if attempt > 1:
                            await progress_cb(f"retrying ({attempt}/{max_retries})...")
                        else:
                            await progress_cb("downloading...")
                    
                    last_emit = 0.0
                    
                    async def pyrogram_progress(current: int, total: int) -> None:
                        nonlocal last_emit
                        now = asyncio.get_event_loop().time()
                        if now - last_emit > 0.8 and progress_cb:
                            speed = None
                            text = format_progress("downloading", current, total, speed)
                            await progress_cb(text)
                            last_emit = now
                    
                    # Download using shared client (supports concurrent downloads)
                    await app.download_media(
                        file_id,
                        file_name=str(dest_path),
                        progress=pyrogram_progress,
                    )
                    
                    # Verify file was downloaded
                    if dest_path.exists() and dest_path.stat().st_size > 0:
                        if progress_cb:
                            await progress_cb("done")
                        return
                    else:
                        raise RuntimeError("Download completed but file is empty")
                    
                except Exception as exc:
                    last_error = exc
                    LOGGER.warning("Pyrogram download attempt %d/%d failed: %s", attempt, max_retries, exc)
                    
                    # Handle FLOOD_WAIT - wait the required time
                    if "FLOOD_WAIT" in str(exc):
                        import re
                        match = re.search(r'(\d+) seconds', str(exc))
                        if match:
                            wait_time = int(match.group(1))
                            if progress_cb:
                                await progress_cb(f"rate limited, waiting {wait_time}s...")
                            await asyncio.sleep(wait_time + 5)
                            continue
                    
                    if attempt < max_retries:
                        await asyncio.sleep(2 * attempt)  # Backoff: 2s, 4s, 6s
            
            # All Pyrogram retries failed
            if last_error:
                if progress_cb:
                    await progress_cb(f"failed: {last_error}")
                raise RuntimeError(f"Pyrogram download failed after {max_retries} attempts: {last_error}")
        
        # Fallback: Bot API (only works for files < 20MB)
        LOGGER.info("Using Bot API for download (limit 20MB)")
        file = await bot.get_file(file_id)
        if not file.file_path:
            raise RuntimeError("Telegram file path missing")
        url = f"https://api.telegram.org/file/bot{bot.token}/{file.file_path}"
        total = file.file_size
        progress = ByteProgress("downloading", total, progress_cb)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                downloaded = 0
                async with aiofiles.open(dest_path, "wb") as handle:
                    async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                        await handle.write(chunk)
                        downloaded += len(chunk)
                        await progress.emit(downloaded)
        await progress.done()


def _run_ytdlp(
    url: str,
    dest_path: Path,
    media_format: str,
    progress_cb: SyncProgressCallback,
    external_downloader: Optional[str],
) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    base_path = dest_path
    if base_path.suffix:
        base_path = base_path.with_suffix("")

    def emit(text: str) -> None:
        if progress_cb:
            progress_cb(text)

    def hook(status: dict) -> None:
        if status.get("status") == "downloading":
            total = status.get("total_bytes") or status.get("total_bytes_estimate")
            downloaded = status.get("downloaded_bytes", 0)
            speed = status.get("speed")
            emit(format_progress("downloading", downloaded, total, speed))
        elif status.get("status") == "finished":
            emit("processing")

    ydl_opts = {
        "outtmpl": f"{base_path}.%(ext)s",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "progress_hooks": [hook],
        "overwrites": True,
        # Timeout and retry settings
        "socket_timeout": 120,          # 2 minute socket timeout
        "retries": 3,                   # Retry download 3 times
        "fragment_retries": 5,          # Retry fragments 5 times
        "retry_sleep_functions": {"http": lambda n: 5 * (n + 1)},  # Exponential backoff
        "file_access_retries": 3,       # Retry file access
        "extractor_retries": 3,         # Retry extractor
    }
    if media_format == "audio":
        ydl_opts["format"] = "bestaudio[ext=m4a]/bestaudio/best"
    else:
        ydl_opts["format"] = "bestvideo[ext=mp4]/bestvideo/best"
    if external_downloader:
        ydl_opts["external_downloader"] = external_downloader
        ydl_opts["external_downloader_args"] = {
            "aria2c": ["-x", "16", "-k", "5M", "--continue=true", "--max-tries=5", "--retry-wait=3", "--timeout=60", "--summary-interval=1"]
        }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = None
        if isinstance(info, dict):
            requested = info.get("requested_downloads")
            if requested:
                filename = requested[0].get("filepath") or requested[0].get("filename")
            if not filename:
                filename = info.get("filepath")
        if not filename:
            filename = ydl.prepare_filename(info)
    emit("done")
    return Path(filename)


async def _run_wget(
    url: str,
    dest_path: Path,
    progress_cb: Optional[ProgressCallback],
) -> Path:
    """Download using wget with retry, resume, and timeout support."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "wget",
        url,
        "-O", str(dest_path),
        # Retry & Resume
        "--tries=5",                    # Retry up to 5 times
        "--continue",                   # Resume partial downloads
        "--waitretry=3",                # Wait 3 seconds between retries
        # Timeout handling
        "--timeout=60",                 # Network timeout 60s
        "--dns-timeout=30",             # DNS lookup timeout
        "--connect-timeout=30",         # Connection timeout
        "--read-timeout=120",           # Read timeout 2 minutes
        # Optimizations
        "--no-check-certificate",       # Skip SSL verification for speed
        "--progress=bar:force:noscroll", # Show progress
        "-q", "--show-progress",        # Quiet but show progress bar
    ]
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    
    last_emit = 0.0
    if process.stderr:  # wget outputs progress to stderr
        async for raw in process.stderr:
            try:
                line = raw.decode("utf-8", "ignore").strip()
                # wget progress: 50% [=====>     ] 50M  10.5M/s eta 5s
                if "%" in line:
                    now = asyncio.get_event_loop().time()
                    if now - last_emit > 0.8 and progress_cb:
                        # Clean up the progress line
                        await progress_cb(f"wget: {line[:60]}")
                        last_emit = now
            except Exception:
                continue

    stdout, stderr = await process.communicate()
    
    if process.returncode != 0:
        error = stderr.decode("utf-8", "ignore").strip() if stderr else "wget failed"
        raise RuntimeError(error)
    
    # Verify file exists and has content
    if not dest_path.exists() or dest_path.stat().st_size == 0:
        raise RuntimeError("wget completed but file is empty or missing")
    
    if progress_cb:
        await progress_cb("done")
    
    return dest_path


async def _run_aria2c(
    url: str,
    dest_path: Path,
    progress_cb: Optional[ProgressCallback],
) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "aria2c",
        url,
        "--dir", str(dest_path.parent),
        "--out", dest_path.name,
        "--allow-overwrite=true",
        # Multi-connection settings
        "--max-connection-per-server=16",
        "--split=16",
        "--min-split-size=5M",              # Optimized chunk size (5MB per chunk)
        "--piece-length=5M",                # Piece length for better parallel downloads
        # Retry & Resume support
        "--continue=true",                  # Resume partial/interrupted downloads
        "--max-tries=5",                    # Retry up to 5 times on failure
        "--retry-wait=3",                   # Wait 3 seconds between retries
        "--max-file-not-found=3",           # Retry if server returns file not found
        # Connection timeout handling
        "--timeout=60",                     # Overall connection timeout (60s)
        "--connect-timeout=30",             # Initial connection timeout (30s)
        "--lowest-speed-limit=10K",         # Drop connections slower than 10KB/s
        "--max-resume-failure-tries=5",     # Resume retry attempts
        # Optimizations
        "--stream-piece-selector=geom",     # Geometric piece selection for streaming
        "--uri-selector=adaptive",          # Adaptive URI selection
        "--auto-file-renaming=false",       # Don't rename on conflict
        "--summary-interval=1",
        "--console-log-level=warn",         # Reduce log noise
    ]
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    
    # Progress parsing for aria2c (simplified)
    if process.stdout:
        async for raw in process.stdout:
            try:
                line = raw.decode("utf-8", "ignore").strip()
                # Example aria2c output: [#b037be 1.1GiB/1.1GiB(99%) CN:1 DL:2.8MiB ETA:13s]
                # We want: 99% 1.1GiB/1.1GiB 2.8MiB/s ETA 13s
                if "[" in line and "]" in line and "(" in line in line:
                    try:
                        # Extract percentage: (99%)
                        percent_match = re.search(r"\((\d+%)\)", line)
                        percent = percent_match.group(1) if percent_match else ""
                        
                        # Extract sizes: 1.1GiB/1.1GiB
                        size_match = re.search(r"(\d+\.?\d*[KMGT]i?B/\d+\.?\d*[KMGT]i?B)", line)
                        sizes = size_match.group(1) if size_match else ""
                        
                        # Extract speed: DL:2.8MiB
                        speed_match = re.search(r"DL:(\d+\.?\d*[KMGT]i?B)", line)
                        speed = f"{speed_match.group(1)}/s" if speed_match else ""
                        
                        # Extract ETA: ETA:13s
                        eta_match = re.search(r"ETA:(\w+)", line)
                        eta = f"ETA {eta_match.group(1)}" if eta_match else ""
                        
                        text = f"{percent} {sizes} {speed} {eta}".strip()
                        if text and progress_cb:
                            await progress_cb(text)
                    except Exception:
                        # Fallback to cleaned raw line if regex fails
                        status_line = line[line.find("[")+1 : line.find("]")]
                        if progress_cb:
                            await progress_cb(status_line)
                elif "download completed" in line.lower():
                    if progress_cb:
                        await progress_cb("done")
            except Exception:
                continue

    stdout, stderr = await process.communicate()
    if progress_cb:
        await progress_cb("done")
    if process.returncode != 0:
        error = stderr.decode("utf-8", "ignore").strip() if stderr else "aria2c failed"
        raise RuntimeError(error)
    
    return dest_path


async def download_url(
    url: str,
    dest_path: Path,
    media_format: str,
    semaphore: asyncio.Semaphore,
    progress_cb: Optional[ProgressCallback] = None,
    external_downloader: Optional[str] = None,
    max_retries: int = 3,
) -> Path:
    loop = asyncio.get_running_loop()

    def sync_progress(text: str) -> None:
        if progress_cb:
            asyncio.run_coroutine_threadsafe(progress_cb(text), loop)

    async with semaphore:
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                # ENGINE 1: Try wget first (simple, reliable, good resume support)
                if shutil.which("wget"):
                    try:
                        LOGGER.info("Trying wget for %s (attempt %d/%d)", url, attempt, max_retries)
                        if progress_cb:
                            await progress_cb(f"downloading (wget, attempt {attempt})")
                        result = await _run_wget(url, dest_path, progress_cb)
                        if result.exists() and result.stat().st_size > 0:
                            return result
                        else:
                            raise RuntimeError("wget completed but file is empty or missing")
                    except Exception as exc:
                        LOGGER.warning("wget failed (attempt %d): %s", attempt, exc)
                        if progress_cb:
                            await progress_cb(f"wget failed, trying aria2c...")
                
                # ENGINE 2: Try aria2c (multi-connection, faster for large files)
                if shutil.which("aria2c"):
                    try:
                        LOGGER.info("Trying aria2c for %s (attempt %d/%d)", url, attempt, max_retries)
                        if progress_cb:
                            await progress_cb(f"downloading (aria2c, attempt {attempt})")
                        result = await _run_aria2c(url, dest_path, progress_cb)
                        if result.exists() and result.stat().st_size > 0:
                            return result
                        else:
                            raise RuntimeError("aria2c completed but file is empty or missing")
                    except Exception as exc:
                        LOGGER.warning("aria2c failed (attempt %d): %s", attempt, exc)
                        if progress_cb:
                            await progress_cb(f"aria2c failed, trying yt-dlp...")
                
                # ENGINE 3: Fallback to yt-dlp (handles complex sites, authentication, etc.)
                LOGGER.info("Trying yt-dlp for %s (attempt %d/%d)", url, attempt, max_retries)
                if progress_cb:
                    await progress_cb(f"downloading (yt-dlp, attempt {attempt})")
                
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        _run_ytdlp, url, dest_path, media_format, sync_progress, external_downloader
                    ),
                    timeout=300  # 5 minute timeout for yt-dlp
                )
                
                # Verify file exists
                if result.exists() and result.stat().st_size > 0:
                    return result
                else:
                    raise RuntimeError("yt-dlp completed but file is empty or missing")
                    
            except asyncio.TimeoutError:
                last_error = "Download timed out after 5 minutes"
                LOGGER.warning("Download timeout (attempt %d/%d): %s", attempt, max_retries, url)
            except Exception as exc:
                last_error = str(exc)
                LOGGER.warning("Download failed (attempt %d/%d): %s", attempt, max_retries, exc)
            
            # Wait before retry (exponential backoff: 5s, 10s, 20s)
            if attempt < max_retries:
                wait_time = 5 * (2 ** (attempt - 1))
                if progress_cb:
                    await progress_cb(f"retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        
        # All retries failed
        if progress_cb:
            await progress_cb(f"failed: {last_error}")
        raise RuntimeError(f"Download failed after {max_retries} attempts: {last_error}")


async def _probe_video_info(path: Path) -> dict:
    """Returns duration, width, height or empty dict if probe fails."""
    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration:stream=width,height",
        "-of", "json",
        str(path)
    ]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    if process.returncode != 0:
        return {}
    
    import json
    try:
        data = json.loads(stdout.decode())
        format_info = data.get("format", {})
        streams = data.get("streams", [])
        video_stream = next((s for s in streams if s.get("width")), {})
        
        return {
            "duration": int(float(format_info.get("duration", 0))),
            "width": int(video_stream.get("width", 0)),
            "height": int(video_stream.get("height", 0)),
        }
    except (ValueError, KeyError, StopIteration, TypeError):
        return {}


async def _generate_thumb(video_path: Path) -> Optional[Path]:
    """Generates a thumbnail for the video at 10% of its duration."""
    info = await _probe_video_info(video_path)
    duration = info.get("duration", 0)
    offset = max(1, int(duration * 0.1))
    
    thumb_path = video_path.with_suffix(".jpg")
    cmd = [
        "ffmpeg",
        "-y",
        "-ss", str(offset),
        "-i", str(video_path),
        "-vframes", "1",
        "-q:v", "2",
        str(thumb_path)
    ]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await process.communicate()
    if process.returncode == 0:
        return thumb_path
    return None


async def _probe_duration(path: Path) -> Optional[float]:
    info = await _probe_video_info(path)
    return float(info["duration"]) if "duration" in info else None


async def merge_stream_copy(
    video_path: Path,
    audio_path: Path,
    output_path: Path,
    progress_cb: Optional[ProgressCallback] = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    duration_video, duration_audio = await asyncio.gather(
        _probe_duration(video_path), _probe_duration(audio_path)
    )
    total_duration = None
    if duration_video and duration_audio:
        total_duration = min(duration_video, duration_audio)
    else:
        total_duration = duration_video or duration_audio

    if progress_cb:
        await progress_cb("starting")

    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(audio_path),
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-c",
        "copy",
        "-shortest",
        "-progress",
        "pipe:1",
        "-nostats",
        str(output_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    last_emit = 0.0
    if process.stdout:
        async for raw in process.stdout:
            line = raw.decode().strip()
            if line.startswith("out_time_ms=") and total_duration:
                current_ms = int(line.split("=", 1)[1] or 0)
                current = current_ms / 1_000_000
                percent = min((current / total_duration) * 100, 100)
                now = time.monotonic()
                if now - last_emit > 0.8 and progress_cb:
                    await progress_cb(f"{percent:.1f}%")
                    last_emit = now

    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        error = stderr.decode().strip() if stderr else "ffmpeg failed"
        LOGGER.error("ffmpeg error: %s", error)
        raise RuntimeError(error)

    if progress_cb:
        await progress_cb("done")


async def upload_with_progress(
    bot: Bot,
    chat_id: int,
    video_path: Path,
    caption: str,
    progress_cb: Optional[ProgressCallback] = None,
    destination_id: Optional[int | str] = None,
) -> None:
    """Uploads file using Pyrogram to bypass the 50MB Bot API limit."""
    api_id = os.getenv("API_ID")
    api_hash = os.getenv("API_HASH")
    bot_token = bot.token
    target_chat = destination_id or chat_id

    if not api_id or not api_hash:
        LOGGER.warning("API_ID or API_HASH missing. Falling back to aiogram (limit 50MB).")
        try:
            if progress_cb:
                await progress_cb("starting upload (aiogram)")
            await bot.send_video(
                chat_id=target_chat,
                video=FSInputFile(str(video_path)),
                caption=caption,
                supports_streaming=True,
            )
            if progress_cb:
                await progress_cb("done")
            return
        except Exception as e:
            if progress_cb:
                await progress_cb(f"failed: {str(e)}")
            raise e

    # Use Pyrogram for large files
    try:
        if progress_cb:
            await progress_cb("preparing metadata")
        
        # Extract metadata for better Telegram display
        info = await _probe_video_info(video_path)
        thumb_path = await _generate_thumb(video_path)
        
        duration = info.get("duration", 0)
        width = info.get("width", 0)
        height = info.get("height", 0)

        async with Client(
            name=f"uploader_{chat_id}",
            api_id=int(api_id),
            api_hash=api_hash,
            bot_token=bot_token,
            in_memory=True,
        ) as app:
            if progress_cb:
                await progress_cb("uploading")

            async def pyrogram_progress(current, total):
                if progress_cb:
                    await progress_cb(format_progress("uploading", current, total, None))

            await app.send_video(
                chat_id=target_chat,
                video=str(video_path),
                caption=caption,
                supports_streaming=True,
                duration=duration,
                width=width,
                height=height,
                thumb=str(thumb_path) if thumb_path else None,
                progress=pyrogram_progress
            )
            
        if thumb_path:
            thumb_path.unlink(missing_ok=True)
            
        if progress_cb:
            await progress_cb("done")
    except Exception as e:
        if progress_cb:
            await progress_cb(f"failed: {str(e)}")
        raise e
