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
from pyrogram import Client

from utils import format_bytes, format_duration, format_speed

DOWNLOAD_CHUNK_SIZE = 512 * 1024
LOGGER = logging.getLogger(__name__)

ProgressCallback = Callable[[str], Awaitable[None]]


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
# Pyrogram client configuration
_pyrogram_lock = asyncio.Lock()


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
    api_id = os.getenv("API_ID")
    api_hash = os.getenv("API_HASH")
    
    async with semaphore:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Try Pyrogram if credentials available (supports files up to 2GB)
        if api_id and api_hash:
            last_error = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    if progress_cb:
                        if attempt > 1:
                            await progress_cb(f"retrying ({attempt}/{max_retries})...")
                        else:
                            await progress_cb("connecting...")
                    
                    # Use async context manager - ensures proper connect/disconnect
                    async with Client(
                        name=f"dl_{int(time.time())}",
                        api_id=int(api_id),
                        api_hash=api_hash,
                        bot_token=bot.token,
                        in_memory=True,
                        sleep_threshold=60,
                    ) as app:
                        if progress_cb:
                            await progress_cb("downloading...")
                        
                        last_emit = 0.0
                        download_start = time.monotonic()
                        
                        async def pyrogram_progress(current: int, total: int) -> None:
                            nonlocal last_emit
                            now = time.monotonic()
                            if now - last_emit > 0.8 and progress_cb:
                                elapsed = max(now - download_start, 0.001)
                                speed = current / elapsed
                                text = format_progress("downloading", current, total, speed)
                                await progress_cb(text)
                                last_emit = now
                        
                        # Download using Pyrogram
                        await app.download_media(
                            file_id,
                            file_name=str(dest_path),
                            progress=pyrogram_progress,
                        )
                    
                    # Verify file was downloaded
                    if dest_path.exists() and dest_path.stat().st_size > 0:
                        if progress_cb:
                            await progress_cb("done")
                        LOGGER.info("Pyrogram download complete: %s (%d bytes)", 
                                   dest_path.name, dest_path.stat().st_size)
                        return
                    else:
                        raise RuntimeError("Download completed but file is empty")
                    
                except Exception as exc:
                    last_error = exc
                    LOGGER.warning("Pyrogram download attempt %d/%d failed: %s", 
                                  attempt, max_retries, exc)
                    
                    # Handle FLOOD_WAIT
                    if "FLOOD_WAIT" in str(exc):
                        match = re.search(r'(\d+) seconds', str(exc))
                        if match:
                            wait_time = int(match.group(1))
                            if progress_cb:
                                await progress_cb(f"rate limited, waiting {wait_time}s...")
                            await asyncio.sleep(wait_time + 5)
                            continue
                    
                    if attempt < max_retries:
                        await asyncio.sleep(2 * attempt)
            
            # All Pyrogram retries failed - fall back to Bot API
            LOGGER.warning("Pyrogram failed after %d attempts, trying Bot API: %s", 
                          max_retries, last_error)
        
        # Fallback: Bot API (only works for files < 20MB)
        if progress_cb:
            await progress_cb("downloading (Bot API)...")
        
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
        LOGGER.info("Bot API download complete: %s (%d bytes)", 
                   dest_path.name, dest_path.stat().st_size)



async def _run_aria2c(
    url: str,
    dest_path: Path,
    progress_cb: Optional[ProgressCallback],
) -> Path:
    """Download using aria2c with multi-connection support."""
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
        "--min-split-size=1M",
        # Retry & Resume support
        "--continue=true",
        "--max-tries=5",
        "--retry-wait=3",
        "--max-file-not-found=3",
        # Connection timeout handling
        "--timeout=120",
        "--connect-timeout=60",
        "--max-resume-failure-tries=5",
        # Output settings
        "--console-log-level=error",
        "--summary-interval=0",
        "--download-result=hide",
        "--auto-file-renaming=false",
        "--human-readable=true",
    ]
    
    LOGGER.info("Running aria2c: %s -> %s", url[:80], dest_path.name)
    
    if progress_cb:
        await progress_cb("starting download...")
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    
    try:
        # Set a generous timeout for the download (30 minutes)
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=1800
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError("aria2c download timed out after 30 minutes")
    
    if progress_cb:
        await progress_cb("done")
    
    if process.returncode != 0:
        error_msg = stderr.decode("utf-8", "ignore").strip() if stderr else ""
        if not error_msg:
            error_msg = stdout.decode("utf-8", "ignore").strip() if stdout else ""
        raise RuntimeError(f"aria2c failed (code {process.returncode}): {error_msg[:200]}")
    
    # Verify file was downloaded
    if not dest_path.exists():
        raise RuntimeError(f"aria2c completed but file not found: {dest_path.name}")
    
    if dest_path.stat().st_size == 0:
        dest_path.unlink(missing_ok=True)
        raise RuntimeError("aria2c completed but file is empty")
    
    LOGGER.info("Download complete: %s (%d bytes)", dest_path.name, dest_path.stat().st_size)
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
    """Download a URL using aria2c only."""
    
    if not shutil.which("aria2c"):
        raise RuntimeError("aria2c not found in PATH - required for downloads")

    async with semaphore:
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                LOGGER.info("Downloading with aria2c: %s (attempt %d/%d)", url, attempt, max_retries)
                if progress_cb:
                    if attempt > 1:
                        await progress_cb(f"retrying ({attempt}/{max_retries})...")
                    else:
                        await progress_cb("downloading...")
                
                result = await _run_aria2c(url, dest_path, progress_cb)
                
                # Verify file exists and has content
                if result.exists() and result.stat().st_size > 0:
                    LOGGER.info("Download complete: %s (%d bytes)", result.name, result.stat().st_size)
                    return result
                else:
                    raise RuntimeError("aria2c completed but file is empty or missing")
                    
            except Exception as exc:
                last_error = str(exc)
                LOGGER.warning("aria2c failed (attempt %d/%d): %s", attempt, max_retries, exc)
                
                # Wait before retry (exponential backoff: 3s, 6s, 12s)
                if attempt < max_retries:
                    wait_time = 3 * (2 ** (attempt - 1))
                    if progress_cb:
                        await progress_cb(f"failed, retrying in {wait_time}s...")
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
