from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlparse

VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v"}
AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".opus"}

INVALID_NAME_RE = re.compile(r"[<>:\\|?*\"/]")
LINK_LINE_RE = re.compile(r"^(audio|video|name)(\d*)\s*-\s*(.+)$", re.IGNORECASE)


@dataclass
class LinkJob:
    index: int
    audio: str
    video: str
    name: str


def format_bytes(value: float | int | None) -> str:
    if value is None:
        return "?"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
    return f"{size:.1f}TB"


def format_speed(value: float | int | None) -> str:
    if value is None:
        return "?"
    return f"{format_bytes(value)}/s"


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    total = int(seconds)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def ensure_mp4_name(name: str) -> str:
    clean = name.strip().strip("\"")
    if not clean:
        return "merged.mp4"
    if Path(clean).suffix:
        return clean
    return f"{clean}.mp4"


def safe_filename(name: str, fallback: str = "file") -> str:
    clean = INVALID_NAME_RE.sub("_", name.strip())
    return clean or fallback


def is_direct_url(url: str) -> bool:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    return suffix in VIDEO_EXTS.union(AUDIO_EXTS)


def guess_filename(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    name = name.split("?")[0]
    if not name:
        return fallback
    return safe_filename(name)


def parse_links_text(text: str) -> List[LinkJob]:
    buckets: Dict[int, Dict[str, str]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = LINK_LINE_RE.match(line)
        if not match:
            continue
        key = match.group(1).lower()
        index_value = match.group(2).strip()
        index = int(index_value) if index_value else 1
        value = match.group(3).strip()
        bucket = buckets.setdefault(index, {})
        bucket[key] = value
    jobs: List[LinkJob] = []
    for index in sorted(buckets):
        entry = buckets[index]
        audio = entry.get("audio")
        video = entry.get("video")
        if not audio or not video:
            continue
        name = ensure_mp4_name(entry.get("name", f"merge_{index}"))
        jobs.append(LinkJob(index=index, audio=audio, video=video, name=name))
    return jobs
