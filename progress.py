from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

EDIT_THROTTLE_SECONDS = 1.0
MAX_PROGRESS_LINES = 20

def get_premium_status(stage: str, text: str) -> str:
    """Parses standard status text and returns a beautified HTML card."""
    if not text or text in ["queued", "done", "starting", "processing", "waiting"]:
        return f"   » <b>{stage}</b>: <code>{text or 'queued'}</code>"

    # Try to extract data: 99% 1.1GiB/1.1GiB 2.8MiB/s ETA 13s
    import re
    pct = re.search(r"(\d+\.?\d*)%", text)
    sz = re.search(r"(\d+\.?\d*[KMGT]i?B/\d+\.?\d*[KMGT]i?B)", text)
    spd = re.search(r"(\d+\.?\d*[KMGT]i?B/s)", text)
    eta = re.search(r"ETA\s*(\w+)", text)
    
    if not pct:
        return f"   » <b>{stage}</b>: <code>{text}</code>"

    percentage = float(pct.group(1))
    filled = int(percentage // 10)
    bar = "🟧" * filled + "⬜" * (10 - filled)
    
    # Using blockquote for the premium feel
    card = [
        f"<blockquote>✦ {bar} ✦",
        f"» 🔋 <b>Percentage</b> • <code>{percentage:.1f}%</code>",
    ]
    if spd: card.append(f"» 🚀 <b>Speed</b> • <code>{spd.group(1)}</code>")
    if sz: card.append(f"» 🚦 <b>Size</b> • <code>{sz.group(1)}</code>")
    if eta: card.append(f"» ⏰ <b>ETA</b> • <code>{eta.group(1)}</code></blockquote>")
    else: card[-1] += "</blockquote>"
    
    return "\n".join(card)


@dataclass
class JobStatus:
    name: str
    video: str = "queued"
    audio: str = "queued"
    merge: str = "queued"
    upload: str = "queued"


class ProgressTracker:
    def __init__(self, bot: Bot, chat_id: int, title: str) -> None:
        self.bot = bot
        self.chat_id = chat_id
        self.title = title
        self.message_id: Optional[int] = None
        self.jobs: "OrderedDict[int, JobStatus]" = OrderedDict()
        self._lock = asyncio.Lock()
        self._last_edit = 0.0

    async def start(self) -> None:
        if self.message_id is not None:
            return
        message = await self.bot.send_message(self.chat_id, self.render())
        self.message_id = message.message_id

    def add_job(self, job_id: int, name: str) -> None:
        self.jobs[job_id] = JobStatus(name=name)

    async def update(self, job_id: int, **kwargs: str) -> None:
        job = self.jobs[job_id]
        for key, value in kwargs.items():
            setattr(job, key, value)
        await self.refresh()

    async def refresh(self, force: bool = False) -> None:
        if self.message_id is None:
            await self.start()
        async with self._lock:
            now = time.monotonic()
            if not force and now - self._last_edit < EDIT_THROTTLE_SECONDS:
                return
            text = self.render()
            try:
                await self.bot.edit_message_text(
                    text=text,
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                )
            except TelegramBadRequest as exc:
                if "message is not modified" not in str(exc).lower():
                    raise
            self._last_edit = time.monotonic()

    def render(self) -> str:
        lines = [f"📥 <b>{self.title}</b>\n"]
        for job_id, job in self.jobs.items():
            lines.append(f"📁 <b>{job_id}. {job.name}</b>")
            
            lines.append(get_premium_status("Video", job.video))
            lines.append(get_premium_status("Audio", job.audio))
            lines.append(get_premium_status("Merge", job.merge))
            lines.append(get_premium_status("Upload", job.upload))
            lines.append("") # Spacer
            
        if len(lines) > MAX_PROGRESS_LINES:
            lines = lines[:MAX_PROGRESS_LINES]
            lines.append("<i>... (more jobs being tracked)</i>")
        return "\n".join(lines)
