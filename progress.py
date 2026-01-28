from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

EDIT_THROTTLE_SECONDS = 1.0
MAX_PROGRESS_LINES = 14

def get_progress_bar(text: str) -> str:
    """Extracts percentage from text and returns a visual bar or empty string."""
    if not text or "%" not in text:
        return ""
    try:
        # Match percentage like 45.2% or 45%
        import re
        match = re.search(r"(\d+\.?\d*)%", text)
        if not match:
            return ""
        percentage = float(match.group(1))
        # Create a 10-char bar
        filled = int(percentage // 10)
        bar = "▰" * filled + "▱" * (10 - filled)
        return f"\n      └ {bar} {percentage:.1f}%"
    except Exception:
        return ""


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
        lines = [f"✨ **{self.title}**"]
        for job_id, job in self.jobs.items():
            lines.append(f"📦 **{job_id}. {job.name}**")
            
            # Create sub-status lines
            v_bar = get_progress_bar(job.video)
            a_bar = get_progress_bar(job.audio)
            m_bar = get_progress_bar(job.merge)
            u_bar = get_progress_bar(job.upload)
            
            lines.append(f"   🔹 V: `{job.video or 'queued'}`")
            if v_bar: lines.append(v_bar)
            lines.append(f"   🔹 A: `{job.audio or 'queued'}`")
            if a_bar: lines.append(a_bar)
            lines.append(f"   🔹 M: `{job.merge or 'queued'}`")
            if m_bar: lines.append(m_bar)
            lines.append(f"   🔹 U: `{job.upload or 'queued'}`")
            if u_bar: lines.append(u_bar)
            lines.append("") # Spacer
            
        if len(lines) > MAX_PROGRESS_LINES:
            # Note: with bars, we might hit the limit faster
            lines = lines[:MAX_PROGRESS_LINES]
            lines.append("... (more jobs below)")
        return "\n".join(lines)
