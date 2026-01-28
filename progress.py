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
        lines = [f"MERGE STATUS: {self.title}"]
        for job_id, job in self.jobs.items():
            lines.append(f"{job_id}. {job.name}")
            lines.append(
                f"   V: {job.video} | A: {job.audio} | M: {job.merge} | U: {job.upload}"
            )
        if len(lines) > MAX_PROGRESS_LINES:
            remaining = len(lines) - MAX_PROGRESS_LINES + 1
            lines = lines[: MAX_PROGRESS_LINES - 1]
            lines.append(f"... and {remaining} more lines")
        return "\n".join(lines)
