from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Optional

from aiogram import Bot, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder

EDIT_THROTTLE_SECONDS = 1.0
MAX_PROGRESS_LINES = 20
START_RETRY_SECONDS = 5.0
LOGGER = logging.getLogger(__name__)

def get_premium_status(stage: str, text: str) -> str:
    """Parses standard status text and returns a beautified text card."""
    if not text or text in ["queued", "done", "starting", "processing", "waiting"]:
        return f"   » {stage}: {text or 'queued'}"

    # Try to extract data: 99% 1.1GiB/1.1GiB 2.8MiB/s ETA 13s
    import re
    pct = re.search(r"(\d+\.?\d*)%", text)
    sz = re.search(r"(\d+\.?\d*[KMGT]i?B/\d+\.?\d*[KMGT]i?B)", text)
    spd = re.search(r"(\d+\.?\d*[KMGT]i?B/s)", text)
    eta = re.search(r"ETA\s*(\w+)", text)
    
    if not pct:
        return f"   » {stage}: {text}"

    percentage = float(pct.group(1))
    filled = int(percentage // 10)
    bar = "🟧" * filled + "⬜" * (10 - filled)
    
    # Header for the block
    header = f"\n     📁 {stage} 📁 "
    
    # Mode logic
    mode = "Download"
    if stage == "Merge": mode = "Merging"
    if stage == "Upload": mode = "Upload"

    card = [
        header,
        f"   ✦ {bar} ✦",
        f"   » 🔋 Percentage • {percentage:.1f}%",
    ]
    if spd: card.append(f"   » 🚀 Speed • {spd.group(1)}")
    if sz: card.append(f"   » 🚦 Size • {sz.group(1)}")
    if eta: card.append(f"   » ⏰ ETA • {eta.group(1)}")
    
    card.append(f"   » Mode: {mode}")
    
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
        self._start_retry_after = 0.0
        self.current_page = 0 # 0-indexed

    async def start(self) -> None:
        if self.message_id is not None:
            return
        now = time.monotonic()
        if now < self._start_retry_after:
            return
        try:
            message = await self.bot.send_message(
                self.chat_id,
                self.render(),
                reply_markup=self.get_markup()
            )
        except Exception as exc:
            self._start_retry_after = time.monotonic() + START_RETRY_SECONDS
            LOGGER.warning(
                "Progress tracker start failed for chat %s; retrying: %s",
                self.chat_id,
                exc,
            )
            return
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
            if self.message_id is None:
                return
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
                    reply_markup=self.get_markup()
                )
            except TelegramBadRequest as exc:
                error_text = str(exc).lower()
                if "message is not modified" in error_text:
                    pass
                else:
                    # Message can disappear (deleted, not editable). Continue job and retry later.
                    if "message to edit not found" in error_text or "message can't be edited" in error_text:
                        self.message_id = None
                        self._start_retry_after = time.monotonic() + START_RETRY_SECONDS
                    LOGGER.warning(
                        "Progress tracker edit issue for chat %s: %s",
                        self.chat_id,
                        exc,
                    )
            except Exception as exc:
                LOGGER.warning(
                    "Progress tracker edit failed for chat %s; continuing: %s",
                    self.chat_id,
                    exc,
                )
            self._last_edit = time.monotonic()

    def render(self) -> str:
        job_ids = list(self.jobs.keys())
        total_jobs = len(job_ids)
        if total_jobs == 0:
            return f"📥 {self.title}\nWaiting for jobs..."
        
        # Ensure page is in range
        if self.current_page >= total_jobs:
            self.current_page = total_jobs - 1
        if self.current_page < 0:
            self.current_page = 0

        job_id = job_ids[self.current_page]
        job = self.jobs[job_id]
        
        lines = [
            f"📥 {self.title}",
            f"🗄 {self.current_page + 1} / {total_jobs}",
            f"📁 {job.name}\n"
        ]
        
        lines.append(get_premium_status("Video", job.video))
        lines.append(get_premium_status("Audio", job.audio))
        lines.append(get_premium_status("Merge", job.merge))
        lines.append(get_premium_status("Upload", job.upload))
        
        return "\n".join(lines)

    def get_markup(self) -> Optional[types.InlineKeyboardMarkup]:
        total_jobs = len(self.jobs)
        if total_jobs <= 1:
            return None
            
        builder = InlineKeyboardBuilder()
        # Prev [Page/Total] Next
        builder.button(text="⏪ Prev", callback_data=f"page_prev_{self.chat_id}")
        builder.button(text=f"{self.current_page + 1} / {total_jobs}", callback_data="noop")
        builder.button(text="Next ⏩", callback_data=f"page_next_{self.chat_id}")
        builder.adjust(3)
        return builder.as_markup()
