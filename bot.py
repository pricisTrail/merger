"""Video + audio merge bot (Bot API, no re-encode)."""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

from pipeline import (
    ToolMissingError,
    download_telegram_file,
    download_url,
    ensure_dependencies,
    merge_stream_copy,
    upload_with_progress,
)
from progress import ProgressTracker
from utils import (
    AUDIO_EXTS,
    VIDEO_EXTS,
    LinkJob,
    ensure_mp4_name,
    parse_links_text,
    safe_filename,
)

LOGGER = logging.getLogger(__name__)
DATA_DIR = Path(os.getenv("WORK_DIR", "data")).resolve()
DOWNLOAD_CONCURRENCY = int(os.getenv("DOWNLOAD_CONCURRENCY", "4"))
MERGE_CONCURRENCY = int(os.getenv("MERGE_CONCURRENCY", "2"))
KEEP_FILES = os.getenv("KEEP_FILES", "0") == "1"


class MergeStates(StatesGroup):
    waiting_video = State()
    waiting_audio = State()


router = Router()
ACTIVE_TASKS: Dict[int, List[asyncio.Task]] = defaultdict(list)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
MERGE_SEMAPHORE = asyncio.Semaphore(MERGE_CONCURRENCY)


def track_task(chat_id: int, task: asyncio.Task) -> None:
    ACTIVE_TASKS[chat_id].append(task)

    def _cleanup(done: asyncio.Task) -> None:
        try:
            ACTIVE_TASKS[chat_id].remove(done)
        except ValueError:
            return
        if done.cancelled():
            return
        exc = done.exception()
        if exc:
            LOGGER.exception("Background task failed", exc_info=exc)

    task.add_done_callback(_cleanup)


def is_video_document(document: types.Document) -> bool:
    if document.mime_type and document.mime_type.startswith("video/"):
        return True
    if document.file_name:
        return Path(document.file_name).suffix.lower() in VIDEO_EXTS
    return False


def is_audio_document(document: types.Document) -> bool:
    if document.mime_type and document.mime_type.startswith("audio/"):
        return True
    if document.file_name:
        return Path(document.file_name).suffix.lower() in AUDIO_EXTS
    return False


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    return dp


def resolve_output_name(video_name: Optional[str], fallback: str) -> str:
    if video_name:
        stem = Path(video_name).stem
        return ensure_mp4_name(stem)
    return ensure_mp4_name(fallback)


async def process_single_merge(
    bot: Bot,
    chat_id: int,
    video_id: str,
    audio_id: str,
    video_name: Optional[str],
    audio_name: Optional[str],
) -> None:
    job_id = int(time.time())
    output_name = resolve_output_name(video_name, f"merged_{job_id}")
    job_dir = DATA_DIR / str(chat_id) / f"job_{job_id}"
    job_dir.mkdir(parents=True, exist_ok=True)

    tracker = ProgressTracker(bot, chat_id, "Single merge")
    tracker.add_job(1, output_name)
    await tracker.start()

    video_path = job_dir / safe_filename(video_name or f"video_{job_id}.mp4")
    audio_path = job_dir / safe_filename(audio_name or f"audio_{job_id}.mp3")
    output_path = job_dir / safe_filename(output_name)

    async def video_cb(text: str) -> None:
        await tracker.update(1, video=text)

    async def audio_cb(text: str) -> None:
        await tracker.update(1, audio=text)

    async def merge_cb(text: str) -> None:
        await tracker.update(1, merge=text)

    async def upload_cb(text: str) -> None:
        await tracker.update(1, upload=text)

    try:
        await tracker.update(1, video="starting", audio="starting")
        await asyncio.gather(
            download_telegram_file(bot, video_id, video_path, DOWNLOAD_SEMAPHORE, video_cb),
            download_telegram_file(bot, audio_id, audio_path, DOWNLOAD_SEMAPHORE, audio_cb),
        )
        await tracker.update(1, merge="waiting")
        async with MERGE_SEMAPHORE:
            await merge_stream_copy(video_path, audio_path, output_path, merge_cb)
        await tracker.update(1, upload="starting")
        await upload_with_progress(bot, chat_id, output_path, output_name, upload_cb)
        await tracker.update(1, upload="done")
    except Exception as exc:
        await tracker.update(1, merge=f"failed: {exc}", upload="failed")
        LOGGER.exception("Single merge failed", exc_info=exc)
    finally:
        if not KEEP_FILES:
            shutil.rmtree(job_dir, ignore_errors=True)


async def process_link_job(
    bot: Bot,
    chat_id: int,
    job: LinkJob,
    tracker: ProgressTracker,
    external_downloader: Optional[str],
) -> None:
    job_dir = DATA_DIR / str(chat_id) / f"link_{job.index}"
    job_dir.mkdir(parents=True, exist_ok=True)

    output_name = ensure_mp4_name(job.name)
    output_path = job_dir / safe_filename(output_name)
    video_dest = job_dir / "video"
    audio_dest = job_dir / "audio"

    async def video_cb(text: str) -> None:
        await tracker.update(job.index, video=text)

    async def audio_cb(text: str) -> None:
        await tracker.update(job.index, audio=text)

    async def merge_cb(text: str) -> None:
        await tracker.update(job.index, merge=text)

    async def upload_cb(text: str) -> None:
        await tracker.update(job.index, upload=text)

    try:
        await tracker.update(job.index, video="starting", audio="starting")
        video_task = asyncio.create_task(
            download_url(
                job.video,
                video_dest,
                "video",
                DOWNLOAD_SEMAPHORE,
                video_cb,
                external_downloader,
            )
        )
        audio_task = asyncio.create_task(
            download_url(
                job.audio,
                audio_dest,
                "audio",
                DOWNLOAD_SEMAPHORE,
                audio_cb,
                external_downloader,
            )
        )
        video_path, audio_path = await asyncio.gather(video_task, audio_task)
        await tracker.update(job.index, merge="waiting")
        async with MERGE_SEMAPHORE:
            await merge_stream_copy(video_path, audio_path, output_path, merge_cb)
        await tracker.update(job.index, upload="starting")
        await upload_with_progress(bot, chat_id, output_path, output_name, upload_cb)
        await tracker.update(job.index, upload="done")
    except Exception as exc:
        await tracker.update(job.index, merge=f"failed: {exc}", upload="failed")
        LOGGER.exception("Batch job %s failed", job.index, exc_info=exc)
    finally:
        if not KEEP_FILES:
            shutil.rmtree(job_dir, ignore_errors=True)


async def process_link_jobs(bot: Bot, message: types.Message, jobs: List[LinkJob]) -> None:
    if not jobs:
        await message.answer("No valid jobs found in links.txt")
        return
    external_downloader = "aria2c" if shutil.which("aria2c") else None
    tracker = ProgressTracker(bot, message.chat.id, f"Batch ({len(jobs)})")
    for job in jobs:
        tracker.add_job(job.index, job.name)
    await tracker.start()
    tasks = [
        asyncio.create_task(process_link_job(bot, message.chat.id, job, tracker, external_downloader))
        for job in jobs
    ]
    await asyncio.gather(*tasks)


async def start_video_flow(message: types.Message, state: FSMContext, file_id: str, name: str) -> None:
    await state.update_data(video_id=file_id, video_name=name)
    await state.set_state(MergeStates.waiting_audio)
    await message.answer("Video received. Now send the audio file.")


async def start_audio_flow(message: types.Message, state: FSMContext, file_id: str, name: str) -> None:
    data = await state.get_data()
    video_id = data.get("video_id")
    video_name = data.get("video_name")
    if not video_id:
        await message.answer("Please send a video first.")
        return
    await state.clear()
    await message.answer("Processing merge. Progress will appear below.")
    task = asyncio.create_task(
        process_single_merge(message.bot, message.chat.id, video_id, file_id, video_name, name)
    )
    track_task(message.chat.id, task)


@router.message(CommandStart())
async def cmd_start(message: types.Message) -> None:
    await message.answer(
        "Send a video file, then an audio file to merge without re-encoding.\n"
        "For batch mode, send /links and upload links.txt with audio/video/name entries."
    )


@router.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    await message.answer(
        "Flow:\n"
        "1) Send a video file.\n"
        "2) Send an audio file.\n"
        "I will download, merge (stream copy), and upload the result.\n"
        "Batch: /links to send links.txt."
    )


@router.message(Command("links"))
async def cmd_links(message: types.Message) -> None:
    await message.answer(
        "Send links.txt or paste the text in this format:\n\n"
        "audio - <link>\nvideo - <link>\nname - <name>.mp4\n\n"
        "audio1 - <link>\nvideo1 - <link>\nname1 - <name>.mp4"
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Waiting state cleared. Active jobs keep running.")


@router.message(F.video)
async def handle_video(message: types.Message, state: FSMContext) -> None:
    video = message.video
    name = video.file_name or f"video_{message.message_id}.mp4"
    await start_video_flow(message, state, video.file_id, name)


@router.message(F.audio)
async def handle_audio(message: types.Message, state: FSMContext) -> None:
    if await state.get_state() != MergeStates.waiting_audio.state:
        await message.answer("Please send a video first.")
        return
    audio = message.audio
    name = audio.file_name or f"audio_{message.message_id}.mp3"
    await start_audio_flow(message, state, audio.file_id, name)


@router.message(F.document)
async def handle_document(message: types.Message, state: FSMContext) -> None:
    document = message.document
    if not document:
        return
    current_state = await state.get_state()
    if is_audio_document(document):
        if current_state == MergeStates.waiting_audio.state:
            await start_audio_flow(message, state, document.file_id, document.file_name or "audio")
            return
        await message.answer("Please send a video first.")
        return
    if is_video_document(document):
        await start_video_flow(message, state, document.file_id, document.file_name or "video.mp4")
        return
    if document.file_name and document.file_name.lower().endswith(".txt"):
        await handle_links_file(message, document)
        return
    if document.mime_type == "text/plain":
        await handle_links_file(message, document)
        return
    await message.answer("Unsupported document type. Send video/audio or links.txt.")


@router.message(F.text)
async def handle_text(message: types.Message, state: FSMContext) -> None:
    if await state.get_state() == MergeStates.waiting_audio.state:
        await message.answer("Waiting for audio file. Send audio to continue.")
        return
    text = message.text or ""
    jobs = parse_links_text(text)
    if jobs:
        await message.answer("Starting batch merge. Progress will appear below.")
        task = asyncio.create_task(process_link_jobs(message.bot, message, jobs))
        track_task(message.chat.id, task)
        return
    await message.answer("Send a video file or /links for batch mode.")


async def handle_links_file(message: types.Message, document: types.Document) -> None:
    temp_dir = DATA_DIR / str(message.chat.id)
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_path = temp_dir / f"links_{message.message_id}.txt"
    await download_telegram_file(message.bot, document.file_id, temp_path, DOWNLOAD_SEMAPHORE)
    async with aiofiles.open(temp_path, "r", encoding="utf-8") as handle:
        text = await handle.read()
    if not KEEP_FILES:
        temp_path.unlink(missing_ok=True)
    jobs = parse_links_text(text)
    await message.answer("Starting batch merge. Progress will appear below.")
    task = asyncio.create_task(process_link_jobs(message.bot, message, jobs))
    track_task(message.chat.id, task)


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN missing")
    try:
        ensure_dependencies()
    except ToolMissingError as exc:
        raise RuntimeError(str(exc)) from exc

    bot = Bot(token=token)
    dp = build_dispatcher()
    asyncio.run(dp.start_polling(bot))


if __name__ == "__main__":
    main()
