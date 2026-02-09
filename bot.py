"""Video + audio merge bot (Bot API, no re-encode)."""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, List, Optional

import aiofiles
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
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
    get_user_settings,
    parse_links_text,
    safe_filename,
    set_user_settings,
)

LOGGER = logging.getLogger(__name__)
DATA_DIR = Path(os.getenv("WORK_DIR", "data")).resolve()
# Force fully-serial processing for file merges.
DOWNLOAD_CONCURRENCY = 1
MERGE_CONCURRENCY = 1
KEEP_FILES = os.getenv("KEEP_FILES", "0") == "1"
PORT = int(os.getenv("PORT", "8000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")


class MergeStates(StatesGroup):
    waiting_audio = State()
    waiting_video = State()
    waiting_name = State()


@dataclass
class QueuedTelegramMergeJob:
    queue_id: int
    chat_id: int
    video_id: str
    audio_id: str
    video_name: str
    audio_name: str
    output_name: str


router = Router()
ACTIVE_TASKS: Dict[int, List[asyncio.Task]] = defaultdict(list)
ACTIVE_TRACKERS: Dict[int, List[ProgressTracker]] = defaultdict(list)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
MERGE_SEMAPHORE = asyncio.Semaphore(MERGE_CONCURRENCY)

MERGE_QUEUE: Deque[QueuedTelegramMergeJob] = deque()
QUEUE_LOCK = asyncio.Lock()
QUEUE_EVENT = asyncio.Event()
QUEUE_NEXT_ID = 1
RUNNING_QUEUE_JOB: Optional[QueuedTelegramMergeJob] = None
RUNNING_QUEUE_TASK: Optional[asyncio.Task] = None
QUEUE_WORKER_TASK: Optional[asyncio.Task] = None
QUEUE_SHUTTING_DOWN = False


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


async def enqueue_telegram_merge(job: QueuedTelegramMergeJob) -> int:
    async with QUEUE_LOCK:
        MERGE_QUEUE.append(job)
        position = len(MERGE_QUEUE)
        if RUNNING_QUEUE_JOB is not None:
            position += 1
        QUEUE_EVENT.set()
        return position


async def build_queued_merge_job(
    chat_id: int,
    video_id: str,
    audio_id: str,
    video_name: str,
    audio_name: str,
    output_name: str,
) -> QueuedTelegramMergeJob:
    global QUEUE_NEXT_ID
    async with QUEUE_LOCK:
        queue_id = QUEUE_NEXT_ID
        QUEUE_NEXT_ID += 1
    return QueuedTelegramMergeJob(
        queue_id=queue_id,
        chat_id=chat_id,
        video_id=video_id,
        audio_id=audio_id,
        video_name=video_name,
        audio_name=audio_name,
        output_name=output_name,
    )


async def queue_counts(chat_id: int) -> tuple[int, int]:
    async with QUEUE_LOCK:
        running = 1 if RUNNING_QUEUE_JOB and RUNNING_QUEUE_JOB.chat_id == chat_id else 0
        queued = sum(1 for item in MERGE_QUEUE if item.chat_id == chat_id)
        return running, queued


async def prune_queued_jobs(chat_id: int) -> int:
    async with QUEUE_LOCK:
        before = len(MERGE_QUEUE)
        kept = deque(item for item in MERGE_QUEUE if item.chat_id != chat_id)
        removed = before - len(kept)
        MERGE_QUEUE.clear()
        MERGE_QUEUE.extend(kept)
        if not MERGE_QUEUE and RUNNING_QUEUE_JOB is None:
            QUEUE_EVENT.clear()
        return removed


async def cancel_running_job_for_chat(chat_id: int) -> bool:
    async with QUEUE_LOCK:
        if (
            RUNNING_QUEUE_JOB is None
            or RUNNING_QUEUE_JOB.chat_id != chat_id
            or RUNNING_QUEUE_TASK is None
            or RUNNING_QUEUE_TASK.done()
        ):
            return False
        RUNNING_QUEUE_TASK.cancel()
        return True


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
    ACTIVE_TRACKERS[chat_id].append(tracker)
    await tracker.start()

    # Prefix filenames to prevent collision when video and audio have same name
    video_path = job_dir / safe_filename(f"v_{video_name}" if video_name else f"v_video_{job_id}.mp4")
    audio_path = job_dir / safe_filename(f"a_{audio_name}" if audio_name else f"a_audio_{job_id}.mp3")
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
        if tracker in ACTIVE_TRACKERS[chat_id]:
            ACTIVE_TRACKERS[chat_id].remove(tracker)
        if not KEEP_FILES:
            shutil.rmtree(job_dir, ignore_errors=True)


async def process_single_merge_with_name(
    bot: Bot,
    chat_id: int,
    video_id: str,
    audio_id: str,
    video_name: str,
    audio_name: str,
    output_name: str,
) -> None:
    job_id = int(time.time())
    output_name = ensure_mp4_name(output_name)
    job_dir = DATA_DIR / str(chat_id) / f"job_{job_id}"
    job_dir.mkdir(parents=True, exist_ok=True)

    tracker = ProgressTracker(bot, chat_id, "Single Merge")
    tracker.add_job(1, output_name)
    ACTIVE_TRACKERS[chat_id].append(tracker)
    await tracker.start()

    # Prefix filenames to prevent collision when video and audio have same name
    video_path = job_dir / safe_filename(f"v_{video_name}")
    audio_path = job_dir / safe_filename(f"a_{audio_name}")
    output_path = job_dir / safe_filename(output_name)

    async def video_cb(text: str) -> None:
        await tracker.update(1, video=text)

    async def audio_cb(text: str) -> None:
        await tracker.update(1, audio=text)

    async def merge_cb(text: str) -> None:
        await tracker.update(1, merge=text)

    async def upload_cb(text: str) -> None:
        await tracker.update(1, upload=text)

    # Get user destination
    settings = get_user_settings(DATA_DIR, chat_id)
    dest = settings.get("target_channel")

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
        await upload_with_progress(bot, chat_id, output_path, output_name, upload_cb, destination_id=dest)
    except Exception as exc:
        await tracker.update(1, merge=f"failed: {exc}", upload="failed")
        LOGGER.exception("Single merge failed", exc_info=exc)
    finally:
        if tracker in ACTIVE_TRACKERS[chat_id]:
            ACTIVE_TRACKERS[chat_id].remove(tracker)
        if not KEEP_FILES:
            shutil.rmtree(job_dir, ignore_errors=True)


async def merge_queue_worker(bot: Bot) -> None:
    global RUNNING_QUEUE_JOB, RUNNING_QUEUE_TASK
    while True:
        await QUEUE_EVENT.wait()
        async with QUEUE_LOCK:
            if not MERGE_QUEUE:
                QUEUE_EVENT.clear()
                continue
            job = MERGE_QUEUE.popleft()
            RUNNING_QUEUE_JOB = job
            if not MERGE_QUEUE:
                QUEUE_EVENT.clear()
        try:
            RUNNING_QUEUE_TASK = asyncio.create_task(
                process_single_merge_with_name(
                    bot=bot,
                    chat_id=job.chat_id,
                    video_id=job.video_id,
                    audio_id=job.audio_id,
                    video_name=job.video_name,
                    audio_name=job.audio_name,
                    output_name=job.output_name,
                )
            )
            await RUNNING_QUEUE_TASK
        except asyncio.CancelledError:
            if QUEUE_SHUTTING_DOWN:
                raise
            LOGGER.info("Queued merge job %s was cancelled.", job.queue_id)
        except Exception as exc:
            LOGGER.exception("Queued merge job %s failed", job.queue_id, exc_info=exc)
        finally:
            RUNNING_QUEUE_TASK = None
            async with QUEUE_LOCK:
                RUNNING_QUEUE_JOB = None
                if MERGE_QUEUE:
                    QUEUE_EVENT.set()


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

    # Get user destination
    settings = get_user_settings(DATA_DIR, chat_id)
    dest = settings.get("target_channel")

    video_path = None
    audio_path = None

    try:
        # Phase 1: Download video and audio
        await tracker.update(job.index, video="starting", audio="starting")
        
        try:
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
        except Exception as exc:
            # Determine which download failed
            error_msg = str(exc)
            if "video" in error_msg.lower() or (video_path is None):
                await tracker.update(job.index, video=f"failed: {exc}", merge="skipped", upload="skipped")
            else:
                await tracker.update(job.index, audio=f"failed: {exc}", merge="skipped", upload="skipped")
            LOGGER.exception("Download failed for job %s", job.index, exc_info=exc)
            return
        
        # Phase 2: Merge
        await tracker.update(job.index, merge="waiting")
        try:
            async with MERGE_SEMAPHORE:
                await merge_stream_copy(video_path, audio_path, output_path, merge_cb)
        except Exception as exc:
            await tracker.update(job.index, merge=f"failed: {exc}", upload="skipped")
            LOGGER.exception("Merge failed for job %s", job.index, exc_info=exc)
            return
        
        # Phase 3: Upload
        await tracker.update(job.index, upload="starting")
        try:
            await upload_with_progress(bot, chat_id, output_path, output_name, upload_cb, destination_id=dest)
            await tracker.update(job.index, upload="done")
        except Exception as exc:
            await tracker.update(job.index, upload=f"failed: {exc}")
            LOGGER.exception("Upload failed for job %s", job.index, exc_info=exc)
            
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
    ACTIVE_TRACKERS[message.chat.id].append(tracker)
    for job in jobs:
        tracker.add_job(job.index, job.name)
    await tracker.start()
    tasks = [
        asyncio.create_task(process_link_job(bot, message.chat.id, job, tracker, external_downloader))
        for job in jobs
    ]
    await asyncio.gather(*tasks)
    if tracker in ACTIVE_TRACKERS[message.chat.id]:
        ACTIVE_TRACKERS[message.chat.id].remove(tracker)


@router.message(Command("single"))
async def cmd_single(message: types.Message, state: FSMContext) -> None:
    await state.set_data({"mode": "single"})
    await state.set_state(MergeStates.waiting_audio)
    await message.answer("\U0001F3B5 **Step 1/3**: Please send the **Audio** file.")


@router.message(Command("batch"))
async def cmd_batch(message: types.Message, state: FSMContext) -> None:
    await state.set_data({"mode": "batch", "batch_items": []})
    await state.set_state(MergeStates.waiting_audio)
    await message.answer(
        "Batch mode started.\n"
        "Send jobs as Audio -> Video -> Output Name.\n"
        "Send /done when finished.\n\n"
        "\U0001F3B5 **Step 1/3**: Please send the **Audio** file."
    )


@router.message(Command("done"))
async def cmd_done(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("No active /batch flow. Use /batch to start.")
        return

    data = await state.get_data()
    if data.get("mode") != "batch":
        await message.answer("/done only works in /batch mode.")
        return

    items = data.get("batch_items", [])
    if not items:
        await message.answer("No completed batch items yet. Add at least one job before /done.")
        return

    if current_state != MergeStates.waiting_audio:
        await message.answer("In-progress item is incomplete and will be skipped.")

    await state.clear()
    await message.answer(f"Queued {len(items)} item(s). Processing will run one-by-one.")

    for item in items:
        job = await build_queued_merge_job(
            chat_id=message.chat.id,
            video_id=item["video_id"],
            audio_id=item["audio_id"],
            video_name=item["video_name"],
            audio_name=item["audio_name"],
            output_name=item["output_name"],
        )
        position = await enqueue_telegram_merge(job)
        if position == 1:
            await message.answer(
                "\u2705 Starting merge for **{}**. Progress shown below.".format(item["output_name"])
            )
        else:
            await message.answer(
                "\U0001F552 Queued **{}** at position **{}**.".format(item["output_name"], position)
            )


async def start_audio_flow(message: types.Message, state: FSMContext, file_id: str, name: str) -> None:
    await state.update_data(audio_id=file_id, audio_name=name)
    await state.set_state(MergeStates.waiting_video)
    await message.answer("\U0001F4F9 **Step 2/3**: Audio received. Now send the **Video** file.")


async def start_video_flow(message: types.Message, state: FSMContext, file_id: str, name: str) -> None:
    await state.update_data(video_id=file_id, video_name=name)
    await state.set_state(MergeStates.waiting_name)
    await message.answer("\u270F\uFE0F **Step 3/3**: Video received. Now send the **Output Name** for your file (e.g. MyVideo).")


@router.message(CommandStart())
async def cmd_start(message: types.Message) -> None:
    await message.answer(
        "\U0001F680 <b>Telegram Multi-Merger Bot</b>\n\n"
        "I can merge video and audio without re-encoding.\n\n"
        "\U0001F539 <b>/single</b> - One interactive merge (Audio -> Video -> Name).\n"
        "\U0001F539 <b>/batch</b> - Keep adding jobs in 3 steps until <b>/done</b>.\n"
        "\U0001F539 <b>/links</b> - Batch merge using links.txt.\n\n"
        "\u2699\uFE0F <b>Settings</b>\n"
        "\U0001F539 <b>/setchannel</b> <code>ID</code> - Set destination channel.\n"
        "\U0001F539 <b>/channel</b> - Show current destination.\n"
        "\U0001F539 <b>/removechannel</b> - Reset to private chat.\n\n"
        "\U0001F4CA <b>Other</b>\n"
        "\U0001F539 <b>/status</b> - Check running and queued jobs.\n"
        "\U0001F539 <b>/stop</b> - Cancel running jobs and clear queue.\n"
        "\U0001F539 <b>/cancel</b> - Clear current input flow."
    )


@router.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    text = (
        "\U0001F4D6 <b>Telegram Merger Bot - Guide</b>\n\n"
        "Core commands:\n"
        "<blockquote>"
        "\U0001F539 <b>/single</b> - One merge in 3 steps.\n"
        "\U0001F539 <b>/batch</b> - Keep adding jobs in 3 steps, then send <b>/done</b>.\n"
        "\U0001F539 <b>/links</b> - Batch mode from links text or file.\n"
        "\U0001F539 <b>/cancel</b> - Reset current conversation flow.\n"
        "</blockquote>\n"
        "Queue behavior:\n"
        "<blockquote>"
        "Only one merge job runs at a time. Remaining jobs are queued and processed in order."
        "</blockquote>\n"
        "Destination commands:\n"
        "<blockquote>"
        "\U0001F539 <b>/setchannel</b> <code>[ID/@User]</code>\n"
        "\U0001F539 <b>/channel</b>\n"
        "\U0001F539 <b>/removechannel</b>"
        "</blockquote>\n"
        "Monitoring:\n"
        "<blockquote>"
        "\U0001F539 <b>/status</b> - Running + queued overview.\n"
        "\U0001F539 <b>/stop</b> - Cancel running jobs and clear your queue."
        "</blockquote>"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("links"))
async def cmd_links(message: types.Message) -> None:
    await message.answer(
        "📂 <b>Batch Mode Configuration</b>\n\n"
        "Send a <code>.txt</code> file or paste text in this format:\n\n"
        "<code>audio - &lt;link&gt;</code>\n"
        "<code>video - &lt;link&gt;</code>\n"
        "<code>name  - &lt;name&gt;.mp4</code>"
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("🧹 **State Cleared.** All waiting inputs have been reset. Type /status to see running jobs.")


@router.message(Command("status"))
async def cmd_status(message: types.Message) -> None:
    chat_id = message.chat.id
    tasks = ACTIVE_TASKS.get(chat_id, [])
    trackers = ACTIVE_TRACKERS.get(chat_id, [])
    running_queue, queued_queue = await queue_counts(chat_id)

    if not tasks and not trackers and not running_queue and not queued_queue:
        await message.answer("\u2705 <b>No active jobs</b> for this chat.")
        return

    try:
        import psutil

        usage = (
            f"\U0001F5A5 <b>System Load</b>: {psutil.cpu_percent()}% | "
            f"<b>RAM</b>: {psutil.virtual_memory().percent}%"
        )
    except ImportError:
        usage = "\U0001F5A5 System tracking not available"

    text = (
        f"\U0001F4CA <b>Detailed Status for {message.from_user.first_name}</b>\n"
        f"\U0001F3C3 <b>Active Processes</b>: {len(tasks) + running_queue}\n"
        f"\U0001F9FE <b>Queued Items</b>: {queued_queue}\n"
        f"{usage}\n\n"
    )

    for tracker in trackers:
        text += tracker.render() + "\n"

    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("stop"))
async def cmd_stop(message: types.Message) -> None:
    chat_id = message.chat.id
    tasks = ACTIVE_TASKS.get(chat_id, [])

    cancelled_tasks = 0
    for task in tasks:
        task.cancel()
        cancelled_tasks += 1

    removed_queue = await prune_queued_jobs(chat_id)
    cancelled_running_queue = await cancel_running_job_for_chat(chat_id)

    total = cancelled_tasks + removed_queue + (1 if cancelled_running_queue else 0)
    if total == 0:
        await message.answer("No active jobs found for this chat.")
        return

    await message.answer(
        f"Cancelled {cancelled_tasks} active task(s), "
        f"removed {removed_queue} queued item(s), "
        f"running queued job cancelled: {'yes' if cancelled_running_queue else 'no'}."
    )


@router.message(Command("setchannel"))
async def cmd_setchannel(message: types.Message) -> None:
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("⚠️ <b>Usage</b>: <code>/setchannel -100123456789</code> or <code>/setchannel @MyChannel</code>")
        return
    
    target = args[1].strip()
    # If it's a number, convert to int
    if target.startswith("-") and target[1:].isdigit():
        target = int(target)
    
    try:
        # Verify bot is admin or at least the channel exists
        test = await message.bot.get_chat(target)
        target_id = test.id
        title = test.title or test.full_name
        
        settings = get_user_settings(DATA_DIR, message.chat.id)
        settings["target_channel"] = target_id
        set_user_settings(DATA_DIR, message.chat.id, settings)
        
        await message.answer(f"✅ <b>Success!</b> Merged files will now be uploaded to: <b>{title}</b> (<code>{target_id}</code>)")
    except Exception as e:
        await message.answer(f"❌ <b>Error</b>: Could not set channel. Make sure the bot is an Admin there.\n\n<code>{str(e)}</code>")


@router.message(Command("channel"))
async def cmd_channel(message: types.Message) -> None:
    settings = get_user_settings(DATA_DIR, message.chat.id)
    dest = settings.get("target_channel")
    
    if not dest:
        await message.answer("📍 <b>Current Destination</b>: Your Private Chat (PM).")
        return
    
    try:
        chat = await message.bot.get_chat(dest)
        await message.answer(f"📍 <b>Current Destination</b>: <b>{chat.title or chat.full_name}</b> (<code>{dest}</code>)")
    except Exception:
        await message.answer(f"📍 <b>Current Destination</b>: <code>{dest}</code> (Cannot fetch info, check admin rights).")


@router.message(Command("removechannel"))
async def cmd_removechannel(message: types.Message) -> None:
    settings = get_user_settings(DATA_DIR, message.chat.id)
    if "target_channel" in settings:
        del settings["target_channel"]
        set_user_settings(DATA_DIR, message.chat.id, settings)
        await message.answer("✅ <b>Destination Reset!</b> Files will now be uploaded to your Private Chat.")
    else:
        await message.answer("💡 You don't have a channel set.")


@router.message(F.audio)
async def handle_audio(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state != MergeStates.waiting_audio:
        return # Ignore unsolicited audio if not in flow
    audio = message.audio
    name = audio.file_name or f"audio_{message.message_id}.mp3"
    await start_audio_flow(message, state, audio.file_id, name)


@router.message(F.video)
async def handle_video(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state != MergeStates.waiting_video:
        await message.answer("Please send /single or /batch to start the merge flow.")
        return
    video = message.video
    name = video.file_name or f"video_{message.message_id}.mp4"
    await start_video_flow(message, state, video.file_id, name)


@router.message(F.document)
async def handle_document(message: types.Message, state: FSMContext) -> None:
    document = message.document
    if not document:
        return
    current_state = await state.get_state()
    
    # Handle .txt files (links batch) - only when NOT in /single flow
    if document.file_name and document.file_name.lower().endswith(".txt"):
        if current_state not in (MergeStates.waiting_audio, MergeStates.waiting_video):
            await handle_links_file(message, document)
            return
    
    # /single flow: Step 1 - Accept ANY file as audio
    if current_state == MergeStates.waiting_audio:
        await start_audio_flow(message, state, document.file_id, document.file_name or "audio")
        return
    
    # /single flow: Step 2 - Accept ANY file as video
    if current_state == MergeStates.waiting_video:
        await start_video_flow(message, state, document.file_id, document.file_name or "video")
        return
    
    # Not in /single flow - handle normally
    if document.file_name and document.file_name.lower().endswith(".txt"):
        await handle_links_file(message, document)
        return
    
    await message.answer("Use /single or /batch to start merging, or send a links.txt file for links batch.")


@router.message(F.text)
async def handle_text(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    
    # Handle the "Name" step of /single or /batch flow.
    if current_state == MergeStates.waiting_name:
        data = await state.get_data()
        mode = data.get("mode", "single")
        output_name = ensure_mp4_name(message.text.strip())

        if mode == "batch":
            items = list(data.get("batch_items", []))
            items.append(
                {
                    "video_id": data["video_id"],
                    "audio_id": data["audio_id"],
                    "video_name": data["video_name"],
                    "audio_name": data["audio_name"],
                    "output_name": output_name,
                }
            )
            await state.set_data({"mode": "batch", "batch_items": items})
            await state.set_state(MergeStates.waiting_audio)
            await message.answer(
                f"Saved item **{len(items)}** as **{output_name}**.\n"
                "Send next audio file, or /done to start queued processing.\n\n"
                "\U0001F3B5 **Step 1/3**: Please send the **Audio** file."
            )
            return

        await state.clear()
        job = await build_queued_merge_job(
            chat_id=message.chat.id,
            video_id=data["video_id"],
            audio_id=data["audio_id"],
            video_name=data["video_name"],
            audio_name=data["audio_name"],
            output_name=output_name,
        )
        position = await enqueue_telegram_merge(job)
        if position == 1:
            await message.answer(f"\u2705 Starting merge for **{output_name}**. Progress shown below.")
        else:
            await message.answer(f"\U0001F552 Queued **{output_name}** at position **{position}**.")
        return

    # Handle Batch paste
    text = message.text or ""
    jobs = parse_links_text(text)
    if jobs:
        await message.answer("🚀 Starting batch merge. Progress shown below.")
        task = asyncio.create_task(process_link_jobs(message.bot, message, jobs))
        track_task(message.chat.id, task)
        return
    
    await message.answer("Send /single or /batch for guided mode, or /links for links-based batch mode.")


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

@router.callback_query(F.data.startswith("page_"))
async def handle_pagination(callback: types.CallbackQuery) -> None:
    data = callback.data
    chat_id = int(data.split("_")[-1])
    action = data.split("_")[1] # prev or next
    
    trackers = ACTIVE_TRACKERS.get(chat_id, [])
    if not trackers:
        await callback.answer("❌ Status tracker no longer active.")
        return
        
    # We navigate the latest tracker for that chat
    tracker = trackers[-1]
    total = len(tracker.jobs)
    
    if action == "prev":
        tracker.current_page = (tracker.current_page - 1) % total
    elif action == "next":
        tracker.current_page = (tracker.current_page + 1) % total

    await tracker.refresh(force=True)
    await callback.answer()

def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN missing")
    webhook_url = os.getenv("WEBHOOK_URL")
    webhook_path_env = os.getenv("WEBHOOK_PATH", "/webhook")
    webhook_secret = os.getenv("WEBHOOK_SECRET")
    port = int(os.getenv("PORT", "8000"))
    if not webhook_url:
        raise RuntimeError("WEBHOOK_URL missing (public base url, e.g. https://<app>.koyeb.app)")
    try:
        ensure_dependencies()
    except ToolMissingError as exc:
        raise RuntimeError(str(exc)) from exc

    webhook_base = webhook_url.rstrip("/")
    webhook_path = webhook_path_env if webhook_path_env.startswith("/") else f"/{webhook_path_env}"
    webhook_full = f"{webhook_base}{webhook_path}"

    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = build_dispatcher()

    async def _set_webhook_background() -> None:
        try:
            LOGGER.info("Setting webhook to %s ...", webhook_full)
            await asyncio.wait_for(
                bot.set_webhook(
                    webhook_full, 
                    secret_token=webhook_secret if webhook_secret else None,
                    drop_pending_updates=True
                ),
                timeout=15,
            )
            info = await bot.get_webhook_info()
            LOGGER.info("Webhook set successfully! Current info: %s", info)
        except Exception as exc:
            LOGGER.exception("CRITICAL: Failed to set webhook to %s", webhook_full, exc_info=exc)

    async def on_startup(*_args, **_kwargs) -> None:
        global QUEUE_WORKER_TASK, QUEUE_SHUTTING_DOWN
        QUEUE_SHUTTING_DOWN = False
        asyncio.create_task(_set_webhook_background())
        if QUEUE_WORKER_TASK is None or QUEUE_WORKER_TASK.done():
            QUEUE_WORKER_TASK = asyncio.create_task(merge_queue_worker(bot))

    async def on_shutdown(*_args, **_kwargs) -> None:
        global QUEUE_WORKER_TASK, QUEUE_SHUTTING_DOWN
        LOGGER.info("Shutting down...")
        QUEUE_SHUTTING_DOWN = True
        if QUEUE_WORKER_TASK is not None:
            QUEUE_WORKER_TASK.cancel()
            try:
                await QUEUE_WORKER_TASK
            except asyncio.CancelledError:
                pass
            QUEUE_WORKER_TASK = None
        await bot.session.close()

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    async def health(_: web.Request) -> web.Response:
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/", health)

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=webhook_secret,
    ).register(app, path=webhook_path)
    setup_application(app, dp, bot=bot)
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()

