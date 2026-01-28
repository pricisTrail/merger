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
DOWNLOAD_CONCURRENCY = int(os.getenv("DOWNLOAD_CONCURRENCY", "4"))
MERGE_CONCURRENCY = int(os.getenv("MERGE_CONCURRENCY", "2"))
KEEP_FILES = os.getenv("KEEP_FILES", "0") == "1"
PORT = int(os.getenv("PORT", "8000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")


class MergeStates(StatesGroup):
    waiting_audio = State()
    waiting_video = State()
    waiting_name = State()


router = Router()
ACTIVE_TASKS: Dict[int, List[asyncio.Task]] = defaultdict(list)
ACTIVE_TRACKERS: Dict[int, List[ProgressTracker]] = defaultdict(list)
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
    ACTIVE_TRACKERS[chat_id].append(tracker)
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

    video_path = job_dir / safe_filename(video_name)
    audio_path = job_dir / safe_filename(audio_name)
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
        await upload_with_progress(bot, chat_id, output_path, output_name, upload_cb, destination_id=dest)
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
    await state.set_state(MergeStates.waiting_audio)
    await message.answer("🎵 **Step 1/3**: Please send the **Audio** file.")


async def start_audio_flow(message: types.Message, state: FSMContext, file_id: str, name: str) -> None:
    await state.update_data(audio_id=file_id, audio_name=name)
    await state.set_state(MergeStates.waiting_video)
    await message.answer("📹 **Step 2/3**: Audio received. Now send the **Video** file.")


async def start_video_flow(message: types.Message, state: FSMContext, file_id: str, name: str) -> None:
    await state.update_data(video_id=file_id, video_name=name)
    await state.set_state(MergeStates.waiting_name)
    await message.answer("✏️ **Step 3/3**: Video received. Now send the **Output Name** for your file (e.g. MyVideo).")


@router.message(CommandStart())
async def cmd_start(message: types.Message) -> None:
    await message.answer(
        "🚀 <b>Telegram Multi-Merger Bot</b>\n\n"
        "I can merge video and audio without re-encoding!\n\n"
        "🔹 <b>/single</b> — Merge one pair step-by-step.\n"
        "🔹 <b>/links</b> — Batch merge using links.txt.\n\n"
        "⚙️ <b>Settings</b>\n"
        "🔹 <b>/setchannel</b> <code>ID</code> — Set destination channel.\n"
        "🔹 <b>/channel</b> — Show current destination.\n"
        "🔹 <b>/removechannel</b> — Reset to Private Chat.\n\n"
        "📊 <b>Other</b>\n"
        "🔹 <b>/status</b> — Check active jobs.\n"
        "🔹 <b>/stop</b> — Stop all active jobs."
    )


@router.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    text = (
        "📖 <b>Telegram Merger Bot — Complete Guide</b>\n\n"
        "I am a powerful media processing bot that can merge Video and Audio files without re-encoding, preserving 100% of the original quality.\n\n"
        
        "🛠 <b>Core Functions</b>\n"
        "<blockquote>"
        "🔹 <b>/single</b> — Start a step-by-step merge. I'll ask for audio, then video, then a name.\n"
        "🔹 <b>/links</b> — Show instructions for Batch Mode processing.\n"
        "</blockquote>\n"

        "⚙️ <b>Channel Destinations</b>\n"
        "<i>You can send merged files to any channel where I am an Admin.</i>\n"
        "<blockquote>"
        "🔹 <b>/setchannel</b> <code>[ID/@User]</code> — Set target channel.\n"
        "🔹 <b>/channel</b> — Verify current destination.\n"
        "🔹 <b>/removechannel</b> — Reset to Private Chat."
        "</blockquote>\n"

        "📊 <b>Management</b>\n"
        "<blockquote>"
        "🔹 <b>/status</b> — Detailed real-time view of all active jobs.\n"
        "🔹 <b>/stop</b> — Kill all background tasks and clean up.\n"
        "🔹 <b>/cancel</b> — Reset the current /single flow."
        "</blockquote>\n\n"

        "💡 <b>Pro Tips:</b>\n"
        "• Paste the <code>links.txt</code> content directly in chat for instant batching.\n"
        "• Files up to <b>2GB</b> are supported via Pyrogram.\n"
        "• Use <code>/status</code> if you lose a progress message."
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
    
    if not tasks:
        await message.answer("✅ <b>No active jobs</b> for this chat.")
        return
    
    try:
        import psutil
        usage = f"🖥 <b>System Load</b>: {psutil.cpu_percent()}% | <b>RAM</b>: {psutil.virtual_memory().percent}%"
    except ImportError:
        usage = "🖥 System tracking not available"

    text = (
        f"📊 <b>Detailed Status for {message.from_user.first_name}</b>\n"
        f"🏃 <b>Active Processes</b>: {len(tasks)}\n"
        f"{usage}\n\n"
    )
    
    for tracker in trackers:
        text += tracker.render() + "\n"

    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("stop"))
async def cmd_stop(message: types.Message) -> None:
    # ... (existing stop logic) ...
    chat_id = message.chat.id
    tasks = ACTIVE_TASKS.get(chat_id, [])
    if not tasks:
        await message.answer("No active jobs found for this chat.")
        return
    
    count = len(tasks)
    for task in tasks:
        task.cancel()
    
    await message.answer(f"Cancelled {count} active job(s). Cleanup might take a few seconds.")


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
        await message.answer("⚠️ Please send /single to start the merge flow correctly.")
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
    if is_video_document(document):
        if current_state == MergeStates.waiting_video:
            await start_video_flow(message, state, document.file_id, document.file_name or "video.mp4")
            return
        await message.answer("Please send an audio file first. MAKE SURE THE FIRST DOCUMENT WILL BE AUDIO THEN VIDEO")
        return
    if is_audio_document(document):
        await start_audio_flow(message, state, document.file_id, document.file_name or "audio")
        return
    if document.file_name and document.file_name.lower().endswith(".txt"):
        await handle_links_file(message, document)
        return
    if document.mime_type == "text/plain":
        await handle_links_file(message, document)
        return
    await message.answer("Unsupported document type. Send video/audio or links.txt. , MAKE SURE THE FIRST DOCUMENT WILL BE AUDIO THEN VIDEO")


@router.message(F.text)
async def handle_text(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    
    # Handle the "Name" step of /single flow
    if current_state == MergeStates.waiting_name:
        data = await state.get_data()
        await state.clear()
        
        output_name = message.text.strip()
        await message.answer(f"✅ Starting merge for **{output_name}**. Progress shown below.")
        
        task = asyncio.create_task(
            process_single_merge_with_name(
                message.bot, 
                message.chat.id, 
                data["video_id"], 
                data["audio_id"], 
                data["video_name"], 
                data["audio_name"],
                output_name
            )
        )
        track_task(message.chat.id, task)
        return

    # Handle Batch paste
    text = message.text or ""
    jobs = parse_links_text(text)
    if jobs:
        await message.answer("🚀 Starting batch merge. Progress shown below.")
        task = asyncio.create_task(process_link_jobs(message.bot, message, jobs))
        track_task(message.chat.id, task)
        return
    
    await message.answer("❓ Send /single to start a merge or /links for batch mode.")


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
        asyncio.create_task(_set_webhook_background())

    async def on_shutdown(*_args, **_kwargs) -> None:
        LOGGER.info("Shutting down...")
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
