import asyncio
import hashlib
import math
import re
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from telegram import Update
from telegram.ext import ContextTypes

from .config import BotConfig
from .constants import (
    ARCHIVE_EXTENSIONS,
    KEY_ARCHIVES_TO_EXTRACT,
    KEY_CURRENT_ARCHIVE,
    KEY_EXTRACTED_FILES,
    KEY_FILES_TO_COMPRESS,
    KEY_KNOWN_PASSWORD,
    KEY_COMPRESS_QUEUE_MESSAGE_ID,
    KEY_EXTRACT_QUEUE_MESSAGE_ID,
    KEY_LAST_COMPRESS_QUEUE_TEXT,
    KEY_LAST_EXTRACT_QUEUE_TEXT,
    KEY_PASSWORD_PROMPT_MESSAGE_ID,
    KEY_START_MESSAGE_ID,
    KEY_ACTION_PROMPT_MESSAGE_ID,
    KEY_EXTRACT_TOTAL_RECEIVED,
    KEY_EXTRACT_ARCHIVES_DONE,
    KEY_RESULT_ACTIONS,
    KEY_PENDING_ACTION,
    KEY_PENDING_FILE_ID,
    KEY_PENDING_FILE_NAME,
    KEY_PENDING_SOURCE_MESSAGE_ID,
    KEY_PENDING_LOCAL_TARGETS,
    KEY_CANCEL_REQUESTED,
    KEY_MESSAGE_IDS,
    KEY_MODE,
    KEY_PROGRESS,
    MODE_IDLE,
)


def ensure_defaults(context: ContextTypes.DEFAULT_TYPE) -> None:
    user_data = context.user_data
    user_data.setdefault(KEY_MODE, MODE_IDLE)
    user_data.setdefault(KEY_FILES_TO_COMPRESS, [])
    user_data.setdefault(KEY_ARCHIVES_TO_EXTRACT, [])
    user_data.setdefault(KEY_EXTRACTED_FILES, [])
    user_data.setdefault(KEY_MESSAGE_IDS, [])
    user_data.setdefault(KEY_PROGRESS, {})
    user_data.setdefault(KEY_COMPRESS_QUEUE_MESSAGE_ID, None)
    user_data.setdefault(KEY_EXTRACT_QUEUE_MESSAGE_ID, None)
    user_data.setdefault(KEY_LAST_COMPRESS_QUEUE_TEXT, "")
    user_data.setdefault(KEY_LAST_EXTRACT_QUEUE_TEXT, "")
    user_data.setdefault(KEY_PASSWORD_PROMPT_MESSAGE_ID, None)
    user_data.setdefault(KEY_START_MESSAGE_ID, None)
    user_data.setdefault(KEY_ACTION_PROMPT_MESSAGE_ID, None)
    user_data.setdefault(KEY_EXTRACT_TOTAL_RECEIVED, 0)
    user_data.setdefault(KEY_EXTRACT_ARCHIVES_DONE, 0)
    user_data.setdefault(KEY_RESULT_ACTIONS, {})
    user_data.setdefault(KEY_PENDING_ACTION, None)
    user_data.setdefault(KEY_PENDING_FILE_ID, None)
    user_data.setdefault(KEY_PENDING_FILE_NAME, None)
    user_data.setdefault(KEY_PENDING_SOURCE_MESSAGE_ID, None)
    user_data.setdefault(KEY_PENDING_LOCAL_TARGETS, [])
    user_data.setdefault(KEY_CANCEL_REQUESTED, False)


def remember_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None) -> None:
    ensure_defaults(context)
    if message_id is None:
        return
    ids: list[int] = context.user_data[KEY_MESSAGE_IDS]
    if message_id not in ids:
        ids.append(message_id)
    if len(ids) > 800:
        del ids[:-800]


def reset_user_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data[KEY_MODE] = MODE_IDLE
    context.user_data[KEY_FILES_TO_COMPRESS] = []
    context.user_data[KEY_ARCHIVES_TO_EXTRACT] = []
    context.user_data[KEY_EXTRACTED_FILES] = []
    context.user_data[KEY_CURRENT_ARCHIVE] = None
    context.user_data[KEY_KNOWN_PASSWORD] = None
    context.user_data[KEY_PROGRESS] = {}
    context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = None
    context.user_data[KEY_EXTRACT_QUEUE_MESSAGE_ID] = None
    context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = ""
    context.user_data[KEY_LAST_EXTRACT_QUEUE_TEXT] = ""
    context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = None
    context.user_data[KEY_START_MESSAGE_ID] = None
    context.user_data[KEY_ACTION_PROMPT_MESSAGE_ID] = None
    context.user_data[KEY_EXTRACT_TOTAL_RECEIVED] = 0
    context.user_data[KEY_EXTRACT_ARCHIVES_DONE] = 0
    context.user_data[KEY_RESULT_ACTIONS] = {}
    context.user_data[KEY_PENDING_ACTION] = None
    context.user_data[KEY_PENDING_FILE_ID] = None
    context.user_data[KEY_PENDING_FILE_NAME] = None
    context.user_data[KEY_PENDING_SOURCE_MESSAGE_ID] = None
    context.user_data[KEY_PENDING_LOCAL_TARGETS] = []
    context.user_data[KEY_CANCEL_REQUESTED] = False


def format_bytes(size: int) -> str:
    if size < 0:
        size = 0
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"


def format_media_duration(seconds: int | float | None) -> str:
    if seconds is None:
        return ""
    total = max(0, int(seconds))
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def progress_bar(percent: float, width: int, filled_char: str = "█", empty_char: str = "░") -> str:
    bounded = max(0.0, min(100.0, percent))
    filled = int((bounded / 100.0) * width)
    if filled <= 0:
        return empty_char * width
    if filled >= width:
        return filled_char * width
    return filled_char * filled + empty_char * max(0, width - filled)


def base_workspace_root() -> Path:
    return Path(tempfile.gettempdir()) / "telegram_compress_bot"


def user_workspace(user_id: int) -> Path:
    root = base_workspace_root()
    workspace = root / str(user_id)
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def cleanup_root_if_empty() -> None:
    root = base_workspace_root()
    if not root.exists():
        return
    try:
        has_children = any(root.iterdir())
    except Exception:
        return
    if not has_children:
        shutil.rmtree(root, ignore_errors=True)


def cleanup_user_workspace(user_id: int, recreate: bool = False, preserve_postprocess: bool = False) -> None:
    workspace = base_workspace_root() / str(user_id)
    if workspace.exists():
        if preserve_postprocess:
            # Guardar postprocess antes de limpiar
            post_dir = workspace / "postprocess"
            post_backup = None
            if post_dir.exists():
                post_backup = Path(tempfile.gettempdir()) / f"post_backup_{uuid.uuid4().hex}"
                shutil.copytree(post_dir, post_backup)
            
            # Limpiar workspace
            shutil.rmtree(workspace, ignore_errors=True)
            
            # Restaurar postprocess
            if post_backup and post_backup.exists():
                post_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(post_backup), str(post_dir))
        else:
            shutil.rmtree(workspace, ignore_errors=True)
    if recreate:
        workspace.mkdir(parents=True, exist_ok=True)
    cleanup_root_if_empty()


def safe_name(name: str) -> str:
    cleaned = "".join(char for char in name if char not in "\\/:*?\"<>|").strip()
    return cleaned or f"file_{uuid.uuid4().hex}"


def unique_target_path(directory: Path, original_name: str) -> Path:
    safe = safe_name(original_name)
    candidate = directory / safe
    if not candidate.exists():
        return candidate

    stem = Path(safe).stem
    suffix = Path(safe).suffix
    index = 1
    while True:
        candidate = directory / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def is_archive_file(file_name: str) -> bool:
    lower_name = file_name.lower()
    if any(lower_name.endswith(ext) for ext in ARCHIVE_EXTENSIONS):
        return True
    if re.search(r"\.z\d{2}$", lower_name):
        return True
    if re.search(r"\.r\d{2}$", lower_name):
        return True
    if re.search(r"\.part\d+\.rar$", lower_name):
        return True
    if re.search(r"\.7z\.\d{3}$", lower_name):
        return True
    if re.search(r"\.(zip|rar)\.\d{3}$", lower_name):
        return True
    # Bot's own split format: file.zip.part001, file.rar.part002, etc.
    if re.search(r"\.(zip|rar|7z|tar|gz|tgz|bz2|xz)\.part\d{3}$", lower_name):
        return True
    # Plain numeric split: file.001, file.002 (7-Zip / HJSplit / generic splits)
    if re.search(r"\.\d{3}$", lower_name):
        return True
    return False


def is_bot_split_part(file_name: str) -> bool:
    """Return True if *file_name* matches the bot's own .partNNN split format."""
    return bool(re.search(r"\.part\d{3}$", file_name.lower()))


def bot_split_base_name(file_name: str) -> str:
    """Strip the trailing .partNNN suffix to recover the original archive name."""
    return re.sub(r"\.part\d{3}$", "", file_name, flags=re.IGNORECASE)


def detect_message_file(message) -> dict[str, Any] | None:
    if message.document:
        file_name = message.document.file_name or f"document_{message.document.file_unique_id}"
        return {
            "file_id": message.document.file_id,
            "file_name": safe_name(file_name),
            "file_size": int(message.document.file_size or 0),
        }

    if message.audio:
        file_name = message.audio.file_name or f"audio_{message.audio.file_unique_id}.mp3"
        return {
            "file_id": message.audio.file_id,
            "file_name": safe_name(file_name),
            "file_size": int(message.audio.file_size or 0),
            "duration_seconds": int(message.audio.duration or 0),
        }

    if message.video:
        file_name = message.video.file_name or f"video_{message.video.file_unique_id}.mp4"
        return {
            "file_id": message.video.file_id,
            "file_name": safe_name(file_name),
            "file_size": int(message.video.file_size or 0),
            "duration_seconds": int(message.video.duration or 0),
        }

    if message.voice:
        return {
            "file_id": message.voice.file_id,
            "file_name": f"voice_{message.voice.file_unique_id}.ogg",
            "file_size": int(message.voice.file_size or 0),
            "duration_seconds": int(message.voice.duration or 0),
        }

    if message.photo:
        largest = message.photo[-1]
        return {
            "file_id": largest.file_id,
            "file_name": f"photo_{largest.file_unique_id}.jpg",
            "file_size": int(largest.file_size or 0),
        }

    if message.video_note:
        return {
            "file_id": message.video_note.file_id,
            "file_name": f"video_note_{message.video_note.file_unique_id}.mp4",
            "file_size": int(message.video_note.file_size or 0),
        }

    if message.animation:
        file_name = message.animation.file_name or f"animation_{message.animation.file_unique_id}.mp4"
        return {
            "file_id": message.animation.file_id,
            "file_name": safe_name(file_name),
            "file_size": int(message.animation.file_size or 0),
        }

    return None


def validate_file_ready(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"No existe: {file_path.name}")
    if not file_path.is_file():
        raise ValueError(f"No es archivo regular: {file_path.name}")
    if file_path.stat().st_size <= 0:
        raise ValueError(f"Archivo vacio: {file_path.name}")


def compute_sha256(file_path: Path, chunk_size: int = 2 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as fobj:
        while True:
            chunk = fobj.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


async def compute_sha256_async(file_path: Path, chunk_size: int = 2 * 1024 * 1024) -> str:
    return await asyncio.to_thread(compute_sha256, file_path, chunk_size)


def effective_part_size(cfg: BotConfig) -> int:
    max_safe = max(1, cfg.telegram_max_upload_bytes - (1024 * 1024))
    return max(1, min(cfg.upload_part_bytes, max_safe))


def expected_parts_for_size(size_bytes: int, cfg: BotConfig) -> int:
    if size_bytes <= effective_part_size(cfg):
        return 1
    part_size = effective_part_size(cfg)
    return max(1, math.ceil(size_bytes / part_size))


def split_file_for_upload(
    file_path: Path,
    cfg: BotConfig,
    cancel_requested: Callable[[], bool] | None = None,
    part_size_override: int | None = None,
) -> list[Path]:
    size = file_path.stat().st_size
    part_size = max(1, int(part_size_override)) if part_size_override else effective_part_size(cfg)
    if size <= part_size:
        return [file_path]
    parts_dir = file_path.parent / f"{file_path.stem}_parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    parts: list[Path] = []

    checksum_enabled_for_size = (
        cfg.enable_checksum
        and (cfg.checksum_max_bytes <= 0 or size <= cfg.checksum_max_bytes)
    )
    original_hash = compute_sha256(file_path) if checksum_enabled_for_size else ""

    with file_path.open("rb") as source:
        index = 1
        buffer_size = 8 * 1024 * 1024
        while True:
            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")
            part_path = parts_dir / f"{file_path.name}.part{index:03d}"
            bytes_written = 0
            with part_path.open("wb") as target:
                while bytes_written < part_size:
                    if cancel_requested and cancel_requested():
                        raise RuntimeError("Operacion cancelada por el usuario")
                    remaining = part_size - bytes_written
                    read_size = buffer_size if remaining > buffer_size else remaining
                    chunk = source.read(read_size)
                    if not chunk:
                        break
                    target.write(chunk)
                    bytes_written += len(chunk)
            if bytes_written <= 0:
                try:
                    part_path.unlink()
                except Exception:
                    pass
                break
            parts.append(part_path)
            index += 1

    if checksum_enabled_for_size:
        rebuilt_hash = hashlib.sha256()
        for part in parts:
            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")
            with part.open("rb") as pf:
                while True:
                    if cancel_requested and cancel_requested():
                        raise RuntimeError("Operacion cancelada por el usuario")
                    block = pf.read(2 * 1024 * 1024)
                    if not block:
                        break
                    rebuilt_hash.update(block)
        if rebuilt_hash.hexdigest() != original_hash:
            raise RuntimeError("Checksum invalido al dividir archivo grande")

    return parts


def collect_extracted_files(directory: Path) -> list[Path]:
    files: list[Path] = []
    for file_path in directory.rglob("*"):
        if file_path.is_file():
            files.append(file_path)
    return files


def now_monotonic() -> float:
    return time.monotonic()
