import asyncio
from html import escape as html_escape
from pathlib import Path
from typing import Any, Awaitable, Callable

from telegram import Update
from telegram.error import BadRequest, RetryAfter
from telegram.ext import ContextTypes

from .common import (
    ensure_defaults,
    format_bytes,
    now_monotonic,
    progress_bar,
    remember_message,
    split_file_for_upload,
    validate_file_ready,
)
from .config import BotConfig
from .constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, KEY_PROGRESS, VIDEO_EXTENSIONS
from .keyboards import progress_cancel_keyboard


def _format_duration(seconds: float) -> str:
    sec = max(0, int(seconds))
    hours, rem = divmod(sec, 3600)
    minutes, sec_part = divmod(rem, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{sec_part:02d}"
    return f"{minutes:02d}:{sec_part:02d}"


def format_bot_text(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return "ℹ️ <b>Información</b>"
    if raw.startswith("<"):
        return raw

    lower = raw.lower()
    if any(token in lower for token in ("error", "fallo", "no se pudo", "denegado", "inval")):
        emoji = "❌"
    elif any(token in lower for token in ("complet", "finaliz", "enviado", "listo", "ok")):
        emoji = "✅"
    elif any(token in lower for token in ("cola", "estado", "proceso", "modo", "resumen", "aviso")):
        emoji = "📌"
    else:
        emoji = "ℹ️"

    lines = raw.splitlines()
    first = html_escape(lines[0])
    rest = "\n".join(html_escape(line) for line in lines[1:])
    if rest:
        return f"{emoji} <b>{first}</b>\n{rest}"
    return f"{emoji} {first}"


async def tracked_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
    is_html: bool = False,
) -> None:
    safe_text = text if is_html else format_bot_text(text)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        chat = update.effective_chat
        if not chat:
            return
        sent = await context.bot.send_message(chat_id=chat.id, text=safe_text, reply_markup=reply_markup)
    else:
        sent = await target_message.reply_text(safe_text, reply_markup=reply_markup)
    remember_message(update, context, sent.message_id)


async def retry_async(cfg: BotConfig, op_name: str, coro_factory: Callable[[], Awaitable[Any]]) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, cfg.max_retries + 1):
        try:
            return await coro_factory()
        except RetryAfter as retry_exc:
            wait_for = float(retry_exc.retry_after) + 0.2
            await asyncio.sleep(wait_for)
            last_exc = retry_exc
        except Exception as exc:
            if "operacion cancelada por el usuario" in str(exc).lower():
                raise
            if "file is too big" in str(exc).lower():
                raise
            last_exc = exc
            if attempt >= cfg.max_retries:
                break
            await asyncio.sleep(cfg.retry_base_delay * attempt)
    raise RuntimeError(f"{op_name} fallo tras {cfg.max_retries} intentos: {last_exc}")


async def upsert_progress(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    cfg: BotConfig,
    stage: str,
    current: int,
    total: int,
    details: str,
    force: bool = False,
    cancellable: bool = False,
) -> None:
    ensure_defaults(context)
    chat = update.effective_chat
    if not chat:
        return

    total_safe = max(total, 1)
    bounded_current = max(0, min(current, total_safe))
    percent = (bounded_current / total_safe) * 100
    now = now_monotonic()

    state: dict[str, Any] = context.user_data[KEY_PROGRESS]
    last_percent = state.get("last_percent", -1)
    last_ts = state.get("last_ts", 0.0)
    last_stage = state.get("stage", "")

    stage_started_at = float(state.get("stage_started_at", now))
    if stage != last_stage:
        stage_started_at = now
        state["stage_started_at"] = stage_started_at

    if not force:
        if stage == last_stage and (percent - float(last_percent)) < 1 and (now - float(last_ts)) < cfg.progress_min_update_seconds:
            return

    elapsed = max(0.0, now - stage_started_at)
    ratio = bounded_current / total_safe
    eta_seconds: float | None = None
    if ratio > 0 and ratio < 1 and elapsed > 0.1:
        eta_seconds = (elapsed / ratio) - elapsed

    eta_text = _format_duration(eta_seconds) if eta_seconds is not None else "calculando"
    
    stage_lower = stage.lower()
    if "descarg" in stage_lower:
        stage_title = "DESCARGANDO ARCHIVOS"
    elif "descompr" in stage_lower or "extrac" in stage_lower:
        stage_title = "EXTRAYENDO ARCHIVOS"
    elif "comprim" in stage_lower:
        stage_title = "COMPRIMIENDO ARCHIVOS"
    elif "subiend" in stage_lower:
        stage_title = "SUBIENDO ARCHIVOS"
    else:
        stage_title = stage.upper()

    bar = progress_bar(percent, cfg.progress_bar_size, filled_char="✦", empty_char="◌")
    line1 = f"✧ {stage_title} ✧"
    line2 = f"{bar} {percent:.0f}%"

    uses_byte_progress = total_safe >= 1024 and any(
        token in stage_lower for token in ("descarg", "comprim", "subiend", "encrypt")
    )
    elapsed_text = _format_duration(elapsed)

    if uses_byte_progress:
        speed_bps = int(bounded_current / elapsed) if elapsed > 0.1 else 0
        line3 = (
            f"✧ {format_bytes(bounded_current)} de {format_bytes(total_safe)}"
            + (f" • {format_bytes(speed_bps)}/s" if speed_bps > 0 else "")
        )
    else:
        line3 = f"✧ {bounded_current}/{total_safe} archivos"

    line4 = f"✧ {html_escape(details)}"
    line5 = f"✧ ⏱️ {elapsed_text} transcurrido • {eta_text} restantes"
    text = f"{line1}\n{line2}\n{line3}\n{line4}\n{line5}"
    reply_markup = progress_cancel_keyboard() if cancellable else None

    message_id = state.get("message_id")
    if not message_id:
        sent = await context.bot.send_message(chat_id=chat.id, text=text, reply_markup=reply_markup)
        remember_message(update, context, sent.message_id)
        state["message_id"] = sent.message_id
    else:
        try:
            await context.bot.edit_message_text(chat_id=chat.id, message_id=message_id, text=text, reply_markup=reply_markup)
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise

    state["last_percent"] = percent
    state["last_ts"] = now
    state["stage"] = stage


async def send_file_with_best_method(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    file_path: Path,
    reply_markup=None,
) -> int:
    ext = file_path.suffix.lower()
    sent_message = None

    for _ in range(3):
        try:
            with file_path.open("rb") as payload:
                if ext in IMAGE_EXTENSIONS:
                    sent_message = await context.bot.send_photo(chat_id=chat_id, photo=payload, reply_markup=reply_markup)
                elif ext in AUDIO_EXTENSIONS:
                    sent_message = await context.bot.send_audio(chat_id=chat_id, audio=payload, reply_markup=reply_markup)
                elif ext in VIDEO_EXTENSIONS:
                    sent_message = await context.bot.send_video(chat_id=chat_id, video=payload, reply_markup=reply_markup)
                else:
                    sent_message = await context.bot.send_document(
                        chat_id=chat_id,
                        document=payload,
                        filename=file_path.name,
                        reply_markup=reply_markup,
                    )
            break
        except RetryAfter as retry_exc:
            await asyncio.sleep(float(retry_exc.retry_after) + 0.2)

    if sent_message is None:
        with file_path.open("rb") as payload:
            sent_message = await context.bot.send_document(
                chat_id=chat_id,
                document=payload,
                filename=file_path.name,
                reply_markup=reply_markup,
            )

    return sent_message.message_id


async def send_path_with_size_control(
    context: ContextTypes.DEFAULT_TYPE,
    cfg: BotConfig,
    chat_id: int,
    file_path: Path,
    reply_markup=None,
) -> list[int]:
    validate_file_ready(file_path)
    size = file_path.stat().st_size
    if size <= cfg.telegram_max_upload_bytes:
        async def _single_upload():
            return await send_file_with_best_method(context, chat_id, file_path, reply_markup=reply_markup)

        message_id = await retry_async(cfg, f"Subida {file_path.name}", _single_upload)
        return [message_id]

    ids: list[int] = []
    parts = await asyncio.to_thread(split_file_for_upload, file_path, cfg)
    for index, part in enumerate(parts, start=1):
        validate_file_ready(part)
        caption = (
            f"Archivo grande dividido: {file_path.name} (parte {index}/{len(parts)})"
            if index == 1
            else None
        )

        async def _upload_part():
            with part.open("rb") as payload:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=payload,
                    filename=part.name,
                    caption=caption,
                    reply_markup=reply_markup,
                )
            return sent.message_id

        sent_id = await retry_async(cfg, f"Subida parte {part.name}", _upload_part)
        ids.append(sent_id)
    return ids
