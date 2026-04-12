import asyncio
import math
import re
import shutil
from html import escape as html_escape
from pathlib import Path
from typing import Any, Awaitable, Callable

from telegram import Update
from telegram.error import BadRequest, RetryAfter
from telegram.ext import ContextTypes

from .common import (
    effective_part_size,
    ensure_defaults,
    now_monotonic,
    progress_bar,
    remember_message,
    split_file_for_upload,
    validate_file_ready,
)
from .config import BotConfig
from .constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, KEY_PROGRESS, VIDEO_EXTENSIONS
from .keyboards import progress_cancel_keyboard

try:
    from mutagen import File as MutagenFile
except Exception:  # pragma: no cover - fallback when optional dependency is missing
    MutagenFile = None


def _format_duration(seconds: float) -> str:
    sec = max(0, int(seconds))
    hours, rem = divmod(sec, 3600)
    minutes, sec_part = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h, {minutes}m, {sec_part}s"
    if minutes > 0:
        return f"{minutes}m, {sec_part}s"
    return f"{sec_part}s"


def _progress_spinner(elapsed: float) -> str:
    frames = ["◐", "◓", "◑", "◒"]
    idx = int(max(0.0, elapsed) * 2) % len(frames)
    return frames[idx]


def _format_iec_bytes(size: int) -> str:
    value = float(max(0, size))
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.2f} {unit}"
        value /= 1024.0


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
    force_new_message: bool = False,
) -> None:
    safe_text = text if is_html else format_bot_text(text)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    chat = update.effective_chat
    if not chat and not target_message:
        return

    async def _send_direct() -> Any:
        if not chat:
            raise RuntimeError("Chat no disponible")
        return await context.bot.send_message(chat_id=chat.id, text=safe_text, reply_markup=reply_markup)

    async def _send_reply() -> Any:
        if not target_message:
            return await _send_direct()
        return await target_message.reply_text(safe_text, reply_markup=reply_markup)

    sent = None
    for attempt in range(3):
        try:
            if force_new_message or not target_message:
                sent = await _send_direct()
            else:
                sent = await _send_reply()
            break
        except RetryAfter as retry_exc:
            await asyncio.sleep(float(retry_exc.retry_after) + 0.2)
        except Exception:
            if attempt == 0:
                # Fallback to direct send on first failure.
                try:
                    sent = await _send_direct()
                    break
                except RetryAfter as retry_exc:
                    await asyncio.sleep(float(retry_exc.retry_after) + 0.2)
                except Exception:
                    pass
            if attempt >= 2:
                raise
    remember_message(update, context, sent.message_id)


async def _retry_async_inner(
    cfg: BotConfig,
    op_name: str,
    coro_factory: Callable[[], Awaitable[Any]],
    cancel_requested: Callable[[], bool] | None,
    sleeper: Callable[[float, Callable[[], bool] | None], Awaitable[None]],
    op_timeout_seconds: float | None = None,
) -> Any:
    async def _cancel_task_gracefully(task: asyncio.Task[Any], grace_seconds: float = 3.0) -> None:
        if task.done():
            return
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=max(0.1, float(grace_seconds)))
        except asyncio.TimeoutError:
            # Si la tarea ignora la cancelación (p.ej. I/O colgado), no nos quedamos esperando.
            return
        except Exception:
            return

    last_exc: Exception | None = None
    timeout_limit = float(op_timeout_seconds) if op_timeout_seconds is not None else float(cfg.telegram_operation_timeout)
    timeout_limit = max(5.0, timeout_limit)
    for attempt in range(1, cfg.max_retries + 1):
        if cancel_requested and cancel_requested():
            raise RuntimeError("Operacion cancelada por el usuario")
        try:
            task = asyncio.create_task(coro_factory())
            start_ts = now_monotonic()
            while True:
                if cancel_requested and cancel_requested():
                    await _cancel_task_gracefully(task)
                    raise RuntimeError("Operacion cancelada por el usuario")
                done, _ = await asyncio.wait({task}, timeout=0.5)
                if task in done:
                    return await task
                if (now_monotonic() - start_ts) >= timeout_limit:
                    await _cancel_task_gracefully(task)
                    raise asyncio.TimeoutError()
        except RetryAfter as retry_exc:
            wait_for = float(retry_exc.retry_after) + 0.2
            await sleeper(wait_for, cancel_requested)
            last_exc = retry_exc
        except Exception as exc:
            if "operacion cancelada por el usuario" in str(exc).lower():
                raise
            if "file is too big" in str(exc).lower():
                raise
            # No reintentar timeouts de upload: propagar al llamador para que
            # pueda decidir dividir en partes mas pequenas en vez de reintentar
            # subidas de 20+ min que saturan la VPS.
            if isinstance(exc, (asyncio.TimeoutError,)):
                raise
            exc_text = str(exc).lower()
            if any(t in exc_text for t in ("timed out", "timeout", "read timed out")):
                raise
            last_exc = exc
            if attempt >= cfg.max_retries:
                break
            await sleeper(cfg.retry_base_delay * attempt, cancel_requested)
    raise RuntimeError(f"{op_name} fallo tras {cfg.max_retries} intentos: {last_exc}")


async def retry_async_with_cancel(
    cfg: BotConfig,
    op_name: str,
    coro_factory: Callable[[], Awaitable[Any]],
    cancel_requested: Callable[[], bool] | None,
    timeout_seconds: float | None = None,
) -> Any:
    async def _sleep_with_cancel(delay: float, cancel_cb: Callable[[], bool] | None) -> None:
        if delay <= 0:
            return
        remaining = float(delay)
        while remaining > 0:
            if cancel_cb and cancel_cb():
                raise RuntimeError("Operacion cancelada por el usuario")
            step = min(0.5, remaining)
            await asyncio.sleep(step)
            remaining -= step

    return await _retry_async_inner(
        cfg,
        op_name,
        coro_factory,
        cancel_requested,
        _sleep_with_cancel,
        op_timeout_seconds=timeout_seconds,
    )


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
    pulse: bool = False,
    percent_override: float | None = None,
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
    last_details_rendered = str(state.get("last_details", ""))

    # Optional override: allow callers (e.g. multipart uploads) to display progress
    # based on parts completed, even if byte counters only move at part boundaries.
    # Keep it in state so the heartbeat loop preserves the same display.
    if percent_override is not None:
        try:
            state["percent_override"] = float(percent_override)
        except Exception:
            state.pop("percent_override", None)
    elif stage != last_stage:
        # Stage changed and the caller didn't provide a new override.
        state.pop("percent_override", None)

    effective_percent = percent
    if "percent_override" in state:
        try:
            effective_percent = float(state.get("percent_override", percent))
        except Exception:
            effective_percent = percent
    effective_percent = max(0.0, min(100.0, effective_percent))

    details_changed = str(details) != last_details_rendered

    state["current"] = bounded_current
    state["total"] = total_safe
    state["details"] = details
    state["cancellable"] = cancellable

    # Conserva el ultimo progreso real en bytes para que el estado final
    # (que a veces usa 1/1 o marcadores de cierre) siga mostrando MB recorridos.
    details_lower = " ".join(str(details or "").splitlines()).lower()
    current_looks_like_bytes = total_safe >= 1024 or bounded_current >= 1024 or " de " in details_lower or " of " in details_lower
    if current_looks_like_bytes:
        state["byte_current"] = int(bounded_current)
        state["byte_total"] = int(total_safe)

    previous_current = int(state.get("previous_current", bounded_current))
    if stage != last_stage or bounded_current != previous_current:
        state["last_value_change_ts"] = now
    state["previous_current"] = bounded_current

    stage_started_at = float(state.get("stage_started_at", now))
    if stage != last_stage:
        stage_started_at = now
        state["stage_started_at"] = stage_started_at
        state["speed_last_bytes"] = 0
        state["speed_last_ts"] = now
        state["last_value_change_ts"] = now
        state["last_nonzero_speed_bps"] = 0

    if not force:
        if (
            stage == last_stage
            and not details_changed
            and (effective_percent - float(last_percent)) < 1
            and (now - float(last_ts)) < cfg.progress_min_update_seconds
        ):
            return

    elapsed = max(0.0, now - stage_started_at)
    
    stage_lower = stage.lower()
    if "descarg" in stage_lower:
        stage_title = "Downloading... Please wait"
    elif "subiend" in stage_lower:
        stage_title = "Uploading... Please wait"
    elif "descompr" in stage_lower or "extrac" in stage_lower:
        stage_title = "Extracting... Please wait"
    elif "comprim" in stage_lower:
        stage_title = "Compressing... Please wait"
    elif "encrypt" in stage_lower:
        stage_title = "Encrypting... Please wait"
    elif "renombr" in stage_lower:
        stage_title = "Renaming... Please wait"
    elif "cancel" in stage_lower:
        stage_title = "Cancelling... Please wait"
    elif "error" in stage_lower:
        stage_title = "Processing failed"
    elif "complet" in stage_lower:
        stage_title = "Completed"
    else:
        stage_title = "Processing... Please wait"

    display_percent = effective_percent
    if pulse and bounded_current == 0 and percent < 100.0:
        display_percent = min(95.0, max(1.0, 2.0 + (1.0 - math.exp(-elapsed / 12.0)) * 93.0))

    terminal_stage = any(token in stage_lower for token in ("complet", "error", "cancel"))
    if not terminal_stage and display_percent >= 100.0:
        display_percent = 99.0

    bar = progress_bar(display_percent, max(10, cfg.progress_bar_size), filled_char="⬢", empty_char="⬡")
    spinner = _progress_spinner(elapsed)
    line1 = f"<b>{html_escape(stage_title)} {spinner}</b>"
    line2 = f"<code>[{bar}]</code>"
    line3 = f"<b>Processing:</b> <code>{display_percent:.2f}%</code>"

    details_clean = " ".join(str(details or "").splitlines()).strip()
    if len(details_clean) > 180:
        details_clean = details_clean[:177] + "..."
    details_html = html_escape(details_clean)
    line_details = (
        f"<b>Details:</b> <code>{details_html}</code>"
        if details_html
        else "<b>Details:</b> <code>--</code>"
    )

    # Evitar datos duplicados: muchos flujos ya colocan "X de Y" en Details,
    # y ademas mostramos una linea "X of Y". Si detectamos que Details ya
    # contiene progreso en bytes, omitimos la linea de datos.
    details_lower = details_clean.lower()
    byte_tokens = re.findall(r"\b\d+(?:\.\d+)?\s*(?:b|kb|mb|gb|tb|kib|mib|gib|tib)\b", details_lower)
    details_has_byte_progress = (len(byte_tokens) >= 2) or (
        len(byte_tokens) >= 1 and (" de " in details_lower or " of " in details_lower)
    )

    byte_current = int(state.get("byte_current", bounded_current))
    byte_total = int(state.get("byte_total", total_safe))
    has_byte_memory = byte_total >= 1024 and byte_current >= 0
    uses_byte_progress = has_byte_memory or (
        total_safe >= 1024 and any(
            token in stage_lower for token in ("descarg", "comprim", "subiend", "encrypt", "descompr", "extrac", "renombr")
        )
    )
    eta_text = "--"
    if uses_byte_progress:
        speed_bps = 0
        prev_bytes = int(state.get("speed_last_bytes", 0))
        prev_ts = float(state.get("speed_last_ts", stage_started_at))
        last_nonzero_speed = int(state.get("last_nonzero_speed_bps", 0))

        if byte_current < prev_bytes or now <= prev_ts:
            prev_bytes = 0
            prev_ts = stage_started_at

        delta_bytes = byte_current - prev_bytes
        delta_t = now - prev_ts
        if delta_bytes > 0 and delta_t > 0.1:
            speed_bps = int(delta_bytes / delta_t)

        # Fallback 1: velocidad promedio de la etapa cuando no hay delta reciente.
        if speed_bps <= 0 and byte_current > 0 and elapsed > 0.5:
            speed_bps = int(byte_current / elapsed)

        # Fallback 2: ultima velocidad no nula observada para evitar mostrar "calculating".
        if speed_bps <= 0 and last_nonzero_speed > 0:
            speed_bps = last_nonzero_speed

        if speed_bps > 0:
            state["last_nonzero_speed_bps"] = speed_bps

        state["speed_last_bytes"] = byte_current
        state["speed_last_ts"] = now

        if byte_current >= byte_total:
            eta_text = "0s"
        elif speed_bps > 0:
            eta_seconds = (byte_total - byte_current) / speed_bps
            eta_text = _format_duration(eta_seconds)

        line4 = f"<b>Data:</b> <code>{_format_iec_bytes(byte_current)} of {_format_iec_bytes(byte_total)}</code>"
        line5 = f"<b>Speed:</b> <code>{_format_iec_bytes(max(0, speed_bps))}/s</code>"
    else:
        line4 = "<b>Data:</b> <code>calculating...</code>"
        line5 = "<b>Speed:</b> <code>0 B/s</code>"

    line6 = f"<b>ETA:</b> <code>{html_escape(eta_text)}</code>"

    if uses_byte_progress and details_has_byte_progress:
        text = f"{line1}\n\n{line2}\n{line3}\n{line_details}\n{line5}\n{line6}"
    else:
        text = f"{line1}\n\n{line2}\n{line3}\n{line_details}\n{line4}\n{line5}\n{line6}"
    reply_markup = progress_cancel_keyboard() if cancellable else None

    async def _retry_progress_op(op: Callable[[], Awaitable[Any]]) -> Any:
        for attempt in range(3):
            try:
                return await op()
            except RetryAfter as retry_exc:
                await asyncio.sleep(float(retry_exc.retry_after) + 0.2)
            except Exception:
                if attempt >= 2:
                    raise

    message_id = state.get("message_id")
    progress_ids: list[int] = state.setdefault("message_ids", [])
    global_progress_ids: list[int] = context.user_data.setdefault("__progress_message_ids", [])
    if not message_id:
        sent = await _retry_progress_op(
            lambda: context.bot.send_message(
                chat_id=chat.id,
                text=text,
                reply_markup=reply_markup,
                parse_mode="HTML",
            )
        )
        if sent:
            remember_message(update, context, sent.message_id)
            state["message_id"] = sent.message_id
            if sent.message_id not in progress_ids:
                progress_ids.append(sent.message_id)
            if sent.message_id not in global_progress_ids:
                global_progress_ids.append(sent.message_id)
    else:
        try:
            await _retry_progress_op(
                lambda: context.bot.edit_message_text(
                    chat_id=chat.id,
                    message_id=message_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode="HTML",
                )
            )
        except BadRequest as exc:
            err = str(exc).lower()
            if "message is not modified" in err:
                pass
            elif "message to edit not found" in err or "message can't be edited" in err:
                # Recreate immediately to keep the progress message visible.
                try:
                    sent = await _retry_progress_op(
                        lambda: context.bot.send_message(
                            chat_id=chat.id,
                            text=text,
                            reply_markup=reply_markup,
                            parse_mode="HTML",
                        )
                    )
                    if sent:
                        remember_message(update, context, sent.message_id)
                        state["message_id"] = sent.message_id
                        if sent.message_id not in progress_ids:
                            progress_ids.append(sent.message_id)
                        if sent.message_id not in global_progress_ids:
                            global_progress_ids.append(sent.message_id)
                except Exception:
                    pass
            else:
                raise

    state["last_percent"] = effective_percent
    state["last_ts"] = now
    state["stage"] = stage
    state["last_details"] = str(details)


async def send_file_with_best_method(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    file_path: Path,
    reply_markup=None,
    caption: str | None = None,
    timeout_seconds: float | None = None,
) -> int:
    ext = file_path.suffix.lower()
    sent_message = None

    for _ in range(3):
        try:
            with file_path.open("rb") as payload:
                if ext in IMAGE_EXTENSIONS:
                    sent_message = await asyncio.wait_for(
                        context.bot.send_photo(
                            chat_id=chat_id,
                            photo=payload,
                            caption=caption,
                            parse_mode="HTML" if caption else None,
                            reply_markup=reply_markup,
                        ),
                        timeout=timeout_seconds,
                    )
                elif ext in AUDIO_EXTENSIONS:
                    duration_seconds: int | None = None
                    if MutagenFile is not None:
                        try:
                            meta = MutagenFile(str(file_path))
                            raw_len = getattr(getattr(meta, "info", None), "length", None)
                            if raw_len and raw_len > 0:
                                duration_seconds = max(1, int(round(float(raw_len))))
                        except Exception:
                            duration_seconds = None
                    sent_message = await asyncio.wait_for(
                        context.bot.send_audio(
                            chat_id=chat_id,
                            audio=payload,
                            duration=duration_seconds,
                            caption=caption,
                            parse_mode="HTML" if caption else None,
                            reply_markup=reply_markup,
                        ),
                        timeout=timeout_seconds,
                    )
                elif ext in VIDEO_EXTENSIONS:
                    sent_message = await asyncio.wait_for(
                        context.bot.send_video(
                            chat_id=chat_id,
                            video=payload,
                            caption=caption,
                            parse_mode="HTML" if caption else None,
                            reply_markup=reply_markup,
                        ),
                        timeout=timeout_seconds,
                    )
                else:
                    sent_message = await asyncio.wait_for(
                        context.bot.send_document(
                            chat_id=chat_id,
                            document=payload,
                            filename=file_path.name,
                            caption=caption,
                            parse_mode="HTML" if caption else None,
                            reply_markup=reply_markup,
                        ),
                        timeout=timeout_seconds,
                    )
            break
        except RetryAfter as retry_exc:
            await asyncio.sleep(float(retry_exc.retry_after) + 0.2)

    if sent_message is None:
        with file_path.open("rb") as payload:
            sent_message = await asyncio.wait_for(
                context.bot.send_document(
                    chat_id=chat_id,
                    document=payload,
                    filename=file_path.name,
                    caption=caption,
                    parse_mode="HTML" if caption else None,
                    reply_markup=reply_markup,
                ),
                timeout=timeout_seconds,
            )

    return sent_message.message_id


async def send_path_with_size_control(
    context: ContextTypes.DEFAULT_TYPE,
    cfg: BotConfig,
    chat_id: int,
    file_path: Path,
    reply_markup=None,
    caption_footer: str | None = None,
    cancel_requested: Callable[[], bool] | None = None,
    part_size_override: int | None = None,
    timeout_seconds: float | None = None,
    on_part_sent: Callable[[int, int, int], Awaitable[None]] | None = None,
) -> list[int]:
    validate_file_ready(file_path)
    size = file_path.stat().st_size
    part_size = max(1, int(part_size_override)) if part_size_override else effective_part_size(cfg)
    base_upload_timeout = timeout_seconds if timeout_seconds is not None else cfg.telegram_operation_timeout

    def _recommended_timeout(size_bytes: int) -> float:
        size_bytes = max(1, int(size_bytes))
        base = max(60.0, float(base_upload_timeout))
        estimated = (size_bytes / (5 * 1024 * 1024)) + 600.0
        return max(base, min(7200.0, estimated))

    def _check_disk_space(required_bytes: int) -> None:
        try:
            disk_stat = shutil.disk_usage(file_path.parent)
            # Necesitamos espacio para las partes + margen de 256MB
            if disk_stat.free < (required_bytes + 256 * 1024 * 1024):
                raise RuntimeError(
                    f"Espacio insuficiente en disco para dividir: "
                    f"necesario {required_bytes // (1024*1024)}MB, "
                    f"disponible {disk_stat.free // (1024*1024)}MB"
                )
        except RuntimeError:
            raise
        except Exception:
            pass  # Si no podemos verificar, continuamos

    upload_timeout = _recommended_timeout(size)

    def _should_split_smaller(exc: Exception) -> bool:
        if isinstance(exc, (asyncio.TimeoutError,)):
            return True
        text = str(exc).lower()
        if any(token in text for token in ("file is too big", "entity too large", "request entity too large", "file too large")):
            return True
        if any(token in text for token in ("timed out", "timeout", "read timed out", "network")):
            return True
        return False

    def _cleanup_parts_dir() -> None:
        parts_dir = file_path.parent / f"{file_path.stem}_parts"
        if parts_dir.exists():
            shutil.rmtree(parts_dir, ignore_errors=True)

    if size <= part_size:
        async def _single_upload():
            return await send_file_with_best_method(
                context,
                chat_id,
                file_path,
                reply_markup=reply_markup,
                caption=caption_footer,
                timeout_seconds=upload_timeout,
            )

        try:
            message_id = await retry_async_with_cancel(
                cfg,
                f"Subida {file_path.name}",
                _single_upload,
                cancel_requested,
                timeout_seconds=upload_timeout,
            )
            if on_part_sent:
                await on_part_sent(1, 1, int(message_id))
            return [message_id]
        except Exception as exc:
            if not _should_split_smaller(exc):
                raise
            # Fallback: dividir en partes mas pequenas si la subida directa se cuelga.
            _cleanup_parts_dir()

    # Filtrar solo tamaños que realmente reducen vs el actual
    all_fallback_sizes = [
        part_size,
        1024 * 1024 * 1024,
        768 * 1024 * 1024,
        512 * 1024 * 1024,
    ]
    fallback_sizes = [s for s in all_fallback_sizes if s < size] or [all_fallback_sizes[-1]]

    for size_index, candidate_size in enumerate(fallback_sizes):
        ids: list[int] = []
        if cancel_requested and cancel_requested():
            raise RuntimeError("Operacion cancelada por el usuario")

        _cleanup_parts_dir()
        _check_disk_space(size)
        parts = await asyncio.to_thread(
            split_file_for_upload,
            file_path,
            cfg,
            cancel_requested,
            candidate_size,
        )

        try:
            for index, part in enumerate(parts, start=1):
                if cancel_requested and cancel_requested():
                    raise RuntimeError("Operacion cancelada por el usuario")
                validate_file_ready(part)
                part_timeout = _recommended_timeout(part.stat().st_size)
                caption = (
                    f"Archivo grande dividido: {file_path.name} (parte {index}/{len(parts)})"
                    if index == 1
                    else None
                )
                if caption_footer:
                    caption = f"{caption}\n{caption_footer}" if caption else caption_footer

                async def _upload_part():
                    with part.open("rb") as payload:
                        sent = await asyncio.wait_for(
                            context.bot.send_document(
                                chat_id=chat_id,
                                document=payload,
                                filename=part.name,
                                caption=caption,
                                parse_mode="HTML" if caption else None,
                                reply_markup=reply_markup,
                            ),
                            timeout=part_timeout,
                        )
                    return sent.message_id

                sent_id = await retry_async_with_cancel(
                    cfg,
                    f"Subida parte {part.name}",
                    _upload_part,
                    cancel_requested,
                    timeout_seconds=part_timeout,
                )
                ids.append(sent_id)
                if on_part_sent:
                    await on_part_sent(int(index), int(len(parts)), int(sent_id))
                if index < len(parts):
                    await asyncio.sleep(0.4)

            return ids
        except Exception as exc:
            if not ids and size_index < len(fallback_sizes) - 1 and _should_split_smaller(exc):
                continue
            raise

    return []
