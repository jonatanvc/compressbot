import asyncio
import math
import logging
import os
import queue
import re
import shutil
import subprocess
import threading
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any, Awaitable, Callable

from telegram import Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, Defaults, MessageHandler, filters
from telegram.request import HTTPXRequest

from .archive_ops import run_extract_in_thread
from .common import (
    cleanup_user_workspace,
    collect_extracted_files,
    compute_sha256_async,
    detect_message_file,
    effective_part_size,
    ensure_defaults,
    expected_parts_for_size,
    format_bytes,
    format_media_duration,
    is_archive_file,
    remember_message,
    reset_user_state,
    safe_name,
    split_file_for_upload,
    unique_target_path,
    user_workspace,
    validate_file_ready,
)
from .config import BotConfig
from .constants import (
    CB_MENU_PRINCIPAL,
    CB_CANCELAR,
    CB_COMPRIMIR,
    CB_EXTRAER,
    CB_FINALIZAR_COMPRESION,
    CB_FINALIZAR_EXTRACCION,
    CB_LIMPIAR,
    CB_ENCRYPTAR,
    CB_RENOMBRAR,
    CB_FAVORITO,
    CB_FAVORITO_REMOVE,
    CB_LISTO,
    CB_CANCEL_PROGRESS,
    KEY_ACTION_PROMPT_MESSAGE_ID,
    KEY_ARCHIVES_TO_EXTRACT,
    KEY_COMPRESS_QUEUE_MESSAGE_ID,
    KEY_LAST_COMPRESS_QUEUE_TEXT,
    KEY_LAST_EXTRACT_QUEUE_TEXT,
    KEY_EXTRACT_ARCHIVES_DONE,
    KEY_EXTRACT_TOTAL_RECEIVED,
    KEY_EXTRACT_QUEUE_MESSAGE_ID,
    KEY_PASSWORD_PROMPT_MESSAGE_ID,
    KEY_PENDING_ACTION,
    KEY_PENDING_ACTION_ID,
    KEY_PENDING_FILE_ID,
    KEY_PENDING_FILE_NAME,
    KEY_PENDING_LOCAL_TARGETS,
    KEY_PENDING_SOURCE_MESSAGE_ID,
    KEY_RESULT_ACTIONS,
    KEY_CANCEL_REQUESTED,
    KEY_START_MESSAGE_ID,
    KEY_CURRENT_ARCHIVE,
    KEY_EXTRACTED_FILES,
    KEY_FILES_TO_COMPRESS,
    KEY_KNOWN_PASSWORD,
    KEY_MESSAGE_IDS,
    KEY_MODE,
    KEY_PROGRESS,
    MODE_IDLE,
    MODE_WAIT_COMPRESS_FILES,
    MODE_WAIT_ENCRYPT_PASSWORD,
    MODE_WAIT_EXTRACT_ARCHIVES,
    MODE_WAIT_PASSWORD,
    MODE_WAIT_RENAME,
)
from .exceptions import InvalidPasswordError, Missing7zBackendError, MissingRarBackendError, PasswordRequiredError
from .keyboards import compressed_result_keyboard, compress_keyboard, extract_keyboard, favorite_channel_keyboard, favorite_keyboard, start_keyboard, waiting_upload_keyboard
from .runtime import get_global_heavy_job_semaphore, get_user_lock, get_user_queue_ui_lock
from .telegram_ops import format_bot_text, retry_async, retry_async_with_cancel, send_path_with_size_control, tracked_reply, upsert_progress

logger = logging.getLogger("telegram-compress-bot")


class BotHandlers:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self._cancel_settle_task_key = "__cancel_settle_task"

    @staticmethod
    def _is_fatal_download_config_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return "telegram_api_file_base_url" in text and "config" in text

    async def _maybe_compute_checksum(self, file_path: Path) -> str | None:
        if not self.cfg.enable_checksum:
            return None
        try:
            size = file_path.stat().st_size
        except Exception:
            size = 0
        if self.cfg.checksum_max_bytes > 0 and size > self.cfg.checksum_max_bytes:
            return None
        return await compute_sha256_async(file_path)

    def _download_timeout_seconds(self, file_size_hint: int = 0) -> float:
        base = self.cfg.telegram_read_timeout + self.cfg.telegram_connect_timeout + self.cfg.telegram_pool_timeout
        # Para archivos grandes, el timeout fijo hace que se reinicie cerca del final.
        # Ajuste dinámico: asume al menos ~512 KiB/s (con margen) y limita a 2h.
        size = max(0, int(file_size_hint))
        dynamic = 0.0
        if size > 0:
            dynamic = 120.0 + (size / (512 * 1024))
        return max(90.0, min(7200.0, max(base + 30.0, dynamic)))

    @staticmethod
    def _is_ssl_record_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return (
            "decryption_failed_or_bad_record_mac" in text
            or "bad record mac" in text
            or "sslerror" in text
        )

    async def _download_file_robust(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        file_id: str,
        target_path: Path,
        label: str,
        on_progress: Callable[[int], Awaitable[None]] | None = None,
    ) -> None:
        api_base = (self.cfg.telegram_api_base_url or "").strip().lower()
        if api_base and "api.telegram.org" not in api_base and not self.cfg.telegram_api_file_base_url:
            raise RuntimeError(
                "Configuracion incompleta para Bot API local: definiste TELEGRAM_API_BASE_URL pero falta "
                "TELEGRAM_API_FILE_BASE_URL. Sin base_file_url las descargas pueden quedarse en 0 B. "
                "Configura ambos (por ejemplo en docker-compose) y reintenta."
            )

        async def _download_to_drive_once() -> None:
            if target_path.exists():
                try:
                    target_path.unlink()
                except Exception:
                    pass

            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            meta_timeout = max(30.0, self.cfg.telegram_connect_timeout + self.cfg.telegram_pool_timeout + 10.0)
            tg_file = await asyncio.wait_for(
                context.bot.get_file(
                    file_id,
                    read_timeout=self.cfg.telegram_read_timeout,
                    connect_timeout=self.cfg.telegram_connect_timeout,
                    pool_timeout=self.cfg.telegram_pool_timeout,
                ),
                timeout=meta_timeout,
            )
            file_size_hint = int(getattr(tg_file, "file_size", 0) or 0)
            op_timeout = self._download_timeout_seconds(file_size_hint)

            async def _inner() -> None:
                await tg_file.download_to_drive(
                    custom_path=str(target_path),
                    read_timeout=self.cfg.telegram_read_timeout,
                    write_timeout=self.cfg.telegram_write_timeout,
                    connect_timeout=self.cfg.telegram_connect_timeout,
                    pool_timeout=self.cfg.telegram_pool_timeout,
                )

            async def _with_polling() -> None:
                task = asyncio.create_task(_inner())
                last_reported_size = -1
                last_progress_ts = time.monotonic()
                last_seen_size = 0

                big_file = file_size_hint >= (512 * 1024 * 1024)
                initial_floor = 180.0 if big_file else 60.0
                initial_stall_seconds = max(
                    initial_floor,
                    min(600.0, self.cfg.telegram_connect_timeout + self.cfg.telegram_pool_timeout + 45.0),
                )
                # Si ya hubo progreso, toleramos más antes de considerar “colgado”.
                active_stall_seconds = max(120.0, min(900.0, max(240.0, self.cfg.telegram_read_timeout * 2.0)))

                async def _cancel_task_gracefully(grace_seconds: float = 3.0) -> None:
                    if task.done():
                        return
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=max(0.1, float(grace_seconds)))
                    except Exception:
                        return

                try:
                    while not task.done():
                        if on_progress:
                            try:
                                current_size = target_path.stat().st_size if target_path.exists() else 0
                            except Exception:
                                current_size = 0
                            current_size = max(0, int(current_size))

                            now_ts = time.monotonic()
                            if current_size > last_seen_size:
                                last_seen_size = current_size
                                last_progress_ts = now_ts
                            else:
                                stall_limit = initial_stall_seconds if last_seen_size <= 0 else active_stall_seconds
                                if (now_ts - last_progress_ts) >= stall_limit:
                                    await _cancel_task_gracefully()
                                    raise asyncio.TimeoutError(
                                        f"Descarga sin progreso ({stall_limit:.0f}s) para {label}"
                                    )

                            if current_size != last_reported_size:
                                try:
                                    await on_progress(current_size)
                                except Exception:
                                    pass
                                last_reported_size = current_size
                        await asyncio.sleep(0.35)
                    await task
                finally:
                    await _cancel_task_gracefully()
                if on_progress:
                    try:
                        final_size = target_path.stat().st_size if target_path.exists() else 0
                    except Exception:
                        final_size = 0
                    try:
                        await on_progress(max(0, int(final_size)))
                    except Exception:
                        pass

            await asyncio.wait_for(_with_polling(), timeout=op_timeout)

        try:
            await retry_async_with_cancel(
                self.cfg,
                label,
                _download_to_drive_once,
                cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                timeout_seconds=7200.0,
            )
        except Exception as first_exc:
            if "file is too big" in str(first_exc).lower():
                raise RuntimeError(
                    "Telegram rechazo la descarga por tamano (File is too big). "
                    "Para manejar archivos muy grandes, configura un servidor local de Bot API "
                    "y define TELEGRAM_API_BASE_URL / TELEGRAM_API_FILE_BASE_URL."
                ) from first_exc

            if "descarga sin progreso" in str(first_exc).lower():
                raise RuntimeError(
                    "La descarga se quedo sin progreso (0 B/s) tras varios reintentos. "
                    "Si usas Bot API local, verifica TELEGRAM_API_BASE_URL y TELEGRAM_API_FILE_BASE_URL. "
                    "Si estas en una red con proxy/firewall, revisa conectividad al endpoint de archivos."
                ) from first_exc
            if not self._is_ssl_record_error(first_exc):
                raise

            # Proteccion anti-OOM: evita descargar en memoria archivos grandes.
            try:
                tg_meta = await context.bot.get_file(
                    file_id,
                    read_timeout=self.cfg.telegram_read_timeout,
                    connect_timeout=self.cfg.telegram_connect_timeout,
                    pool_timeout=self.cfg.telegram_pool_timeout,
                )
                file_size_hint = int(getattr(tg_meta, "file_size", 0) or 0)
            except Exception:
                file_size_hint = 0

            # >64 MiB en memoria puede tumbar VPS pequenos (y Dokploy junto al bot).
            if file_size_hint > 64 * 1024 * 1024:
                await retry_async_with_cancel(
                    self.cfg,
                    f"{label} (reintento drive SSL)",
                    _download_to_drive_once,
                    cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                )
                return

            # Fallback para errores SSL transitorios: descarga en memoria y vuelca a disco.
            async def _download_as_bytes_once() -> None:
                async def _inner() -> None:
                    tg_file = await context.bot.get_file(
                        file_id,
                        read_timeout=self.cfg.telegram_read_timeout,
                        connect_timeout=self.cfg.telegram_connect_timeout,
                        pool_timeout=self.cfg.telegram_pool_timeout,
                    )
                    payload = await tg_file.download_as_bytearray(
                        read_timeout=self.cfg.telegram_read_timeout,
                        write_timeout=self.cfg.telegram_write_timeout,
                        connect_timeout=self.cfg.telegram_connect_timeout,
                        pool_timeout=self.cfg.telegram_pool_timeout,
                    )
                    await asyncio.to_thread(target_path.write_bytes, bytes(payload))

                await asyncio.wait_for(_inner(), timeout=self._download_timeout_seconds())

            await retry_async_with_cancel(
                self.cfg,
                f"{label} (fallback SSL)",
                _download_as_bytes_once,
                cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
            )

    def _cleanup_project_residual_files(self) -> dict[str, int]:
        project_root = Path(__file__).resolve().parents[1]
        deleted_files = 0
        deleted_dirs = 0

        # Borra caches y artefactos temporales comunes del proyecto.
        for folder in project_root.rglob("__pycache__"):
            if folder.is_dir():
                shutil.rmtree(folder, ignore_errors=True)
                deleted_dirs += 1

        for folder in project_root.rglob(".pytest_cache"):
            if folder.is_dir():
                shutil.rmtree(folder, ignore_errors=True)
                deleted_dirs += 1

        for folder in project_root.rglob("*_parts"):
            if folder.is_dir():
                shutil.rmtree(folder, ignore_errors=True)
                deleted_dirs += 1

        for file_path in project_root.rglob("*.part???"):
            if file_path.is_file():
                try:
                    file_path.unlink()
                    deleted_files += 1
                except Exception:
                    pass

        for file_path in project_root.rglob("*.log"):
            if file_path.is_file():
                try:
                    file_path.unlink()
                    deleted_files += 1
                except Exception:
                    pass

        return {"files": deleted_files, "dirs": deleted_dirs}

    async def _safe_delete_message(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int) -> None:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass

    async def _progress_heartbeat_loop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            idle_cycles = 0
            while True:
                await asyncio.sleep(max(0.8, self.cfg.progress_min_update_seconds * 2))
                if context.user_data.get(KEY_CANCEL_REQUESTED):
                    return
                progress: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                stage = str(progress.get("stage", ""))
                if not stage:
                    idle_cycles += 1
                    if idle_cycles >= 8:
                        return
                    continue
                idle_cycles = 0
                try:
                    await upsert_progress(
                        update,
                        context,
                        self.cfg,
                        stage,
                        int(progress.get("current", 0)),
                        int(progress.get("total", 1)),
                        str(progress.get("details", "")),
                        force=True,
                        cancellable=bool(progress.get("cancellable", False)),
                        pulse=True,
                    )
                except Exception:
                    # Mantener viva la animacion/progreso ante fallos transitorios de Telegram.
                    continue
        except asyncio.CancelledError:
            return

    def _start_progress_heartbeat(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> asyncio.Task[Any]:
        return asyncio.create_task(self._progress_heartbeat_loop(update, context))

    async def _stop_progress_heartbeat(self, task: asyncio.Task[Any] | None) -> None:
        if not task:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _cancel_settle_watcher(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int | None) -> None:
        max_wait_seconds = 300.0
        started = time.monotonic()
        try:
            while (time.monotonic() - started) < max_wait_seconds:
                await asyncio.sleep(0.5)
                progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                has_progress = bool(progress_state.get("stage"))
                user_busy = bool(user_id and get_user_lock(user_id).locked())

                if user_busy:
                    continue

                # Si ya no hay operacion en curso, limpiamos cualquier rastro UI.
                if not has_progress:
                    context.user_data[KEY_CANCEL_REQUESTED] = False
                    return

                # Si el lock ya no esta ocupado pero queda un progress colgado,
                # limpiamos de forma forzada.
                stage_lower = str(progress_state.get("stage", "")).lower()
                is_terminal = any(token in stage_lower for token in ("complet", "error", "cancel"))
                if is_terminal:
                    await self._cleanup_progress_message(update, context, force=True, delete_message=False, preserve_message_id=True)
                    return

                await self._cleanup_progress_message(update, context, force=True, delete_message=False, preserve_message_id=True)
                return

            # Timeout: no tocar el UI ni flags. Algunas operaciones (p.ej. descargas)
            # pueden seguir emitiendo callbacks de progreso y re-crear el mensaje.
            return
        except asyncio.CancelledError:
            return
        except Exception:
            return

    def _schedule_cancel_settle_watcher(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int | None) -> None:
        existing = context.user_data.get(self._cancel_settle_task_key)
        if isinstance(existing, asyncio.Task) and not existing.done():
            return
        context.user_data[self._cancel_settle_task_key] = asyncio.create_task(
            self._cancel_settle_watcher(update, context, user_id)
        )

    async def _delete_messages_bulk(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        message_ids: list[int],
    ) -> None:
        if not message_ids:
            return
        await asyncio.gather(
            *(self._safe_delete_message(context, chat_id, msg_id) for msg_id in message_ids),
            return_exceptions=True,
        )

    async def _cleanup_tracked_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
        chat = update.effective_chat
        if not chat:
            return

        ids = list(dict.fromkeys(context.user_data.get(KEY_MESSAGE_IDS, [])))
        await self._delete_messages_bulk(context, chat.id, sorted(ids, reverse=True))

        context.user_data[KEY_MESSAGE_IDS] = []
        context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = None
        context.user_data[KEY_EXTRACT_QUEUE_MESSAGE_ID] = None
        context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = None
        context.user_data[KEY_START_MESSAGE_ID] = None
        context.user_data[KEY_ACTION_PROMPT_MESSAGE_ID] = None
        context.user_data[KEY_PROGRESS] = {}

    async def _cleanup_progress_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        force: bool = False,
        delete_message: bool = True,
        reset_cancel: bool = True,
        preserve_message_id: bool = False,
    ) -> None:
        ensure_defaults(context)
        chat = update.effective_chat
        if not chat:
            return
        progress: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
        stage = str(progress.get("stage", "")).lower()
        if not force:
            terminal_stage = any(token in stage for token in ("complet", "error", "cancel"))
            if stage and not terminal_stage:
                return
        msg_id = progress.get("message_id")
        progress_ids = list(dict.fromkeys(progress.get("message_ids", [])))
        global_progress_ids = list(dict.fromkeys(context.user_data.get("__progress_message_ids", [])))
        progress_ids.extend(global_progress_ids)
        if msg_id:
            progress_ids.append(int(msg_id))
        if delete_message and progress_ids:
            await asyncio.gather(
                *(self._safe_delete_message(context, chat.id, int(progress_id)) for progress_id in progress_ids),
                return_exceptions=True,
            )
        if delete_message:
            context.user_data["__progress_message_ids"] = []

        if preserve_message_id and msg_id and not delete_message:
            # Conserva el ancla del mensaje para reusar siempre el mismo progress UI.
            context.user_data[KEY_PROGRESS] = {"message_id": msg_id}
        else:
            context.user_data[KEY_PROGRESS] = {}
        if reset_cancel:
            context.user_data[KEY_CANCEL_REQUESTED] = False

    def _reset_cancel_request(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        settle_task = context.user_data.get(self._cancel_settle_task_key)
        if isinstance(settle_task, asyncio.Task) and not settle_task.done():
            settle_task.cancel()
        context.user_data[self._cancel_settle_task_key] = None
        context.user_data[KEY_CANCEL_REQUESTED] = False

    def _raise_if_cancel_requested(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        if bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)):
            raise RuntimeError("Operacion cancelada por el usuario")

    async def _cleanup_password_prompt_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
        chat = update.effective_chat
        if not chat:
            return
        msg_id = context.user_data.get(KEY_PASSWORD_PROMPT_MESSAGE_ID)
        start_msg_id = context.user_data.get(KEY_START_MESSAGE_ID)
        if msg_id and start_msg_id and int(msg_id) == int(start_msg_id):
            # Si reutilizamos el mensaje principal con botones, no lo borramos: lo restauramos.
            try:
                await context.bot.edit_message_text(
                    chat_id=chat.id,
                    message_id=int(start_msg_id),
                    text=format_bot_text("📂 Modo extracción activado\nEnvía los archivos comprimidos que deseas extraer."),
                    reply_markup=waiting_upload_keyboard(),
                    parse_mode="HTML",
                )
            except Exception:
                pass
        elif msg_id:
            await self._safe_delete_message(context, chat.id, int(msg_id))
        context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = None

    async def _upsert_password_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        ensure_defaults(context)
        chat = update.effective_chat
        if not chat:
            return

        target_msg_id = context.user_data.get(KEY_START_MESSAGE_ID) or context.user_data.get(KEY_EXTRACT_QUEUE_MESSAGE_ID)
        safe_text = format_bot_text(text)
        if target_msg_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat.id,
                    message_id=int(target_msg_id),
                    text=safe_text,
                    reply_markup=waiting_upload_keyboard(),
                    parse_mode="HTML",
                )
                context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = int(target_msg_id)
                return
            except BadRequest as exc:
                if "message is not modified" not in str(exc).lower():
                    pass
            except Exception:
                pass

        await tracked_reply(update, context, text, reply_markup=waiting_upload_keyboard(), force_new_message=True)
        prompt_id = context.user_data.get(KEY_MESSAGE_IDS, [])[-1] if context.user_data.get(KEY_MESSAGE_IDS) else None
        context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = prompt_id

    async def _cleanup_action_prompt_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
        chat = update.effective_chat
        if not chat:
            return
        msg_id = context.user_data.get(KEY_ACTION_PROMPT_MESSAGE_ID)
        if msg_id:
            await self._safe_delete_message(context, chat.id, int(msg_id))
        context.user_data[KEY_ACTION_PROMPT_MESSAGE_ID] = None

    def _clear_pending_action(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data[KEY_PENDING_ACTION] = None
        context.user_data[KEY_PENDING_ACTION_ID] = None
        context.user_data[KEY_PENDING_FILE_ID] = None
        context.user_data[KEY_PENDING_FILE_NAME] = None
        context.user_data[KEY_PENDING_SOURCE_MESSAGE_ID] = None

    @staticmethod
    def _recommended_upload_part_size(size_bytes: int, base_part_size: int) -> int:
        size_bytes = max(1, int(size_bytes))
        base_part_size = max(1, int(base_part_size))
        # Respeta el tamaño configurado (p.ej. 2GB) para permitir el máximo permitido por Telegram.
        return base_part_size

    @staticmethod
    def _recommended_upload_timeout(size_bytes: int, base_timeout: float) -> float:
        size_bytes = max(1, int(size_bytes))
        base_timeout = max(60.0, float(base_timeout))
        estimated = (size_bytes / (5 * 1024 * 1024)) + 600.0  # margen amplio para Bot API local y VPS lentos
        return max(base_timeout, min(7200.0, estimated))


    def _split_extract_targets(self, downloaded_files: list[Path]) -> tuple[list[Path], list[str]]:
        files_sorted = sorted(downloaded_files, key=lambda p: p.name.lower())
        name_map: dict[str, Path] = {path.name.lower(): path for path in files_sorted}
        targets: list[Path] = []
        warnings: list[str] = []

        for path in files_sorted:
            lower_name = path.name.lower()

            if re.search(r"\.z\d{2}$", lower_name):
                base_name = re.sub(r"\.z\d{2}$", ".zip", lower_name)
                if base_name not in name_map:
                    warnings.append(f"Falta parte principal para {path.name} (se requiere {base_name}).")
                continue

            if re.search(r"\.part\d+\.rar$", lower_name):
                match = re.search(r"\.part(\d+)\.rar$", lower_name)
                if not match:
                    continue
                part_number = int(match.group(1))
                if part_number == 1:
                    targets.append(path)
                else:
                    first_part = re.sub(r"\.part\d+\.rar$", ".part1.rar", lower_name)
                    if first_part not in name_map:
                        warnings.append(f"Falta {first_part} para iniciar serie de {path.name}.")
                continue

            if re.search(r"\.r\d{2}$", lower_name):
                base_name = re.sub(r"\.r\d{2}$", ".rar", lower_name)
                if base_name not in name_map:
                    warnings.append(f"Falta parte principal para {path.name} (se requiere {base_name}).")
                continue

            if re.search(r"\.7z\.\d{3}$", lower_name):
                match = re.search(r"\.7z\.(\d{3})$", lower_name)
                if not match:
                    continue
                part_number = int(match.group(1))
                if part_number == 1:
                    targets.append(path)
                else:
                    first_part = re.sub(r"\.7z\.\d{3}$", ".7z.001", lower_name)
                    if first_part not in name_map:
                        warnings.append(f"Falta {first_part} para iniciar serie de {path.name}.")
                continue

            if re.search(r"\.(zip|rar)\.\d{3}$", lower_name):
                match = re.search(r"\.(zip|rar)\.(\d{3})$", lower_name)
                if not match:
                    continue
                ext = match.group(1)
                part_number = int(match.group(2))
                if part_number == 1:
                    targets.append(path)
                else:
                    first_part = re.sub(r"\.(zip|rar)\.\d{3}$", f".{ext}.001", lower_name)
                    if first_part not in name_map:
                        warnings.append(f"Falta {first_part} para iniciar serie de {path.name}.")
                continue

            if is_archive_file(path.name):
                targets.append(path)

        unique_targets: list[Path] = []
        seen = set()
        for item in targets:
            token = str(item).lower()
            if token in seen:
                continue
            seen.add(token)
            unique_targets.append(item)
        return unique_targets, warnings

    async def _extract_local_targets(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        targets: list[Path],
        initial_queue_total: int,
        password: str | None,
    ) -> bool:
        user = update.effective_user
        if not user:
            return True

        workspace = user_workspace(user.id)
        output_root = workspace / "extract_output"
        output_root.mkdir(parents=True, exist_ok=True)

        extracted_files: list[str] = context.user_data[KEY_EXTRACTED_FILES]
        extracted_archives = int(context.user_data.get(KEY_EXTRACT_ARCHIVES_DONE, 0))
        extract_bytes_done = int(context.user_data.get("extract_bytes_done", 0))
        targets_total_bytes = sum(path.stat().st_size for path in targets if path.exists())
        total_extract_bytes = max(extract_bytes_done + targets_total_bytes, 1)

        for index, local_archive in enumerate(targets, start=1):
            self._raise_if_cancel_requested(context)
            out_dir = output_root / f"{Path(local_archive).stem}_{uuid.uuid4().hex[:8]}"
            out_dir.mkdir(parents=True, exist_ok=True)

            current_password = password if password is not None else context.user_data.get(KEY_KNOWN_PASSWORD)

            try:
                archive_size_hint = 0
                try:
                    archive_size_hint = int(local_archive.stat().st_size)
                except Exception:
                    archive_size_hint = 0
                archive_size_hint = max(1, archive_size_hint)

                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Descomprimiendo archivos",
                    0,
                    archive_size_hint,
                    (
                        f"Procesando {index}/{len(targets)}"
                    ),
                    cancellable=True,
                )
                heartbeat_task = self._start_progress_heartbeat(update, context)

                def _compute_dir_size_bytes(root: Path) -> int:
                    total = 0
                    try:
                        stack = [str(root)]
                        while stack:
                            path = stack.pop()
                            try:
                                with os.scandir(path) as it:
                                    for entry in it:
                                        try:
                                            if entry.is_dir(follow_symlinks=False):
                                                stack.append(entry.path)
                                            elif entry.is_file(follow_symlinks=False):
                                                total += int(entry.stat(follow_symlinks=False).st_size)
                                        except Exception:
                                            continue
                            except FileNotFoundError:
                                continue
                            except Exception:
                                continue
                    except Exception:
                        return 0
                    return max(0, int(total))

                async def _poll_extract_progress() -> None:
                    last_size = -1
                    while True:
                        if bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)):
                            return
                        try:
                            current_size = await asyncio.to_thread(_compute_dir_size_bytes, out_dir)
                        except Exception:
                            current_size = 0
                        current_size = max(0, int(current_size))
                        if current_size != last_size:
                            total_hint = max(archive_size_hint, current_size, 1)
                            progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                            progress_state["stage"] = "Descomprimiendo archivos"
                            progress_state["current"] = min(current_size, total_hint)
                            progress_state["total"] = total_hint
                            progress_state["details"] = f"Procesando {index}/{len(targets)}"
                            progress_state["cancellable"] = True
                            context.user_data[KEY_PROGRESS] = progress_state
                            last_size = current_size
                        await asyncio.sleep(0.75)

                try:
                    async with get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs):
                        poll_task = asyncio.create_task(_poll_extract_progress())
                        try:
                            await run_extract_in_thread(
                                local_archive,
                                out_dir,
                                password=current_password,
                                cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                            )
                        finally:
                            poll_task.cancel()
                            try:
                                await poll_task
                            except asyncio.CancelledError:
                                pass
                finally:
                    await self._stop_progress_heartbeat(heartbeat_task)
            except PasswordRequiredError:
                await self._cleanup_password_prompt_message(update, context)
                context.user_data[KEY_CURRENT_ARCHIVE] = {
                    "file_name": local_archive.name,
                    "local_archive": str(local_archive),
                    "output_dir": str(out_dir),
                }
                context.user_data[KEY_PENDING_LOCAL_TARGETS] = [str(path) for path in targets[index:]]
                context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Waiting for password",
                    extracted_archives,
                    max(initial_queue_total, len(targets), 1),
                    f"Password required for {local_archive.name}",
                    force=True,
                    cancellable=True,
                )
                await self._upsert_password_prompt(
                    update,
                    context,
                    f"El archivo '{local_archive.name}' requiere password. Escríbela para continuar.",
                )
                return False
            except InvalidPasswordError:
                await self._cleanup_password_prompt_message(update, context)
                context.user_data[KEY_CURRENT_ARCHIVE] = {
                    "file_name": local_archive.name,
                    "local_archive": str(local_archive),
                    "output_dir": str(out_dir),
                }
                context.user_data[KEY_PENDING_LOCAL_TARGETS] = [str(path) for path in targets[index:]]
                context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Waiting for password",
                    extracted_archives,
                    max(initial_queue_total, len(targets), 1),
                    f"Wrong password for {local_archive.name}",
                    force=True,
                    cancellable=True,
                )
                await self._upsert_password_prompt(
                    update,
                    context,
                    "Password incorrecta. Escribe una nueva password para reintentar.",
                )
                return False
            except (MissingRarBackendError, Missing7zBackendError) as exc:
                await tracked_reply(update, context, str(exc), reply_markup=extract_keyboard())
                continue
            except Exception as exc:
                await tracked_reply(update, context, f"No se pudo extraer '{local_archive.name}'. Detalle: {exc}", reply_markup=extract_keyboard())
                continue

            extracted_payloads = list(collect_extracted_files(out_dir))
            extracted_files.extend(str(path) for path in extracted_payloads)
            extracted_archives += 1
            context.user_data[KEY_EXTRACT_ARCHIVES_DONE] = extracted_archives
            extract_bytes_done += max(0, local_archive.stat().st_size)
            context.user_data["extract_bytes_done"] = extract_bytes_done
            await upsert_progress(
                update,
                context,
                self.cfg,
                "Descomprimiendo archivos",
                min(extract_bytes_done, total_extract_bytes),
                total_extract_bytes,
                f"Procesando {index}/{len(targets)}",
                cancellable=True,
            )

        return True

    def _register_result_action(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        file_id: str,
        file_name: str,
        message_id: int,
        action_id: str | None = None,
        parts: list[dict[str, Any]] | None = None,
    ) -> str:
        ensure_defaults(context)
        final_action_id = action_id or uuid.uuid4().hex[:10]
        result_actions: dict[str, dict[str, Any]] = context.user_data[KEY_RESULT_ACTIONS]
        payload: dict[str, Any] = {
            "file_id": file_id,
            "file_name": file_name,
            "message_id": message_id,
        }
        if parts:
            payload["parts"] = parts
        result_actions[final_action_id] = payload
        if len(result_actions) > 30:
            oldest_keys = list(result_actions.keys())[:-30]
            for key in oldest_keys:
                result_actions.pop(key, None)
        return final_action_id

    def _forget_result_action(self, context: ContextTypes.DEFAULT_TYPE, action_id: str) -> None:
        ensure_defaults(context)
        result_actions: dict[str, dict[str, Any]] = context.user_data[KEY_RESULT_ACTIONS]
        result_actions.pop(action_id, None)

    async def _save_action_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        await self._cleanup_action_prompt_message(update, context)
        await tracked_reply(update, context, text)
        ids = context.user_data.get(KEY_MESSAGE_IDS, [])
        context.user_data[KEY_ACTION_PROMPT_MESSAGE_ID] = ids[-1] if ids else None

    async def ensure_authorized(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        user = update.effective_user
        chat = update.effective_chat
        target_message = update.message or (update.callback_query.message if update.callback_query else None)
        if not user or not chat:
            return False

        if self.cfg.only_private_chat and getattr(chat, "type", "") != "private":
            if target_message:
                sent = await target_message.reply_text("🔒 <b>Acceso restringido</b>\nEste bot opera solo en chat privado.")
                remember_message(update, context, sent.message_id)
            return False

        if self.cfg.allowed_user_ids and user.id not in self.cfg.allowed_user_ids:
            if target_message:
                sent = await target_message.reply_text("⛔ <b>Acceso denegado</b>\nTu usuario no está autorizado para usar este bot.")
                remember_message(update, context, sent.message_id)
            return False

        return True

    async def start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.ensure_authorized(update, context):
            return
        ensure_defaults(context)
        reset_user_state(context)
        await tracked_reply(
            update,
            context,
            (
                "👋 <b>Bienvenido al Centro de Compresión</b>\n\n"
                "Selecciona una acción para continuar:\n"
                "• 📂 Extraer archivos\n"
                "• 🗜️ Comprimir archivos\n"
                "• 🧹 Limpiar residuos temporales\n\n"
                "💡 Usa <code>/status</code> para revisar el estado técnico y el progreso en tiempo real."
            ),
            reply_markup=start_keyboard(),
            is_html=True,
        )
        ids = context.user_data.get(KEY_MESSAGE_IDS, [])
        context.user_data[KEY_START_MESSAGE_ID] = ids[-1] if ids else None

    async def status_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.ensure_authorized(update, context):
            return
        remember_message(update, context)
        ensure_defaults(context)
        user = update.effective_user

        mode = context.user_data.get(KEY_MODE, MODE_IDLE)
        files_to_compress = context.user_data.get(KEY_FILES_TO_COMPRESS, [])
        archives_to_extract = context.user_data.get(KEY_ARCHIVES_TO_EXTRACT, [])
        extracted_files = context.user_data.get(KEY_EXTRACTED_FILES, [])
        progress: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})

        progress_stage = progress.get("stage", "sin actividad")
        progress_percent = float(progress.get("last_percent", 0.0))
        progress_percent = max(0.0, min(progress_percent, 100.0))
        busy = False
        if user:
            busy = get_user_lock(user.id).locked()

        base_url = self.cfg.telegram_api_base_url or "default"
        file_base_url = self.cfg.telegram_api_file_base_url or "default"

        text = (
            "📊 <b>Panel de Estado</b>\n\n"
            f"• Modo actual: <code>{mode}</code>\n"
            f"• Cola de compresión: <b>{len(files_to_compress)}</b> archivo(s)\n"
            f"• Cola de extracción: <b>{len(archives_to_extract)}</b> archivo(s)\n"
            f"• Pendientes de envío: <b>{len(extracted_files)}</b>\n"
            f"• Progreso activo: <b>{progress_stage}</b> ({progress_percent:.1f}%)\n"
            f"• API base URL: <code>{base_url}</code>\n"
            f"• API file URL: <code>{file_base_url}</code>\n"
            f"• Concurrencia descarga/subida: <b>{self.cfg.max_download_concurrency}/{self.cfg.max_upload_concurrency}</b>\n"
            f"• Reintentos máximos: <b>{self.cfg.max_retries}</b>\n"
            f"• Checksum SHA-256: <b>{'activo' if self.cfg.enable_checksum else 'inactivo'}</b>\n"
            f"• Operación en curso: <b>{'sí' if busy else 'no'}</b>"
        )
        await tracked_reply(update, context, text, reply_markup=start_keyboard(), is_html=True)

    async def ask_compress(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
        current_mode = context.user_data.get(KEY_MODE, MODE_IDLE)
        queued_files = context.user_data.get(KEY_FILES_TO_COMPRESS, [])
        if current_mode == MODE_WAIT_COMPRESS_FILES and isinstance(queued_files, list) and queued_files:
            await tracked_reply(
                update,
                context,
                (
                    "🗜️ <b>Modo compresión ya activo</b>\n"
                    f"Actualmente tienes <b>{len(queued_files)}</b> archivo(s) en cola.\n"
                    "Envía más archivos o pulsa Finalizar compresión."
                ),
                reply_markup=compress_keyboard(),
                is_html=True,
            )
            return

        reset_user_state(context)
        context.user_data[KEY_MODE] = MODE_WAIT_COMPRESS_FILES
        await tracked_reply(
            update,
            context,
            "🗜️ <b>Modo compresión activado</b>\nEnvía o reenvía los archivos que deseas comprimir.",
            reply_markup=waiting_upload_keyboard(),
            is_html=True,
        )
        ids = context.user_data.get(KEY_MESSAGE_IDS, [])
        context.user_data[KEY_START_MESSAGE_ID] = ids[-1] if ids else None

    async def ask_extract(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
        current_mode = context.user_data.get(KEY_MODE, MODE_IDLE)
        queued_archives = context.user_data.get(KEY_ARCHIVES_TO_EXTRACT, [])
        if current_mode == MODE_WAIT_EXTRACT_ARCHIVES and isinstance(queued_archives, list) and queued_archives:
            await tracked_reply(
                update,
                context,
                (
                    "📂 <b>Modo extracción ya activo</b>\n"
                    f"Actualmente tienes <b>{len(queued_archives)}</b> archivo(s) en cola.\n"
                    "Envía más comprimidos o pulsa Finalizar extracción."
                ),
                reply_markup=extract_keyboard(),
                is_html=True,
            )
            return

        reset_user_state(context)
        context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES
        await tracked_reply(
            update,
            context,
            "📂 <b>Modo extracción activado</b>\nEnvía los archivos comprimidos que deseas extraer.",
            reply_markup=waiting_upload_keyboard(),
            is_html=True,
        )
        ids = context.user_data.get(KEY_MESSAGE_IDS, [])
        context.user_data[KEY_START_MESSAGE_ID] = ids[-1] if ids else None

    async def cancel_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        has_active_progress = bool(context.user_data.get(KEY_PROGRESS, {}).get("stage"))
        if user and (get_user_lock(user.id).locked() or has_active_progress):
            context.user_data[KEY_CANCEL_REQUESTED] = True
            self._schedule_cancel_settle_watcher(update, context, user.id)
            progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
            current = int(progress_state.get("current", 0))
            total = int(progress_state.get("total", 1))
            if total <= 0:
                total = 1
            await upsert_progress(
                update,
                context,
                self.cfg,
                "Cancelando",
                min(max(current, 0), total),
                total,
                "La operación se detendrá en el siguiente punto seguro.",
                force=True,
                cancellable=False,
            )
            return

        if user:
            cleanup_user_workspace(user.id, recreate=False)
        reset_user_state(context)
        await self._cleanup_tracked_messages(update, context)
        await tracked_reply(update, context, "✅ Operación cancelada. El estado ha sido restablecido.", reply_markup=start_keyboard())

    async def clear_chat_and_files(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
        user = update.effective_user
        chat = update.effective_chat
        if not user or not chat:
            return

        cleanup_user_workspace(user.id, recreate=False)
        cleanup_result = self._cleanup_project_residual_files()
        reset_user_state(context)

        await tracked_reply(
            update,
            context,
            (
                "Limpieza completada de archivos residuales.\n"
                f"Carpetas borradas: {cleanup_result['dirs']}\n"
                f"Archivos borrados: {cleanup_result['files']}"
            ),
            reply_markup=start_keyboard(),
        )

    async def queue_file_for_compression(self, update: Update, context: ContextTypes.DEFAULT_TYPE, file_info: dict[str, Any]) -> None:
        user = update.effective_user
        if not user:
            return
        files_to_compress: list[dict[str, Any]] = context.user_data[KEY_FILES_TO_COMPRESS]
        files_to_compress.append(file_info)
        size_text = format_bytes(int(file_info.get("file_size", 0)))
        duration_seconds = int(file_info.get("duration_seconds", 0) or 0)
        duration_text = format_media_duration(duration_seconds) if duration_seconds > 0 else ""
        media_extra = f" | Duracion: {duration_text}" if duration_text else ""
        total = len(files_to_compress)
        queue_text = (
            "Cola de compresion actualizada\n"
            f"Ultimo agregado: {file_info['file_name']} ({size_text}{media_extra})\n"
            f"Total en cola: {total} archivo(s)\n"
            "Pulsa Finalizar compresion para iniciar el proceso."
        )

        async with get_user_queue_ui_lock(user.id):
            if queue_text == str(context.user_data.get(KEY_LAST_COMPRESS_QUEUE_TEXT, "")):
                return

            msg_id = context.user_data.get(KEY_COMPRESS_QUEUE_MESSAGE_ID)
            chat = update.effective_chat
            if chat and msg_id:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat.id,
                        message_id=msg_id,
                        text=format_bot_text(queue_text),
                        reply_markup=compress_keyboard(),
                    )
                    context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = queue_text
                    return
                except BadRequest:
                    pass

            if update.message:
                sent = await update.message.reply_text(format_bot_text(queue_text), reply_markup=compress_keyboard())
                remember_message(update, context, sent.message_id)
                context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = sent.message_id
                context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = queue_text

    async def queue_file_for_extraction(self, update: Update, context: ContextTypes.DEFAULT_TYPE, file_info: dict[str, Any]) -> None:
        user = update.effective_user
        if not user:
            return
        queue: list[dict[str, Any]] = context.user_data[KEY_ARCHIVES_TO_EXTRACT]
        queue.append(file_info)
        context.user_data[KEY_EXTRACT_TOTAL_RECEIVED] = int(context.user_data.get(KEY_EXTRACT_TOTAL_RECEIVED, 0)) + 1

        size_text = format_bytes(int(file_info.get("file_size", 0)))
        total = len(queue)
        queue_text = (
            "Cola de extraccion actualizada\n"
            f"Ultimo agregado: {file_info['file_name']} ({size_text})\n"
            f"Total en cola: {total} archivo(s)\n"
            "Pulsa Finalizar extraccion para iniciar el proceso."
        )

        async with get_user_queue_ui_lock(user.id):
            if queue_text == str(context.user_data.get(KEY_LAST_EXTRACT_QUEUE_TEXT, "")):
                return

            msg_id = context.user_data.get(KEY_EXTRACT_QUEUE_MESSAGE_ID)
            chat = update.effective_chat
            if chat and msg_id:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat.id,
                        message_id=msg_id,
                        text=format_bot_text(queue_text),
                        reply_markup=extract_keyboard(),
                    )
                    context.user_data[KEY_LAST_EXTRACT_QUEUE_TEXT] = queue_text
                    return
                except BadRequest:
                    pass

            if update.message:
                sent = await update.message.reply_text(format_bot_text(queue_text), reply_markup=extract_keyboard())
                remember_message(update, context, sent.message_id)
                context.user_data[KEY_EXTRACT_QUEUE_MESSAGE_ID] = sent.message_id
                context.user_data[KEY_LAST_EXTRACT_QUEUE_TEXT] = queue_text

    async def finalize_compression(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.ensure_authorized(update, context):
            return
        files_to_compress: list[dict[str, Any]] = context.user_data[KEY_FILES_TO_COMPRESS]
        user = update.effective_user
        chat = update.effective_chat
        if not user or not chat:
            return

        if not files_to_compress:
            await tracked_reply(update, context, "No hay archivos en cola para compresion. Envia archivos primero.", reply_markup=compress_keyboard())
            return

        user_lock = get_user_lock(user.id)
        if user_lock.locked():
            await tracked_reply(update, context, "Ya existe una operacion activa para este usuario. Espera a que finalice.", reply_markup=compress_keyboard())
            return

        await context.bot.send_chat_action(chat.id, ChatAction.UPLOAD_DOCUMENT)

        workspace = user_workspace(user.id)
        inbox = workspace / "compress_input"
        outbox = workspace / "compress_output"
        shutil.rmtree(inbox, ignore_errors=True)
        shutil.rmtree(outbox, ignore_errors=True)
        inbox.mkdir(parents=True, exist_ok=True)
        outbox.mkdir(parents=True, exist_ok=True)

        files_to_compress = sorted(
            files_to_compress,
            key=lambda it: (int(it.get("file_size", 0)), str(it.get("file_name", ""))),
        )

        total_download_bytes = sum(int(item.get("file_size", 0)) for item in files_to_compress)
        downloaded_bytes = 0
        downloaded_count = 0
        failed_downloads: list[dict[str, Any]] = []
        inflight_download_bytes: dict[str, int] = {}
        progress_lock = asyncio.Lock()
        download_sem = asyncio.Semaphore(max(1, self.cfg.max_download_concurrency))
        self._reset_cancel_request(context)
        heartbeat_task: asyncio.Task[Any] | None = None
        heavy_gate = get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs)

        async with user_lock:
            await heavy_gate.acquire()
            compression_summary_text: str | None = None
            try:
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Descargando archivos",
                    0,
                    max(total_download_bytes, len(files_to_compress)),
                    f"0/{len(files_to_compress)} archivos descargados",
                    force=True,
                    cancellable=True,
                )
                heartbeat_task = self._start_progress_heartbeat(update, context)

                async def download_one(item: dict[str, Any]) -> None:
                    nonlocal downloaded_bytes, downloaded_count
                    self._raise_if_cancel_requested(context)
                    async with download_sem:
                        self._raise_if_cancel_requested(context)
                        target = unique_target_path(inbox, item["file_name"])

                        try:
                            item_key = str(item["file_id"])
                            async with progress_lock:
                                inflight_download_bytes[item_key] = 0

                            async def _on_partial(size_now: int) -> None:
                                async with progress_lock:
                                    # No clamping: en archivos grandes el file_size del update puede ser inexacto.
                                    inflight_download_bytes[item_key] = max(0, int(size_now))
                                    partial_sum = sum(inflight_download_bytes.values())
                                    current_total = min(
                                        max(total_download_bytes, 1),
                                        max(0, downloaded_bytes + partial_sum),
                                    )
                                    progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                                    progress_state["current"] = current_total
                                    progress_state["total"] = max(total_download_bytes, current_total, 1)
                                    progress_state["details"] = f"{downloaded_count}/{len(files_to_compress)} archivos | {format_bytes(current_total)} descargados"
                                    progress_state["cancellable"] = True
                                    context.user_data[KEY_PROGRESS] = progress_state

                            await self._download_file_robust(
                                context,
                                str(item["file_id"]),
                                target,
                                f"Descarga {item['file_name']}",
                                on_progress=_on_partial,
                            )
                            validate_file_ready(target)
                            _ = await self._maybe_compute_checksum(target)

                            real_size = target.stat().st_size
                            async with progress_lock:
                                inflight_download_bytes.pop(item_key, None)
                                downloaded_count += 1
                                downloaded_bytes += max(0, real_size)
                                await upsert_progress(
                                    update,
                                    context,
                                    self.cfg,
                                    "Descargando archivos",
                                    downloaded_bytes,
                                    max(total_download_bytes, downloaded_bytes, 1),
                                    f"{downloaded_count}/{len(files_to_compress)} archivos | {format_bytes(downloaded_bytes)} descargados",
                                    cancellable=True,
                                )
                        except RuntimeError as exc:
                            async with progress_lock:
                                inflight_download_bytes.pop(str(item["file_id"]), None)
                            if "cancelada" in str(exc).lower():
                                raise
                            if self._is_fatal_download_config_error(exc):
                                raise
                            async with progress_lock:
                                failed_downloads.append(item)
                        except Exception:
                            async with progress_lock:
                                inflight_download_bytes.pop(str(item["file_id"]), None)
                            async with progress_lock:
                                failed_downloads.append(item)

                download_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
                for item in files_to_compress:
                    download_queue.put_nowait(item)

                worker_count = max(1, min(len(files_to_compress), self.cfg.max_download_concurrency))

                async def _download_worker() -> None:
                    while True:
                        try:
                            item = download_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            return
                        try:
                            await download_one(item)
                        finally:
                            download_queue.task_done()

                await asyncio.gather(*(_download_worker() for _ in range(worker_count)))
                self._raise_if_cancel_requested(context)

                # Segunda pasada en serie para reducir omisiones por fallos transitorios.
                unresolved: list[str] = []
                if failed_downloads:
                    retry_candidates = list(failed_downloads)
                    failed_downloads.clear()
                    for item in retry_candidates:
                        self._raise_if_cancel_requested(context)
                        target = unique_target_path(inbox, item["file_name"])
                        try:
                            await self._download_file_robust(
                                context,
                                str(item["file_id"]),
                                target,
                                f"Reintento descarga {item['file_name']}",
                            )
                            validate_file_ready(target)
                            _ = await self._maybe_compute_checksum(target)
                            real_size = target.stat().st_size
                            downloaded_count += 1
                            downloaded_bytes += max(0, real_size)
                            await upsert_progress(
                                update,
                                context,
                                self.cfg,
                                "Descargando archivos",
                                downloaded_bytes,
                                max(total_download_bytes, downloaded_bytes, 1),
                                f"{downloaded_count}/{len(files_to_compress)} archivos | {format_bytes(downloaded_bytes)} descargados",
                                cancellable=True,
                            )
                        except RuntimeError as exc:
                            if "cancelada" in str(exc).lower():
                                raise
                            if self._is_fatal_download_config_error(exc):
                                raise
                            unresolved.append(str(item.get("file_name", "archivo")))
                        except Exception:
                            unresolved.append(str(item.get("file_name", "archivo")))

                if downloaded_count == 0:
                    raise RuntimeError(
                        "No fue posible descargar ningun archivo. Verifica red/Telegram y vuelve a intentarlo."
                    )

                if unresolved:
                    preview = ", ".join(unresolved[:8])
                    extra = "" if len(unresolved) <= 8 else f" y {len(unresolved) - 8} mas"
                    raise RuntimeError(
                        f"No se pudo descargar la cola completa. Archivos pendientes: {preview}{extra}"
                    )

                archive_name = f"comprimido_{uuid.uuid4().hex[:8]}.zip"
                archive_path = outbox / archive_name

                files_for_zip = sorted(
                    [path for path in inbox.rglob("*") if path.is_file()],
                    key=lambda p: (p.stat().st_size, p.name),
                )
                if not files_for_zip:
                    raise RuntimeError("No quedaron archivos validos para comprimir tras las validaciones")
                total_compress_bytes = sum(path.stat().st_size for path in files_for_zip)
                compressed_progress = 0
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Comprimiendo archivos",
                    0,
                    max(total_compress_bytes, len(files_for_zip), 1),
                    f"0/{len(files_for_zip)} archivos comprimidos",
                    force=True,
                    cancellable=True,
                )

                # Construir el ZIP en un thread para no bloquear el event-loop.
                # Ademas, escribimos por chunks para poder reportar progreso real y permitir cancelacion.
                zip_state: dict[str, Any] = {
                    "bytes_done": 0,
                    "file_index": 0,
                    "file_name": "",
                }

                def _build_zip() -> None:
                    chunk_size = 8 * 1024 * 1024
                    with zipfile.ZipFile(
                        archive_path,
                        "w",
                        compression=zipfile.ZIP_DEFLATED,
                        compresslevel=self.cfg.zip_compress_level,
                        allowZip64=True,
                    ) as zf:
                        for index, file_path in enumerate(files_for_zip, start=1):
                            if bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)):
                                raise RuntimeError("Operacion cancelada por el usuario")
                            zip_state["file_index"] = index
                            zip_state["file_name"] = file_path.name

                            # Escribir por chunks evita que quede “congelado” durante un solo archivo grande.
                            with file_path.open("rb") as src, zf.open(file_path.name, "w") as dst:
                                while True:
                                    if bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)):
                                        raise RuntimeError("Operacion cancelada por el usuario")
                                    chunk = src.read(chunk_size)
                                    if not chunk:
                                        break
                                    dst.write(chunk)
                                    zip_state["bytes_done"] = int(zip_state.get("bytes_done", 0)) + len(chunk)

                build_task = asyncio.create_task(asyncio.to_thread(_build_zip))
                last_report_ts = 0.0
                while not build_task.done():
                    self._raise_if_cancel_requested(context)
                    now_ts = time.monotonic()
                    if (now_ts - last_report_ts) >= 1.0:
                        last_report_ts = now_ts
                        bytes_done = int(zip_state.get("bytes_done", 0))
                        file_index = int(zip_state.get("file_index", 0))
                        file_name = str(zip_state.get("file_name") or "")
                        compressed_progress = bytes_done
                        await upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Comprimiendo archivos",
                            min(bytes_done, max(total_compress_bytes, 1)),
                            max(total_compress_bytes, 1),
                            (
                                f"{min(file_index, len(files_for_zip))}/{len(files_for_zip)} archivos | "
                                f"{format_bytes(min(bytes_done, total_compress_bytes))} de {format_bytes(total_compress_bytes)} | "
                                f"{file_name}"
                            ),
                            cancellable=True,
                        )
                    await asyncio.sleep(0.25)

                # Propagar errores del thread (incluye cancelacion)
                await build_task
                compressed_progress = int(zip_state.get("bytes_done", total_compress_bytes))
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Comprimiendo archivos",
                    max(total_compress_bytes, 1),
                    max(total_compress_bytes, 1),
                    f"{len(files_for_zip)}/{len(files_for_zip)} archivos | {format_bytes(total_compress_bytes)} procesados",
                    force=True,
                    cancellable=True,
                )

                validate_file_ready(archive_path)
                _ = await self._maybe_compute_checksum(archive_path)

                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Preparando subida",
                    0,
                    1,
                    "Dividiendo resultado en partes para subida",
                    force=True,
                    cancellable=True,
                )
                self._raise_if_cancel_requested(context)
                def _should_split_smaller(exc: Exception) -> bool:
                    if isinstance(exc, (asyncio.TimeoutError, TimedOut, NetworkError)):
                        return True
                    text = str(exc).lower()
                    if any(token in text for token in ("file is too big", "entity too large", "request entity too large", "file too large")):
                        return True
                    if any(token in text for token in ("timed out", "timeout", "read timed out", "network")):
                        return True
                    return False

                def _cleanup_parts_dir() -> None:
                    parts_dir = archive_path.parent / f"{archive_path.stem}_parts"
                    if parts_dir.exists():
                        shutil.rmtree(parts_dir, ignore_errors=True)

                output_size = archive_path.stat().st_size
                output_parts: list[Path] = [archive_path]
                sent_ids: list[int] = []
                total_upload_bytes_parts = max(1, output_size)
                uploaded_bytes_parts = 0

                # 1) Intentar envío directo (sin dividir) si cabe en el límite configurado.
                should_fallback_to_split = output_size > max(1, int(self.cfg.single_upload_max_bytes))
                if not should_fallback_to_split:
                    await upsert_progress(
                        update,
                        context,
                        self.cfg,
                        "Subiendo resultado",
                        0,
                        total_upload_bytes_parts,
                        f"1/1 parte | {format_bytes(0)} de {format_bytes(output_size)}",
                        force=True,
                        cancellable=True,
                        percent_override=0.0,
                    )

                    action_id_for_single = uuid.uuid4().hex[:10]

                    single_timeout = min(self.cfg.telegram_operation_timeout, 180.0)

                    async def _send_one() -> Any:
                        with archive_path.open("rb") as archive_file:
                            return await asyncio.wait_for(
                                context.bot.send_document(
                                    chat.id,
                                    document=archive_file,
                                    filename=archive_path.name,
                                    caption=f"Compresion completada: {len(files_to_compress)} archivo(s) procesados.",
                                    reply_markup=compressed_result_keyboard(action_id_for_single),
                                ),
                                timeout=single_timeout,
                            )

                    try:
                        sent = await retry_async_with_cancel(
                            self.cfg,
                            f"Subida {archive_path.name}",
                            _send_one,
                            lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                            timeout_seconds=single_timeout,
                        )
                        sent_ids = [sent.message_id]
                        if sent.document:
                            self._register_result_action(
                                context,
                                file_id=sent.document.file_id,
                                file_name=sent.document.file_name or archive_path.name,
                                message_id=sent.message_id,
                                action_id=action_id_for_single,
                            )
                        uploaded_bytes_parts = output_size
                        await upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Subiendo resultado",
                            output_size,
                            total_upload_bytes_parts,
                            f"1/1 parte subida | {format_bytes(output_size)} de {format_bytes(output_size)}",
                            force=True,
                            cancellable=True,
                            percent_override=100.0,
                        )
                    except Exception as exc:
                        if _should_split_smaller(exc):
                            should_fallback_to_split = True
                        else:
                            raise

                # 2) Fallback: dividir solo si hace falta (timeout/red/file too big) o si excede el límite.
                if should_fallback_to_split:
                    recommended_part = min(2 * 1024 * 1024 * 1024, effective_part_size(self.cfg))
                    fallback_sizes = [
                        recommended_part,
                        1024 * 1024 * 1024,
                        768 * 1024 * 1024,
                        512 * 1024 * 1024,
                    ]
                    fallback_index = 0
                    while True:
                        _cleanup_parts_dir()
                        output_parts = await asyncio.to_thread(
                            split_file_for_upload,
                            archive_path,
                            self.cfg,
                            lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                            fallback_sizes[fallback_index],
                        )
                        total_upload_bytes_parts = max(1, sum(part.stat().st_size for part in output_parts))
                        uploaded_bytes_parts = 0

                        await upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Subiendo resultado",
                            0,
                            total_upload_bytes_parts,
                            f"0/{len(output_parts)} partes | Tamano final {format_bytes(output_size)}",
                            force=True,
                            cancellable=True,
                            percent_override=0.0,
                        )

                        sent_ids = []
                        sent_parts_meta: list[dict[str, Any]] = []
                        multipart_action_id = uuid.uuid4().hex[:10]
                        try:
                            for part_index, part in enumerate(output_parts, start=1):
                                self._raise_if_cancel_requested(context)
                                total_parts = max(len(output_parts), 1)
                                percent_parts_before = ((part_index - 1) / total_parts) * 100.0
                                await upsert_progress(
                                    update,
                                    context,
                                    self.cfg,
                                    "Subiendo resultado",
                                    min(uploaded_bytes_parts, total_upload_bytes_parts),
                                    total_upload_bytes_parts,
                                    (
                                        f"Subiendo parte {part_index}/{total_parts} | "
                                        f"Completadas {part_index - 1}/{total_parts} | "
                                        f"{format_bytes(min(uploaded_bytes_parts, output_size))} de {format_bytes(output_size)}"
                                    ),
                                    cancellable=True,
                                    percent_override=percent_parts_before,
                                )
                                caption = (
                                    f"Subiendo parte {part_index}/{total_parts}"
                                    f" | {format_bytes(part.stat().st_size)}"
                                    f" de {format_bytes(output_size)}"
                                )

                                reply_markup = (
                                    compressed_result_keyboard(multipart_action_id)
                                    if part_index == 1
                                    else favorite_keyboard()
                                )

                                part_timeout = self._recommended_upload_timeout(part.stat().st_size, self.cfg.telegram_operation_timeout)
                                part_timeout = min(part_timeout, max(60.0, min(self.cfg.telegram_operation_timeout, 180.0)))

                                async def _send_part() -> Any:
                                    with part.open("rb") as archive_file:
                                        return await asyncio.wait_for(
                                            context.bot.send_document(
                                                chat.id,
                                                document=archive_file,
                                                filename=part.name,
                                                caption=caption,
                                                reply_markup=reply_markup,
                                            ),
                                            timeout=part_timeout,
                                        )

                                sent = await retry_async_with_cancel(
                                    self.cfg,
                                    f"Subida parte {part.name}",
                                    _send_part,
                                    lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                                    timeout_seconds=part_timeout,
                                )
                                sent_ids.append(sent.message_id)
                                if getattr(sent, "document", None):
                                    sent_parts_meta.append(
                                        {
                                            "file_id": sent.document.file_id,
                                            "file_name": sent.document.file_name or part.name,
                                            "message_id": sent.message_id,
                                        }
                                    )
                                if part_index < total_parts:
                                    await asyncio.sleep(0.4)
                                uploaded_bytes_parts += max(0, part.stat().st_size)
                                percent_parts_after = (part_index / total_parts) * 100.0
                                await upsert_progress(
                                    update,
                                    context,
                                    self.cfg,
                                    "Subiendo resultado",
                                    min(uploaded_bytes_parts, total_upload_bytes_parts),
                                    total_upload_bytes_parts,
                                    (
                                        f"{part_index}/{total_parts} partes subidas | "
                                        f"{format_bytes(min(uploaded_bytes_parts, output_size))} de {format_bytes(output_size)}"
                                    ),
                                    force=True,
                                    cancellable=True,
                                    percent_override=percent_parts_after,
                                )
                            break
                        except Exception as exc:
                            if not sent_ids and fallback_index < len(fallback_sizes) - 1 and _should_split_smaller(exc):
                                fallback_index += 1
                                continue
                            raise

                    # Register a single action for the full (multipart) result.
                    if sent_parts_meta:
                        self._register_result_action(
                            context,
                            file_id=str(sent_parts_meta[0].get("file_id")),
                            file_name=archive_path.name,
                            message_id=int(sent_parts_meta[0].get("message_id")),
                            action_id=multipart_action_id,
                            parts=sent_parts_meta,
                        )

                for msg_id in sent_ids:
                    remember_message(update, context, msg_id)

                await upsert_progress(update, context, self.cfg, "Proceso completado", 1, 1, "Compresion y subida finalizadas correctamente.", force=True)
                compression_summary_text = (
                    "Resumen de Compresion\n"
                    f"Archivos recibidos: {len(files_to_compress)}\n"
                    f"Archivos descargados: {downloaded_count}\n"
                    f"Partes subidas: {len(output_parts)}\n"
                    f"Tamano final: {format_bytes(output_size)}"
                )
            except RuntimeError as exc:
                if "cancelada" in str(exc).lower():
                    await upsert_progress(update, context, self.cfg, "Cancelando", 1, 1, "La operación fue cancelada por el usuario.", force=True)
                    await tracked_reply(update, context, "Compresión cancelada por el usuario.", reply_markup=start_keyboard())
                    context.user_data[KEY_FILES_TO_COMPRESS] = []
                    context.user_data[KEY_MODE] = MODE_IDLE
                    context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = None
                    context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = ""
                    return
                await upsert_progress(update, context, self.cfg, "Error en compresion", 1, 1, f"Fallo: {exc}", force=True)
                await tracked_reply(update, context, f"No se pudo completar la compresion. Detalle tecnico: {exc}", reply_markup=start_keyboard())
                context.user_data[KEY_FILES_TO_COMPRESS] = []
                context.user_data[KEY_MODE] = MODE_IDLE
                context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = None
                context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = ""
                return
            except Exception as exc:
                await upsert_progress(update, context, self.cfg, "Error en compresion", 1, 1, f"Fallo: {exc}", force=True)
                await tracked_reply(update, context, f"No se pudo completar la compresion. Detalle tecnico: {exc}", reply_markup=start_keyboard())
                context.user_data[KEY_FILES_TO_COMPRESS] = []
                context.user_data[KEY_MODE] = MODE_IDLE
                context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = None
                context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = ""
                return
            finally:
                await self._stop_progress_heartbeat(heartbeat_task)
                try:
                    # Espera un instante para que el usuario vea el estado completado al 100%
                    await asyncio.sleep(1.0)
                    await self._cleanup_progress_message(update, context, force=True, delete_message=True)
                except Exception:
                    pass
                # Enviar resumen siempre, sea cual sea el estado previo
                if compression_summary_text:
                    try:
                        await tracked_reply(update, context, compression_summary_text, force_new_message=True)
                    except Exception:
                        logger.exception("Failed to send compression summary")
                cleanup_user_workspace(user.id, recreate=False)
                heavy_gate.release()

        context.user_data[KEY_FILES_TO_COMPRESS] = []
        context.user_data[KEY_MODE] = MODE_IDLE
        context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = None
        context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = ""

    async def process_extract_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE, password: str | None = None) -> bool:
        should_cleanup_progress = False
        extraction_finish_text: str | None = None
        if not await self.ensure_authorized(update, context):
            return False
        user = update.effective_user
        chat = update.effective_chat
        if not user or not chat:
            return True

        queue: list[dict[str, Any]] = context.user_data[KEY_ARCHIVES_TO_EXTRACT]
        queue.sort(key=lambda it: (int(it.get("file_size", 0)), str(it.get("file_name", ""))))
        extracted_files: list[str] = context.user_data[KEY_EXTRACTED_FILES]
        extracted_archives = int(context.user_data.get(KEY_EXTRACT_ARCHIVES_DONE, 0))
        initial_queue_total = int(context.user_data.get(KEY_EXTRACT_TOTAL_RECEIVED, 0))
        if initial_queue_total < extracted_archives + len(queue):
            initial_queue_total = extracted_archives + len(queue)
            context.user_data[KEY_EXTRACT_TOTAL_RECEIVED] = initial_queue_total
        total_download_bytes = sum(int(item.get("file_size", 0)) for item in queue)
        if "extract_bytes_done" not in context.user_data:
            context.user_data["extract_bytes_done"] = 0
        downloaded_archives = 0
        downloaded_bytes = 0
        downloaded_locals: list[Path] = []
        failed_download_items: list[dict[str, Any]] = []
        inflight_extract_download_bytes: dict[str, int] = {}
        progress_lock = asyncio.Lock()
        download_sem = asyncio.Semaphore(max(1, min(self.cfg.max_download_concurrency, 6)))
        self._reset_cancel_request(context)
        heavy_gate = get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs)
        heartbeat_task = self._start_progress_heartbeat(update, context)

        await upsert_progress(
            update,
            context,
            self.cfg,
            "Descargando comprimidos",
            0,
            max(total_download_bytes, 1),
            f"0 B de {format_bytes(max(total_download_bytes, 1))}",
            force=True,
            cancellable=True,
        )

        workspace = user_workspace(user.id)
        inbox = workspace / "extract_input"
        output_root = workspace / "extract_output"
        inbox.mkdir(parents=True, exist_ok=True)
        output_root.mkdir(parents=True, exist_ok=True)

        async def _download_extract_item(item: dict[str, Any]) -> None:
            nonlocal downloaded_archives, downloaded_bytes
            self._raise_if_cancel_requested(context)
            async with download_sem:
                self._raise_if_cancel_requested(context)
                archive_name = item["file_name"]
                local_archive = unique_target_path(inbox, archive_name)
                try:
                    item_key = str(item["file_id"])
                    async with progress_lock:
                        inflight_extract_download_bytes[item_key] = 0

                    async def _on_partial(size_now: int) -> None:
                        async with progress_lock:
                            # No clamping: en archivos grandes el file_size del update puede ser inexacto.
                            inflight_extract_download_bytes[item_key] = max(0, int(size_now))
                            partial_sum = sum(inflight_extract_download_bytes.values())
                            current_total = min(
                                max(total_download_bytes, 1),
                                max(0, downloaded_bytes + partial_sum),
                            )
                            progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                            progress_state["current"] = current_total
                            progress_state["total"] = max(total_download_bytes, current_total, 1)
                            progress_state["details"] = (
                                f"{downloaded_archives}/{initial_queue_total} descargados | "
                                f"{format_bytes(current_total)} de {format_bytes(max(total_download_bytes, 1))}"
                            )
                            progress_state["cancellable"] = True
                            context.user_data[KEY_PROGRESS] = progress_state

                    await self._download_file_robust(
                        context,
                        str(item["file_id"]),
                        local_archive,
                        f"Descarga {item['file_name']}",
                        on_progress=_on_partial,
                    )
                    validate_file_ready(local_archive)
                    _ = await self._maybe_compute_checksum(local_archive)
                    async with progress_lock:
                        inflight_extract_download_bytes.pop(item_key, None)
                        downloaded_locals.append(local_archive)
                        downloaded_archives += 1
                        downloaded_bytes += local_archive.stat().st_size
                        await upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Descargando comprimidos",
                            downloaded_bytes,
                            max(total_download_bytes, downloaded_bytes, 1),
                            (
                                f"{downloaded_archives}/{initial_queue_total} descargados | "
                                f"{format_bytes(downloaded_bytes)} de {format_bytes(max(total_download_bytes, 1))}"
                            ),
                            cancellable=True,
                        )
                except RuntimeError as exc:
                    async with progress_lock:
                        inflight_extract_download_bytes.pop(str(item["file_id"]), None)
                    if "cancelada" in str(exc).lower():
                        raise
                    if self._is_fatal_download_config_error(exc):
                        raise
                    async with progress_lock:
                        failed_download_items.append(item)
                except Exception:
                    async with progress_lock:
                        inflight_extract_download_bytes.pop(str(item["file_id"]), None)
                    async with progress_lock:
                        failed_download_items.append(item)

        try:
            async with heavy_gate:
                extract_download_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
                for item in list(queue):
                    extract_download_queue.put_nowait(item)

                worker_count = max(1, min(extract_download_queue.qsize(), self.cfg.max_download_concurrency))

                async def _extract_download_worker() -> None:
                    while True:
                        try:
                            item = extract_download_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            return
                        try:
                            await _download_extract_item(item)
                        finally:
                            extract_download_queue.task_done()

                await asyncio.gather(*(_extract_download_worker() for _ in range(worker_count)))
            self._raise_if_cancel_requested(context)
            queue.clear()

            if failed_download_items:
                unresolved: list[str] = []
                for item in failed_download_items:
                    self._raise_if_cancel_requested(context)
                    local_archive = unique_target_path(inbox, item["file_name"])
                    try:
                        await self._download_file_robust(
                            context,
                            str(item["file_id"]),
                            local_archive,
                            f"Reintento descarga {item['file_name']}",
                        )
                        validate_file_ready(local_archive)
                        _ = await self._maybe_compute_checksum(local_archive)
                        downloaded_locals.append(local_archive)
                        downloaded_archives += 1
                        downloaded_bytes += local_archive.stat().st_size
                        await upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Descargando comprimidos",
                            downloaded_bytes,
                            max(total_download_bytes, downloaded_bytes, 1),
                            (
                                f"{downloaded_archives}/{initial_queue_total} descargados | "
                                f"{format_bytes(downloaded_bytes)} de {format_bytes(max(total_download_bytes, 1))}"
                            ),
                            cancellable=True,
                        )
                    except RuntimeError as exc:
                        if "cancelada" in str(exc).lower():
                            raise
                        if self._is_fatal_download_config_error(exc):
                            raise
                        unresolved.append(str(item.get("file_name", "archivo")))
                    except Exception:
                        unresolved.append(str(item.get("file_name", "archivo")))

                if unresolved:
                    preview = ", ".join(unresolved[:8])
                    extra = "" if len(unresolved) <= 8 else f" y {len(unresolved) - 8} mas"
                    raise RuntimeError(
                        f"No se pudo descargar la cola completa para extraer. Pendientes: {preview}{extra}"
                    )

            targets, target_warnings = self._split_extract_targets(downloaded_locals)
            if target_warnings:
                preview = "\n".join(f"- {warn}" for warn in target_warnings[:5])
                more = "" if len(target_warnings) <= 5 else f"\n- ... y {len(target_warnings) - 5} aviso(s) mas"
                await tracked_reply(update, context, f"Avisos de partes incompletas detectadas:\n{preview}{more}", reply_markup=extract_keyboard())

            if targets:
                try:
                    completed = await self._extract_local_targets(update, context, targets, initial_queue_total, password)
                    if not completed:
                        context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
                        should_cleanup_progress = False
                        extraction_finish_text = "Extraccion pausada: se requiere password para continuar."
                        return False
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(f"Extraction failed: {exc}")
                    should_cleanup_progress = True
                    extraction_finish_text = f"Error en extraccion: {exc}"
                    raise

            context.user_data[KEY_MODE] = MODE_IDLE

            if not extracted_files and targets:
                fallback_files = list(collect_extracted_files(output_root))
                if fallback_files:
                    extracted_files.extend(str(path) for path in fallback_files)

            if not extracted_files and targets:
                cleanup_user_workspace(user.id, recreate=False)
                context.user_data[KEY_EXTRACT_TOTAL_RECEIVED] = 0
                context.user_data[KEY_EXTRACT_ARCHIVES_DONE] = 0
                await upsert_progress(update, context, self.cfg, "Proceso completado", 1, 1, "No se encontraron archivos extraibles.", force=True)
                extraction_finish_text = "Proceso finalizado: no se detectaron archivos extraibles en los comprimidos enviados."
                should_cleanup_progress = True
                return True

            paths_to_send = [Path(file_str) for file_str in extracted_files if Path(file_str).exists()]
            part_size_for_send = max(1, min(effective_part_size(self.cfg), int(self.cfg.single_upload_max_bytes)))

            def _expected_parts_for_send(size_bytes: int) -> int:
                if size_bytes <= part_size_for_send:
                    return 1
                return max(1, math.ceil(size_bytes / part_size_for_send))

            total_upload_parts = sum(_expected_parts_for_send(path.stat().st_size) for path in paths_to_send)
            sent_parts = 0
            total_upload_bytes = sum(path.stat().st_size for path in paths_to_send)
            uploaded_bytes = 0
            uploaded_files_done = 0
            total_files_to_send = len(paths_to_send)
            skipped_files: list[str] = []

            # Circuit breaker: reintentar archivos pequenos (1 parte) unas veces.
            max_file_attempts = 3

            await upsert_progress(
                update,
                context,
                self.cfg,
                "Subiendo archivos extraidos",
                0,
                max(total_upload_bytes, 1),
                f"0/{max(total_upload_parts, 1)} partes subidas | Archivo 0/{max(total_files_to_send, 1)}",
                force=True,
                cancellable=True,
                percent_override=0.0,
            )

            upload_sem = asyncio.Semaphore(self.cfg.max_upload_concurrency)
            progress_lock = asyncio.Lock()

            async def upload_extracted_path(file_path: Path) -> None:
                nonlocal sent_parts, uploaded_bytes, uploaded_files_done
                self._raise_if_cancel_requested(context)
                async with upload_sem:
                    self._raise_if_cancel_requested(context)
                    validate_file_ready(file_path)

                    file_parts = _expected_parts_for_send(file_path.stat().st_size)
                    remaining_parts_after_this = max(total_upload_parts - (sent_parts + file_parts), 0)
                    if remaining_parts_after_this == 0 and sent_parts < total_upload_parts:
                        # Último envío: evita quedarse clavado en 95% (p.ej. 19/20) mientras sube.
                        async with progress_lock:
                            progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                            progress_state["stage"] = "Subiendo archivos extraidos"
                            progress_state["current"] = min(uploaded_bytes, max(total_upload_bytes, 1))
                            progress_state["total"] = max(total_upload_bytes, 1)
                            progress_state["details"] = f"Subiendo último archivo: {file_path.name}"
                            progress_state["cancellable"] = True
                            progress_state["percent_override"] = min(
                                99.0,
                                (sent_parts / max(total_upload_parts, 1)) * 100.0 + 4.0,
                            )
                            context.user_data[KEY_PROGRESS] = progress_state

                    async def _on_part_sent(part_index: int, total_parts: int, message_id: int) -> None:
                        nonlocal sent_parts
                        async with progress_lock:
                            remember_message(update, context, int(message_id))
                            sent_parts += 1
                            percent_parts = (sent_parts / max(total_upload_parts, 1)) * 100.0
                            progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                            progress_state["stage"] = "Subiendo archivos extraidos"
                            progress_state["current"] = min(uploaded_bytes, max(total_upload_bytes, 1))
                            progress_state["total"] = max(total_upload_bytes, 1)
                            progress_state["details"] = (
                                f"{sent_parts}/{max(total_upload_parts, 1)} partes subidas | "
                                f"{file_path.name} ({part_index}/{total_parts})"
                            )
                            progress_state["cancellable"] = True
                            progress_state["percent_override"] = percent_parts
                            context.user_data[KEY_PROGRESS] = progress_state

                    sent_ids = await send_path_with_size_control(
                        context,
                        self.cfg,
                        chat.id,
                        file_path,
                        reply_markup=favorite_keyboard(),
                        caption_footer=f"<code>{file_path.name}</code>",
                        cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                        part_size_override=part_size_for_send,
                        timeout_seconds=self.cfg.telegram_operation_timeout,
                        on_part_sent=_on_part_sent,
                    )
                    async with progress_lock:
                        uploaded_bytes += max(0, file_path.stat().st_size)
                        uploaded_files_done += 1
                        progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                        progress_state["stage"] = "Subiendo archivos extraidos"
                        progress_state["current"] = min(uploaded_bytes, max(total_upload_bytes, 1))
                        progress_state["total"] = max(total_upload_bytes, 1)
                        progress_state["details"] = f"{sent_parts}/{max(total_upload_parts, 1)} partes subidas | Archivo {uploaded_files_done}/{max(total_files_to_send, 1)}"
                        progress_state["cancellable"] = True
                        progress_state["percent_override"] = (sent_parts / max(total_upload_parts, 1)) * 100.0
                        context.user_data[KEY_PROGRESS] = progress_state

            upload_queue: asyncio.Queue[tuple[Path, int]] = asyncio.Queue()
            for file_path in paths_to_send:
                upload_queue.put_nowait((file_path, 1))

            upload_workers = max(1, min(upload_queue.qsize(), self.cfg.max_upload_concurrency))

            async def _upload_worker() -> None:
                while True:
                    try:
                        queued_path, attempt = upload_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await upload_extracted_path(queued_path)
                    except Exception as exc:
                        # Circuit breaker: si un archivo se atasca/falla, no bloquea toda la extracción.
                        try:
                            file_size = max(0, int(queued_path.stat().st_size))
                        except Exception:
                            file_size = 0
                        file_parts = _expected_parts_for_send(file_size)

                        can_retry_safely = file_parts <= 1
                        if can_retry_safely and attempt < max_file_attempts and not bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)):
                            backoff = min(20.0, 2.0 * float(attempt))
                            await asyncio.sleep(backoff)
                            upload_queue.put_nowait((queued_path, attempt + 1))
                        else:
                            async with progress_lock:
                                skipped_files.append(queued_path.name)
                                # Ajusta totales para que el % pueda llegar a 100%.
                                nonlocal total_upload_bytes, total_upload_parts, total_files_to_send
                                total_upload_bytes = max(0, int(total_upload_bytes) - max(0, file_size))
                                total_upload_parts = max(0, int(total_upload_parts) - max(1, int(file_parts)))
                                total_files_to_send = max(0, int(total_files_to_send) - 1)

                                # Normaliza contadores.
                                sent_parts = min(sent_parts, max(total_upload_parts, 0))
                                uploaded_bytes = min(uploaded_bytes, max(total_upload_bytes, uploaded_bytes, 0))

                                progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                                progress_state["stage"] = "Subiendo archivos extraidos"
                                progress_state["current"] = min(uploaded_bytes, max(total_upload_bytes, 1))
                                progress_state["total"] = max(total_upload_bytes, 1)
                                progress_state["details"] = (
                                    f"{sent_parts}/{max(total_upload_parts, 1)} partes subidas | "
                                    f"Omitidos: {len(skipped_files)} (último: {queued_path.name})"
                                )
                                progress_state["cancellable"] = True
                                progress_state["percent_override"] = (sent_parts / max(total_upload_parts, 1)) * 100.0
                                context.user_data[KEY_PROGRESS] = progress_state

                            logger.exception(f"Upload skipped for {queued_path.name}: {exc}")
                    finally:
                        upload_queue.task_done()

            await asyncio.gather(*(_upload_worker() for _ in range(upload_workers)))

            # Garantiza cierre visual al 100% en la etapa de subida antes del mensaje final.
            await upsert_progress(
                update,
                context,
                self.cfg,
                "Subiendo archivos extraidos",
                max(total_upload_bytes, 1),
                max(total_upload_bytes, 1),
                (
                    f"{sent_parts}/{max(total_upload_parts, 1)} partes subidas | "
                    f"{format_bytes(total_upload_bytes)} de {format_bytes(total_upload_bytes)}"
                ),
                force=True,
                cancellable=True,
                percent_override=100.0,
            )

            context.user_data[KEY_EXTRACTED_FILES] = []
            context.user_data[KEY_CURRENT_ARCHIVE] = None
            context.user_data[KEY_PENDING_LOCAL_TARGETS] = []
            context.user_data[KEY_KNOWN_PASSWORD] = None
            context.user_data[KEY_EXTRACT_QUEUE_MESSAGE_ID] = None
            context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = None
            context.user_data[KEY_EXTRACT_TOTAL_RECEIVED] = 0
            context.user_data[KEY_EXTRACT_ARCHIVES_DONE] = 0
            context.user_data["extract_bytes_done"] = 0
            cleanup_user_workspace(user.id, recreate=False)
            await upsert_progress(update, context, self.cfg, "Proceso completado", 1, 1, "Extraccion y subida finalizadas. Recursos temporales limpiados.", force=True)
            summary_text = (
                "Resumen de Extraccion\n"
                f"Comprimidos recibidos: {initial_queue_total}\n"
                f"Comprimidos extraidos: {extracted_archives}\n"
                f"Archivos enviados: {uploaded_files_done}\n"
                f"Archivos omitidos: {len(skipped_files)}\n"
                f"Partes subidas: {sent_parts}\n"
                f"Tamano total enviado: {format_bytes(total_upload_bytes)}"
            )
            extraction_finish_text = summary_text
            should_cleanup_progress = True
            return True
        finally:
            await self._stop_progress_heartbeat(heartbeat_task)
            try:
                if should_cleanup_progress:
                    # Deja visible el estado completado un instante para evitar percepcion de salto/99%.
                    await asyncio.sleep(1.0)
                    await self._cleanup_progress_message(update, context, force=True, delete_message=True)
            except Exception:
                pass
            # Siempre enviar el resumen si existe, sea cual sea el estado de limpieza
            if extraction_finish_text:
                try:
                    await tracked_reply(update, context, extraction_finish_text, force_new_message=True)
                except Exception:
                    logger.exception("Failed to send extraction summary")

    async def finalize_extraction(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.ensure_authorized(update, context):
            return
        user = update.effective_user
        if not user:
            return

        user_lock = get_user_lock(user.id)
        if user_lock.locked():
            await tracked_reply(update, context, "Ya existe una operacion activa para este usuario. Espera a que finalice.", reply_markup=extract_keyboard())
            return

        queue = context.user_data[KEY_ARCHIVES_TO_EXTRACT]
        if not queue:
            await tracked_reply(update, context, "No hay comprimidos en cola para extraer. Envia al menos uno.", reply_markup=extract_keyboard())
            return

        async with user_lock:
            try:
                completed = await self.process_extract_queue(update, context)
                if completed:
                    await self._cleanup_progress_message(update, context, force=True, delete_message=True)
            except RuntimeError as exc:
                if "cancelada" in str(exc).lower():
                    context.user_data[KEY_MODE] = MODE_IDLE
                    context.user_data[KEY_ARCHIVES_TO_EXTRACT] = []
                    context.user_data[KEY_PENDING_LOCAL_TARGETS] = []
                    context.user_data[KEY_LAST_EXTRACT_QUEUE_TEXT] = ""
                    await self._cleanup_progress_message(update, context, force=True, delete_message=True)
                    await tracked_reply(update, context, "Extraccion cancelada por el usuario.", reply_markup=start_keyboard())
                    return
                context.user_data[KEY_MODE] = MODE_IDLE
                await self._cleanup_progress_message(update, context, force=True, delete_message=True)
                await tracked_reply(update, context, f"No se pudo completar la extraccion. Detalle tecnico: {exc}", reply_markup=start_keyboard())
                return
            except Exception as exc:
                context.user_data[KEY_MODE] = MODE_IDLE
                await self._cleanup_progress_message(update, context, force=True, delete_message=True)
                await tracked_reply(update, context, f"No se pudo completar la extraccion. Detalle tecnico: {exc}", reply_markup=start_keyboard())
                return

    async def handle_password_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.ensure_authorized(update, context):
            return
        if not update.message:
            return

        user = update.effective_user
        if not user:
            return

        user_lock = get_user_lock(user.id)
        if user_lock.locked():
            await tracked_reply(update, context, "Ya existe una operacion activa para este usuario. Espera a que finalice.", reply_markup=extract_keyboard())
            return

        async with user_lock:
            await self._handle_password_input_locked(update, context)

    async def _handle_password_input_locked(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return

        password = (update.message.text or "").strip()
        chat = update.effective_chat
        if chat:
            await self._safe_delete_message(context, chat.id, update.message.message_id)
        await self._cleanup_password_prompt_message(update, context)
        current = context.user_data.get(KEY_CURRENT_ARCHIVE)
        if not current:
            context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES
            await tracked_reply(update, context, "No hay archivo pendiente de password. Envia otro comprimido o finaliza.", reply_markup=extract_keyboard())
            return

        archive_path = Path(current["local_archive"])
        output_dir = Path(current["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)

        archive_size_hint = 0
        try:
            archive_size_hint = int(archive_path.stat().st_size)
        except Exception:
            archive_size_hint = 0
        archive_size_hint = max(1, archive_size_hint)

        await upsert_progress(
            update,
            context,
            self.cfg,
            "Descomprimiendo archivos",
            0,
            archive_size_hint,
            "Procesando 1/1",
            force=True,
            cancellable=True,
        )
        heartbeat_task = self._start_progress_heartbeat(update, context)

        def _compute_dir_size_bytes(root: Path) -> int:
            total = 0
            try:
                stack = [str(root)]
                while stack:
                    path = stack.pop()
                    try:
                        with os.scandir(path) as it:
                            for entry in it:
                                try:
                                    if entry.is_dir(follow_symlinks=False):
                                        stack.append(entry.path)
                                    elif entry.is_file(follow_symlinks=False):
                                        total += int(entry.stat(follow_symlinks=False).st_size)
                                except Exception:
                                    continue
                    except FileNotFoundError:
                        continue
                    except Exception:
                        continue
            except Exception:
                return 0
            return max(0, int(total))

        async def _poll_extract_progress() -> None:
            last_size = -1
            while True:
                if bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)):
                    return
                try:
                    current_size = await asyncio.to_thread(_compute_dir_size_bytes, output_dir)
                except Exception:
                    current_size = 0
                current_size = max(0, int(current_size))
                if current_size != last_size:
                    total_hint = max(archive_size_hint, current_size, 1)
                    progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                    progress_state["stage"] = "Descomprimiendo archivos"
                    progress_state["current"] = min(current_size, total_hint)
                    progress_state["total"] = total_hint
                    progress_state["details"] = "Procesando 1/1"
                    progress_state["cancellable"] = True
                    context.user_data[KEY_PROGRESS] = progress_state
                    last_size = current_size
                await asyncio.sleep(0.75)

        try:
            async with get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs):
                poll_task = asyncio.create_task(_poll_extract_progress())
                try:
                    await run_extract_in_thread(
                        archive_path,
                        output_dir,
                        password=password,
                        cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                    )
                finally:
                    poll_task.cancel()
                    try:
                        await poll_task
                    except asyncio.CancelledError:
                        pass
        except InvalidPasswordError:
            context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
            await self._upsert_password_prompt(update, context, "Password incorrecta. Intenta nuevamente.")
            return
        except PasswordRequiredError:
            context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
            await self._upsert_password_prompt(update, context, "El archivo sigue solicitando password. Intenta de nuevo.")
            return
        except RuntimeError as exc:
            text = str(exc).lower()
            if "cancelada" in text:
                context.user_data[KEY_MODE] = MODE_IDLE
                context.user_data[KEY_CURRENT_ARCHIVE] = None
                await tracked_reply(update, context, "Extracción cancelada por el usuario.", reply_markup=start_keyboard())
                return
            if (
                "failed the read enough data" in text
                or "wrong password" in text
                or "bad password" in text
                or "bad decrypt" in text
                or "password" in text
            ):
                context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
                await self._upsert_password_prompt(update, context, "Password incorrecta o archivo cifrado inconsistente. Intenta nuevamente.")
                return
            raise
        except Exception as exc:
            context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES
            context.user_data[KEY_CURRENT_ARCHIVE] = None
            await tracked_reply(update, context, f"Error al extraer usando password. Detalle: {exc}", reply_markup=extract_keyboard())
            return
        finally:
            await self._stop_progress_heartbeat(heartbeat_task)

        extracted_payloads = list(collect_extracted_files(output_dir))
        extracted_files: list[str] = context.user_data[KEY_EXTRACTED_FILES]
        extracted_files.extend(str(path) for path in extracted_payloads)
        context.user_data[KEY_EXTRACT_ARCHIVES_DONE] = int(context.user_data.get(KEY_EXTRACT_ARCHIVES_DONE, 0)) + 1
        context.user_data[KEY_CURRENT_ARCHIVE] = None
        pending_local_targets = [Path(path_str) for path_str in context.user_data.get(KEY_PENDING_LOCAL_TARGETS, []) if Path(path_str).exists()]
        context.user_data[KEY_PENDING_LOCAL_TARGETS] = []
        context.user_data[KEY_KNOWN_PASSWORD] = password
        context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES

        if pending_local_targets:
            initial_total = int(context.user_data.get(KEY_EXTRACT_TOTAL_RECEIVED, 0))
            completed = await self._extract_local_targets(update, context, pending_local_targets, initial_total, password)
            if not completed:
                return

        try:
            await self.process_extract_queue(update, context, password=password)
        except RuntimeError as exc:
            if "cancelada" in str(exc).lower():
                context.user_data[KEY_MODE] = MODE_IDLE
                context.user_data[KEY_ARCHIVES_TO_EXTRACT] = []
                context.user_data[KEY_PENDING_LOCAL_TARGETS] = []
                await self._cleanup_progress_message(update, context, delete_message=True)
                await tracked_reply(update, context, "Extraccion cancelada por el usuario.", reply_markup=start_keyboard())
                return
            context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES
            await self._cleanup_progress_message(update, context, delete_message=True)
            await tracked_reply(update, context, f"No se pudo completar la extraccion. Detalle tecnico: {exc}", reply_markup=extract_keyboard())
            return

    async def _begin_result_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE, action: str, action_id: str) -> None:
        ensure_defaults(context)
        result_actions: dict[str, dict[str, Any]] = context.user_data.get(KEY_RESULT_ACTIONS, {})
        data = result_actions.get(action_id)
        if not data:
            await tracked_reply(update, context, "Este archivo ya no esta disponible para esa accion.", reply_markup=start_keyboard())
            return

        context.user_data[KEY_PENDING_ACTION] = action
        context.user_data[KEY_PENDING_ACTION_ID] = action_id
        context.user_data[KEY_PENDING_FILE_ID] = data.get("file_id")
        context.user_data[KEY_PENDING_FILE_NAME] = data.get("file_name")
        context.user_data[KEY_PENDING_SOURCE_MESSAGE_ID] = data.get("message_id")

        if action == "encrypt":
            context.user_data[KEY_MODE] = MODE_WAIT_ENCRYPT_PASSWORD
            await self._save_action_prompt(
                update,
                context,
                "Escribe la password para generar un 7z protegido (se solicitara una sola vez al extraer).",
            )
            return

        context.user_data[KEY_MODE] = MODE_WAIT_RENAME
        await self._save_action_prompt(update, context, "Escribe el nuevo nombre del archivo (con o sin extension).")

    async def _delete_previous_result_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if not chat:
            return
        source_message_id = context.user_data.get(KEY_PENDING_SOURCE_MESSAGE_ID)
        if source_message_id:
            await self._safe_delete_message(context, chat.id, int(source_message_id))

    async def _download_pending_file(self, context: ContextTypes.DEFAULT_TYPE, target_path: Path) -> None:
        file_id = context.user_data.get(KEY_PENDING_FILE_ID)
        if not file_id:
            raise RuntimeError("No hay archivo pendiente para procesar")

        await self._download_file_robust(
            context,
            str(file_id),
            target_path,
            f"Descarga {target_path.name}",
        )
        validate_file_ready(target_path)

    @staticmethod
    def _part_sort_key(path: Path) -> tuple[int, str]:
        name = path.name
        match = re.search(r"\.part(\d{3,})$", name, flags=re.IGNORECASE)
        if match:
            return (int(match.group(1)), name.lower())
        return (10**9, name.lower())

    def _get_pending_action_parts_meta(self, context: ContextTypes.DEFAULT_TYPE) -> tuple[str | None, list[dict[str, Any]] | None]:
        action_id = context.user_data.get(KEY_PENDING_ACTION_ID)
        if not action_id:
            return None, None
        result_actions: dict[str, dict[str, Any]] = context.user_data.get(KEY_RESULT_ACTIONS, {})
        payload = result_actions.get(str(action_id)) or {}
        parts = payload.get("parts")
        if isinstance(parts, list) and parts:
            return str(action_id), parts
        return str(action_id), None

    async def _download_pending_file_multipart_if_needed(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        target_dir: Path,
        preferred_name: str,
    ) -> Path:
        action_id, parts_meta = self._get_pending_action_parts_meta(context)
        if not parts_meta:
            file_id = str(context.user_data.get(KEY_PENDING_FILE_ID) or "")
            file_name = safe_name(str(context.user_data.get(KEY_PENDING_FILE_NAME) or preferred_name))
            if not file_id:
                raise RuntimeError("No hay archivo pendiente para procesar")

            target_path = unique_target_path(target_dir, file_name)

            # Size hint para mostrar peso real + ETA.
            try:
                tg_meta = await context.bot.get_file(
                    file_id,
                    read_timeout=self.cfg.telegram_read_timeout,
                    connect_timeout=self.cfg.telegram_connect_timeout,
                    pool_timeout=self.cfg.telegram_pool_timeout,
                )
                size_hint = int(getattr(tg_meta, "file_size", 0) or 0)
            except Exception:
                size_hint = 0
            size_hint = max(1, int(size_hint))

            async def _on_partial(size_now: int) -> None:
                current_size = max(0, int(size_now))
                progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                progress_state["stage"] = "Descargando archivo"
                progress_state["current"] = current_size
                progress_state["total"] = max(size_hint, current_size, 1)
                progress_state["details"] = f"{format_bytes(min(current_size, size_hint))} de {format_bytes(size_hint)}"
                progress_state["cancellable"] = True
                context.user_data[KEY_PROGRESS] = progress_state

            await self._download_file_robust(
                context,
                file_id,
                target_path,
                f"Descarga {target_path.name}",
                on_progress=_on_partial,
            )
            validate_file_ready(target_path)
            return target_path

        parts_dir = target_dir / f"__parts_{action_id}"
        parts_dir.mkdir(parents=True, exist_ok=True)
        # Prefetch de sizes para ETA real.
        parts_info: list[dict[str, Any]] = []
        for idx, meta in enumerate(parts_meta, start=1):
            file_id = str(meta.get("file_id") or "")
            file_name = safe_name(str(meta.get("file_name") or f"part{idx:03d}"))
            if not file_id:
                raise RuntimeError("Partes incompletas: falta file_id")
            try:
                tg_meta = await context.bot.get_file(
                    file_id,
                    read_timeout=self.cfg.telegram_read_timeout,
                    connect_timeout=self.cfg.telegram_connect_timeout,
                    pool_timeout=self.cfg.telegram_pool_timeout,
                )
                size_hint = int(getattr(tg_meta, "file_size", 0) or 0)
            except Exception:
                size_hint = 0
            parts_info.append({"idx": idx, "file_id": file_id, "file_name": file_name, "size_hint": max(1, int(size_hint))})

        total_hint_all = max(1, int(sum(int(it["size_hint"]) for it in parts_info)))

        downloaded_parts: list[Path] = []
        inflight_sizes: dict[int, int] = {}
        downloaded_bytes = 0

        for info in parts_info:
            idx = int(info["idx"])
            file_id = str(info["file_id"])
            file_name = str(info["file_name"])
            size_hint = int(info["size_hint"])

            part_path = unique_target_path(parts_dir, file_name)

            async def _on_partial(size_now: int, part_idx: int = idx) -> None:
                inflight_sizes[part_idx] = max(0, int(size_now))
                current_total = max(0, int(downloaded_bytes + sum(inflight_sizes.values())))
                progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                progress_state["stage"] = "Descargando partes"
                progress_state["current"] = min(current_total, max(total_hint_all, current_total, 1))
                progress_state["total"] = max(total_hint_all, current_total, 1)
                progress_state["details"] = (
                    f"Parte {part_idx}/{len(parts_info)} | {format_bytes(min(current_total, total_hint_all))} de {format_bytes(total_hint_all)}"
                )
                progress_state["cancellable"] = True
                context.user_data[KEY_PROGRESS] = progress_state

            await self._download_file_robust(
                context,
                file_id,
                part_path,
                f"Descarga {file_name}",
                on_progress=_on_partial,
            )
            validate_file_ready(part_path)
            downloaded_parts.append(part_path)
            inflight_sizes.pop(idx, None)
            try:
                downloaded_bytes += max(0, int(part_path.stat().st_size))
            except Exception:
                pass

        downloaded_parts.sort(key=self._part_sort_key)
        joined_path = unique_target_path(target_dir, preferred_name)

        total_join_bytes = 0
        for p in downloaded_parts:
            try:
                total_join_bytes += max(0, int(p.stat().st_size))
            except Exception:
                pass
        total_join_bytes = max(1, int(total_join_bytes))

        def _join() -> None:
            joined_bytes = 0
            with joined_path.open("wb") as out:
                for part_path in downloaded_parts:
                    with part_path.open("rb") as src:
                        while True:
                            if bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)):
                                raise RuntimeError("Operacion cancelada por el usuario")
                            chunk = src.read(8 * 1024 * 1024)
                            if not chunk:
                                break
                            out.write(chunk)
                            joined_bytes += len(chunk)
                            progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                            progress_state["stage"] = "Uniendo partes"
                            progress_state["current"] = min(joined_bytes, total_join_bytes)
                            progress_state["total"] = max(total_join_bytes, joined_bytes, 1)
                            progress_state["details"] = f"{format_bytes(min(joined_bytes, total_join_bytes))} de {format_bytes(total_join_bytes)}"
                            progress_state["cancellable"] = True
                            context.user_data[KEY_PROGRESS] = progress_state

        await asyncio.to_thread(_join)
        validate_file_ready(joined_path)

        for part_path in downloaded_parts:
            try:
                part_path.unlink(missing_ok=True)
            except Exception:
                pass
        try:
            shutil.rmtree(parts_dir, ignore_errors=True)
        except Exception:
            pass
        return joined_path

    def _get_7z_executable(self) -> str:
        exe = shutil.which("7z") or shutil.which("7z.exe") or shutil.which("7za") or shutil.which("7za.exe")
        if exe:
            return exe

        # Fallbacks comunes en Windows cuando PATH de la sesion no se actualizo.
        candidates = [
            Path(r"C:\Program Files\7-Zip\7z.exe"),
            Path(r"C:\Program Files (x86)\7-Zip\7z.exe"),
            Path(r"C:\Program Files\7-Zip\7za.exe"),
            Path(r"C:\Program Files (x86)\7-Zip\7za.exe"),
        ]
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)

        raise RuntimeError("No se encontro 7z. Instala 7-Zip y verifica que 7z.exe exista en Program Files o en PATH.")

    def _encrypt_zip_as_7z(
        self,
        source_zip: Path,
        encrypted_7z: Path,
        password: str,
        cancel_requested: Callable[[], bool] | None = None,
        progress_percent: Callable[[float], None] | None = None,
    ) -> None:
        """Convierte un ZIP a 7z cifrado con password unica y header encryption."""
        exe = self._get_7z_executable()

        def _run_7z(cmd: list[str], cwd: Path, on_percent: Callable[[int], None] | None = None) -> tuple[int, str]:
            proc = subprocess.Popen(
                cmd,
                cwd=str(cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
            started = time.monotonic()
            hard_timeout = max(float(self.cfg.telegram_operation_timeout) * 6.0, 900.0)

            q: queue.Queue[str] = queue.Queue()
            tail: list[str] = []

            def _reader() -> None:
                try:
                    if not proc.stdout:
                        return
                    for line in proc.stdout:
                        q.put(line)
                        tail.append(line)
                        if len(tail) > 200:
                            tail.pop(0)
                except Exception:
                    return

            t = threading.Thread(target=_reader, daemon=True)
            t.start()

            last_percent = -1
            try:
                while proc.poll() is None:
                    if cancel_requested and cancel_requested():
                        proc.terminate()
                        try:
                            proc.wait(timeout=3.0)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=3.0)
                        raise RuntimeError("Operacion cancelada por el usuario")
                    if (time.monotonic() - started) > hard_timeout:
                        proc.terminate()
                        try:
                            proc.wait(timeout=3.0)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=3.0)
                        raise RuntimeError("Timeout al crear 7z cifrado")
                    try:
                        while True:
                            line = q.get_nowait()
                            match = re.search(r"(\d{1,3})%", str(line))
                            if match and on_percent:
                                pct = int(match.group(1))
                                if 0 <= pct <= 100 and pct != last_percent:
                                    last_percent = pct
                                    try:
                                        on_percent(pct)
                                    except Exception:
                                        pass
                    except queue.Empty:
                        pass
                    time.sleep(0.2)

                # Drain remaining output
                try:
                    while True:
                        line = q.get_nowait()
                        match = re.search(r"(\d{1,3})%", str(line))
                        if match and on_percent:
                            pct = int(match.group(1))
                            if 0 <= pct <= 100 and pct != last_percent:
                                last_percent = pct
                                try:
                                    on_percent(pct)
                                except Exception:
                                    pass
                except queue.Empty:
                    pass
            except Exception:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=3.0)
                raise
            try:
                t.join(timeout=1.0)
            except Exception:
                pass
            return proc.returncode, "".join(tail).strip()

        # 1) Estrategia preferida (robusta): encapsular el ZIP completo dentro del 7z cifrado.
        # Esto evita cuelgues y uso excesivo de disco/RAM al extraer/reempacar archivos grandes.
        wrap_cmd = [
            exe,
            "a",
            "-t7z",
            str(encrypted_7z),
            source_zip.name,
            f"-p{password}",
            "-mhe=on",
            "-y",
            "-bsp1",
        ]

        def _map_wrap_percent(pct: int) -> None:
            if progress_percent:
                try:
                    # 0..95% reservado para el proceso principal.
                    progress_percent((max(0, min(100, pct)) / 100.0) * 95.0)
                except Exception:
                    pass

        wrap_code, wrap_detail = _run_7z(wrap_cmd, source_zip.parent, on_percent=_map_wrap_percent)
        if wrap_code == 0:
            if progress_percent:
                try:
                    progress_percent(95.0)
                except Exception:
                    pass
            return

        # 2) Fallback (mas lento): extraer + reempacar. Solo se usa si el wrap falla.
        work_dir = encrypted_7z.parent / f"__enc_build_{uuid.uuid4().hex}"
        extracted_dir = work_dir / "input"
        work_dir.mkdir(parents=True, exist_ok=True)
        extracted_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Extraccion manual para poder reportar progreso real.
            with zipfile.ZipFile(source_zip, "r") as zf:
                infos = [info for info in zf.infolist() if not info.is_dir()]
                total_uncompressed = max(1, int(sum(max(0, int(i.file_size)) for i in infos)))
                extracted_bytes = 0

                extracted_root = extracted_dir.resolve()
                for info in zf.infolist():
                    if cancel_requested and cancel_requested():
                        raise RuntimeError("Operacion cancelada por el usuario")

                    rel_name = info.filename
                    if not rel_name:
                        continue
                    target_path = (extracted_dir / rel_name)
                    try:
                        target_resolved = target_path.resolve()
                        if not target_resolved.is_relative_to(extracted_root):
                            continue
                    except Exception:
                        # Si no se puede resolver, omitimos por seguridad.
                        continue

                    if info.is_dir():
                        target_path.mkdir(parents=True, exist_ok=True)
                        continue

                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(info, "r") as src, target_path.open("wb") as dst:
                        while True:
                            if cancel_requested and cancel_requested():
                                raise RuntimeError("Operacion cancelada por el usuario")
                            chunk = src.read(4 * 1024 * 1024)
                            if not chunk:
                                break
                            dst.write(chunk)
                            extracted_bytes += len(chunk)
                            if progress_percent:
                                try:
                                    # 0..30% reservado para extraccion.
                                    progress_percent(min(30.0, (extracted_bytes / total_uncompressed) * 30.0))
                                except Exception:
                                    pass

            extracted_files = [
                p.relative_to(extracted_dir).as_posix()
                for p in extracted_dir.rglob("*")
                if p.is_file()
            ]
            if not extracted_files:
                raise RuntimeError("El ZIP no contiene archivos para encryptar")

            list_file = work_dir / "__7z_filelist.txt"
            list_file.write_text("\n".join(extracted_files), encoding="utf-8")

            cmd = [
                exe,
                "a",
                "-t7z",
                str(encrypted_7z),
                f"@{list_file}",
                f"-p{password}",
                "-mhe=on",
                "-y",
                "-bsp1",
            ]

            def _map_7z_percent(pct: int) -> None:
                if progress_percent:
                    try:
                        # 30..95% reservado para el armado del 7z.
                        progress_percent(30.0 + (max(0, min(100, pct)) / 100.0) * 65.0)
                    except Exception:
                        pass

            code, detail = _run_7z(cmd, extracted_dir, on_percent=_map_7z_percent)
            if code != 0:
                # Si tambien falla, devolvemos el error del fallback (mas relevante aqui).
                raise RuntimeError(f"Fallo al crear 7z cifrado: {detail}")

            if progress_percent:
                try:
                    progress_percent(95.0)
                except Exception:
                    pass
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    def _assert_7z_is_encrypted(self, encrypted_7z: Path, password: str) -> None:
        """Valida que el 7z exige password y que con la password correcta es legible."""
        exe = self._get_7z_executable()
        check_timeout = max(20.0, min(float(self.cfg.telegram_operation_timeout), 300.0))

        # Sin password debe fallar por archivo cifrado/cabeceras cifradas.
        try:
            no_pwd = subprocess.run(
                [exe, "t", str(encrypted_7z), "-p-", "-y"],
                capture_output=True,
                text=True,
                timeout=check_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("Timeout verificando proteccion de password del 7z") from exc
        no_pwd_output = f"{no_pwd.stdout}\n{no_pwd.stderr}".lower()
        if no_pwd.returncode == 0:
            raise RuntimeError("El archivo 7z no esta protegido: se pudo leer sin password")
        if not any(token in no_pwd_output for token in ["wrong password", "can not open encrypted archive", "password"]):
            raise RuntimeError("No se pudo verificar proteccion por password en el 7z generado")

        # Con password correcta debe pasar.
        try:
            with_pwd = subprocess.run(
                [exe, "t", str(encrypted_7z), f"-p{password}", "-y"],
                capture_output=True,
                text=True,
                timeout=check_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("Timeout validando 7z con la password indicada") from exc
        if with_pwd.returncode != 0:
            detail = (with_pwd.stderr or with_pwd.stdout or "").strip()
            raise RuntimeError(f"No se pudo validar el 7z con la password indicada: {detail}")

    async def _send_result_with_actions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: Path, caption: str) -> None:
        chat = update.effective_chat
        if not chat:
            return

        if file_path.stat().st_size > max(1, int(self.cfg.single_upload_max_bytes)):
            # Multipart upload with stable UI progress (parts-based percent) to avoid
            # showing a frozen % during long part uploads.
            progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
            existing_stage = str(progress_state.get("stage") or "")
            stage_for_upload = existing_stage if "subiend" in existing_stage.lower() else "Subiendo resultado"

            part_size_override = self._recommended_upload_part_size(file_path.stat().st_size, effective_part_size(self.cfg))
            parts = await asyncio.to_thread(
                split_file_for_upload,
                file_path,
                self.cfg,
                lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                part_size_override,
            )
            total_parts = max(len(parts), 1)
            total_bytes = max(1, sum(p.stat().st_size for p in parts))
            sent_ids: list[int] = []
            sent_parts_meta: list[dict[str, Any]] = []
            sent_bytes = 0

            multipart_action_id = uuid.uuid4().hex[:10]

            await upsert_progress(
                update,
                context,
                self.cfg,
                stage_for_upload,
                0,
                total_bytes,
                f"0/{total_parts} partes | {format_bytes(0)} de {format_bytes(total_bytes)}",
                force=True,
                cancellable=True,
                percent_override=0.0,
            )

            for index, part in enumerate(parts, start=1):
                self._raise_if_cancel_requested(context)
                percent_before = ((index - 1) / total_parts) * 100.0
                part_timeout = self._recommended_upload_timeout(part.stat().st_size, self.cfg.telegram_operation_timeout)
                part_timeout = min(part_timeout, max(60.0, min(self.cfg.telegram_operation_timeout, 180.0)))
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    stage_for_upload,
                    min(sent_bytes, total_bytes),
                    total_bytes,
                    (
                        f"Subiendo parte {index}/{total_parts} | "
                        f"{format_bytes(min(sent_bytes, total_bytes))} de {format_bytes(total_bytes)}"
                    ),
                    cancellable=True,
                    percent_override=percent_before,
                )

                async def _send_part() -> Any:
                    with part.open("rb") as payload:
                        reply_markup = (
                            compressed_result_keyboard(multipart_action_id)
                            if index == 1
                            else favorite_keyboard()
                        )
                        return await asyncio.wait_for(
                            context.bot.send_document(
                                chat.id,
                                document=payload,
                                filename=part.name,
                                caption=(
                                    f"Archivo grande dividido: {file_path.name} (parte {index}/{total_parts})"
                                    if index == 1
                                    else None
                                ),
                                reply_markup=reply_markup,
                            ),
                            timeout=part_timeout,
                        )

                sent = await retry_async_with_cancel(
                    self.cfg,
                    f"Subida parte {part.name}",
                    _send_part,
                    lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                )
                sent_ids.append(sent.message_id)
                if getattr(sent, "document", None):
                    sent_parts_meta.append(
                        {
                            "file_id": sent.document.file_id,
                            "file_name": sent.document.file_name or part.name,
                            "message_id": sent.message_id,
                        }
                    )
                if index < total_parts:
                    await asyncio.sleep(0.4)
                sent_bytes += max(0, part.stat().st_size)

                percent_after = (index / total_parts) * 100.0
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    stage_for_upload,
                    min(sent_bytes, total_bytes),
                    total_bytes,
                    (
                        f"{index}/{total_parts} partes subidas | "
                        f"{format_bytes(min(sent_bytes, total_bytes))} de {format_bytes(total_bytes)}"
                    ),
                    force=True,
                    cancellable=True,
                    percent_override=percent_after,
                )

            for msg_id in sent_ids:
                remember_message(update, context, msg_id)

            if sent_parts_meta:
                self._register_result_action(
                    context,
                    file_id=str(sent_parts_meta[0].get("file_id")),
                    file_name=file_path.name,
                    message_id=int(sent_parts_meta[0].get("message_id")),
                    action_id=multipart_action_id,
                    parts=sent_parts_meta,
                )

            await tracked_reply(
                update,
                context,
                "Resultado enviado en varias partes. Puedes usar 'Anadir a favoritos' en cualquiera de las partes.",
                reply_markup=start_keyboard(),
            )
            return

        await upsert_progress(
            update,
            context,
            self.cfg,
            "Subiendo resultado",
            0,
            max(1, file_path.stat().st_size),
            "Subida directa (1/1). Puede tardar en conexiones lentas.",
            force=True,
            cancellable=True,
            percent_override=0.0,
        )

        async def _send_one() -> Any:
            action_id = uuid.uuid4().hex[:10]
            with file_path.open("rb") as payload:
                sent_local = await asyncio.wait_for(
                    context.bot.send_document(
                        chat.id,
                        document=payload,
                        filename=file_path.name,
                        caption=caption,
                        reply_markup=compressed_result_keyboard(action_id),
                    ),
                    timeout=self.cfg.telegram_operation_timeout,
                )
            return sent_local, action_id

        sent, action_id = await retry_async_with_cancel(
            self.cfg,
            f"Subida {file_path.name}",
            _send_one,
            lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
        )
        remember_message(update, context, sent.message_id)
        if sent.document:
            self._register_result_action(
                context,
                file_id=sent.document.file_id,
                file_name=sent.document.file_name or file_path.name,
                message_id=sent.message_id,
                action_id=action_id,
            )

    async def _send_to_favorites_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int) -> bool:
        chat = update.effective_chat
        if not chat:
            return False

        target = self.cfg.favorites_channel_id
        if not target:
            await tracked_reply(
                update,
                context,
                "No se ha configurado FAVORITES_CHANNEL_ID. Configuralo en .env para usar favoritos.",
                reply_markup=start_keyboard(),
            )
            return False

        try:
            await asyncio.wait_for(
                context.bot.copy_message(
                    chat_id=target,
                    from_chat_id=chat.id,
                    message_id=message_id,
                    reply_markup=favorite_channel_keyboard(),
                ),
                timeout=self.cfg.telegram_operation_timeout,
            )
            return True
        except Exception as exc:
            await tracked_reply(
                update,
                context,
                f"No se pudo enviar a favoritos. Detalle: {exc}",
                reply_markup=start_keyboard(),
            )
            return False

    async def handle_encrypt_password_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        user = update.effective_user
        if not user:
            return

        user_lock = get_user_lock(user.id)
        if user_lock.locked():
            await tracked_reply(update, context, "Ya existe una operacion activa para este usuario. Espera a que finalice.", reply_markup=start_keyboard())
            return

        async with user_lock:
            await self._handle_encrypt_password_input_locked(update, context)

    async def _handle_encrypt_password_input_locked(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        password = (update.message.text or "").strip()
        if not password:
            await tracked_reply(update, context, "La password no puede estar vacia. Escribe una valida para continuar.")
            return

        user = update.effective_user
        chat = update.effective_chat
        if not user or not chat:
            return

        await self._safe_delete_message(context, chat.id, update.message.message_id)
        await self._cleanup_action_prompt_message(update, context)
        await self._delete_previous_result_message(update, context)

        workspace = user_workspace(user.id)
        post_dir = workspace / "postprocess"
        shutil.rmtree(post_dir, ignore_errors=True)
        post_dir.mkdir(parents=True, exist_ok=True)

        source_name = str(context.user_data.get(KEY_PENDING_FILE_NAME) or "archivo.zip")
        preferred_source_name = re.sub(r"\.part\d{3,}$", "", source_name, flags=re.IGNORECASE)
        heartbeat_task: asyncio.Task[Any] | None = None
        heavy_gate = get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs)
        
        await heavy_gate.acquire()
        try:
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", 0, 1024, "Preparando descarga", force=True, cancellable=True)
            heartbeat_task = self._start_progress_heartbeat(update, context)
            source_path = await self._download_pending_file_multipart_if_needed(
                update,
                context,
                post_dir,
                preferred_source_name,
            )
            self._raise_if_cancel_requested(context)
            source_size = max(1, source_path.stat().st_size)
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", source_size, source_size, "Archivo descargado. Preparando cifrado 7z", force=True, cancellable=True)
            
            if source_path.suffix.lower() != ".zip":
                raise RuntimeError("Solo se permite encryptar archivos ZIP en este flujo")

            temp_encrypted = post_dir / f"__enc_{uuid.uuid4().hex}.7z"
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", 0, source_size, "Cifrando contenido", force=True, cancellable=True)

            loop = asyncio.get_running_loop()
            last_emit_ts = 0.0
            last_emit_pct = -1.0

            def _emit_encrypt_progress(pct: float) -> None:
                nonlocal last_emit_ts, last_emit_pct
                try:
                    pct_f = float(pct)
                except Exception:
                    return
                pct_f = max(0.0, min(100.0, pct_f))
                now_ts = time.monotonic()
                if (now_ts - last_emit_ts) < 0.6 and abs(pct_f - last_emit_pct) < 1.0:
                    return
                last_emit_ts = now_ts
                last_emit_pct = pct_f
                current = int((pct_f / 100.0) * float(source_size))
                current = max(0, min(int(source_size), current))
                try:
                    asyncio.run_coroutine_threadsafe(
                        upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Encryptando archivo",
                            current,
                            int(source_size),
                            f"Cifrando contenido ({pct_f:.0f}%)",
                            force=True,
                            cancellable=True,
                        ),
                        loop,
                    )
                except Exception:
                    return

            await asyncio.to_thread(
                self._encrypt_zip_as_7z,
                source_path,
                temp_encrypted,
                password,
                lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                _emit_encrypt_progress,
            )
            self._raise_if_cancel_requested(context)
            encrypted_size = max(1, temp_encrypted.stat().st_size if temp_encrypted.exists() else source_size)
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", encrypted_size, encrypted_size, "Cifrado completado. Validando archivo", force=True, cancellable=True)

            # Validar antes de renombrar
            await asyncio.to_thread(self._assert_7z_is_encrypted, temp_encrypted, password)
            self._raise_if_cancel_requested(context)
            
            # Limpiar archivo anterior
            try:
                source_path.unlink(missing_ok=True)
            except Exception:
                pass
            
            # Renombrar con nombre final
            encrypted_path = post_dir / safe_name(f"{source_path.stem}_encrypted.7z")
            temp_encrypted.rename(encrypted_path)

            validate_file_ready(encrypted_path)
            encrypted_size = max(1, encrypted_path.stat().st_size)
            await upsert_progress(update, context, self.cfg, "Subiendo archivo encryptado", 0, encrypted_size, "Iniciando subida", force=True, cancellable=True)

            await self._send_result_with_actions(
                update,
                context,
                encrypted_path,
                "Archivo protegido correctamente (7z con password unica).",
            )
            await upsert_progress(update, context, self.cfg, "Proceso completado", encrypted_size, encrypted_size, "Encrypting finalizado correctamente", force=True)
        except RuntimeError as exc:
            if "cancelada" in str(exc).lower():
                await upsert_progress(update, context, self.cfg, "Cancelando", 1, 1, "La operación fue cancelada por el usuario.", force=True)
                await tracked_reply(update, context, "Encriptado cancelado por el usuario.", reply_markup=start_keyboard())
                return
            await upsert_progress(update, context, self.cfg, "Error", 1, 1, f"Fallo: {exc}", force=True)
            await tracked_reply(update, context, f"No se pudo encryptar el archivo. Detalle: {exc}", reply_markup=start_keyboard())
            return
        except Exception as exc:
            await tracked_reply(update, context, f"No se pudo encryptar el archivo. Detalle: {exc}", reply_markup=start_keyboard())
        finally:
            await self._stop_progress_heartbeat(heartbeat_task)
            await self._cleanup_progress_message(update, context, force=True, delete_message=True)
            self._clear_pending_action(context)
            context.user_data[KEY_MODE] = MODE_IDLE
            cleanup_user_workspace(user.id, recreate=False)
            heavy_gate.release()

    async def handle_rename_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        user = update.effective_user
        if not user:
            return

        user_lock = get_user_lock(user.id)
        if user_lock.locked():
            await tracked_reply(update, context, "Ya existe una operacion activa para este usuario. Espera a que finalice.", reply_markup=start_keyboard())
            return

        async with user_lock:
            await self._handle_rename_input_locked(update, context)

    async def _handle_rename_input_locked(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        requested_name = safe_name((update.message.text or "").strip())
        if not requested_name:
            await tracked_reply(update, context, "Nombre invalido. Escribe un nombre valido para continuar.")
            return

        user = update.effective_user
        chat = update.effective_chat
        if not user or not chat:
            return

        await self._safe_delete_message(context, chat.id, update.message.message_id)
        await self._cleanup_action_prompt_message(update, context)
        await self._delete_previous_result_message(update, context)

        original_name = str(context.user_data.get(KEY_PENDING_FILE_NAME) or "archivo.zip")
        if Path(requested_name).suffix == "":
            requested_name = f"{requested_name}{Path(original_name).suffix}"

        workspace = user_workspace(user.id)
        post_dir = workspace / "postprocess"
        shutil.rmtree(post_dir, ignore_errors=True)
        post_dir.mkdir(parents=True, exist_ok=True)
        preferred_source_name = re.sub(r"\.part\d{3,}$", "", original_name, flags=re.IGNORECASE)
        heartbeat_task: asyncio.Task[Any] | None = None
        heavy_gate = get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs)

        await heavy_gate.acquire()
        try:
            await upsert_progress(update, context, self.cfg, "Renombrando archivo", 0, 1024, "Preparando descarga", force=True, cancellable=True)
            heartbeat_task = self._start_progress_heartbeat(update, context)
            source_path = await self._download_pending_file_multipart_if_needed(
                update,
                context,
                post_dir,
                preferred_source_name,
            )
            self._raise_if_cancel_requested(context)
            source_size = max(1, source_path.stat().st_size)
            await upsert_progress(update, context, self.cfg, "Renombrando archivo", source_size, source_size, "Archivo descargado. Aplicando nuevo nombre", force=True, cancellable=True)

            renamed_path = source_path.with_name(requested_name)
            source_path.rename(renamed_path)
            validate_file_ready(renamed_path)

            renamed_size = max(1, renamed_path.stat().st_size)
            await upsert_progress(update, context, self.cfg, "Subiendo archivo renombrado", 0, renamed_size, "Iniciando subida", force=True, cancellable=True)

            await self._send_result_with_actions(update, context, renamed_path, "Archivo renombrado correctamente.")
            await upsert_progress(update, context, self.cfg, "Proceso completado", renamed_size, renamed_size, "Renaming finalizado correctamente", force=True)
        except RuntimeError as exc:
            if "cancelada" in str(exc).lower():
                await upsert_progress(update, context, self.cfg, "Cancelando", 1, 1, "La operación fue cancelada por el usuario.", force=True)
                await tracked_reply(update, context, "Renombrado cancelado por el usuario.", reply_markup=start_keyboard())
                return
            await upsert_progress(update, context, self.cfg, "Error", 1, 1, f"Fallo: {exc}", force=True)
            await tracked_reply(update, context, f"No se pudo renombrar el archivo. Detalle: {exc}", reply_markup=start_keyboard())
            return
        except Exception as exc:
            await tracked_reply(update, context, f"No se pudo renombrar el archivo. Detalle: {exc}", reply_markup=start_keyboard())
        finally:
            await self._stop_progress_heartbeat(heartbeat_task)
            await self._cleanup_progress_message(update, context, force=True, delete_message=True)
            self._clear_pending_action(context)
            context.user_data[KEY_MODE] = MODE_IDLE
            cleanup_user_workspace(user.id, recreate=False)
            heavy_gate.release()

    async def text_router(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message or not update.message.text:
            return

        if not await self.ensure_authorized(update, context):
            return

        ensure_defaults(context)
        mode = context.user_data.get(KEY_MODE, MODE_IDLE)

        text = update.message.text.strip()

        if mode == MODE_WAIT_PASSWORD:
            await self.handle_password_input(update, context)
            return

        if mode == MODE_WAIT_ENCRYPT_PASSWORD:
            await self.handle_encrypt_password_input(update, context)
            return

        if mode == MODE_WAIT_RENAME:
            await self.handle_rename_input(update, context)
            return

        await tracked_reply(update, context, "Usa los botones del menu para continuar con una accion valida.", reply_markup=start_keyboard())

    async def callback_router(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return

        data = query.data or ""
        if data == CB_CANCEL_PROGRESS:
            await query.answer("Cancelando...", cache_time=0)
        else:
            await query.answer()

        if data == CB_FAVORITO_REMOVE:
            if query.message and query.message.chat:
                try:
                    await self._safe_delete_message(context, query.message.chat.id, query.message.message_id)
                except Exception:
                    pass
            return

        if not await self.ensure_authorized(update, context):
            return

        preserve_current_message = (
            data.startswith(f"{CB_ENCRYPTAR}:")
            or data.startswith(f"{CB_RENOMBRAR}:")
            or data.startswith(f"{CB_LISTO}:")
            or data == CB_FAVORITO
            or data == CB_FAVORITO_REMOVE
            or data == CB_CANCEL_PROGRESS
        )
        if query.message and update.effective_chat and not preserve_current_message:
            await self._safe_delete_message(context, update.effective_chat.id, query.message.message_id)
        ensure_defaults(context)
        mode = context.user_data.get(KEY_MODE, MODE_IDLE)

        if data == CB_CANCEL_PROGRESS:
            user = update.effective_user
            user_busy = bool(user and get_user_lock(user.id).locked())
            has_progress = bool(context.user_data.get(KEY_PROGRESS, {}).get("stage"))
            if not user_busy and not has_progress:
                context.user_data[KEY_CANCEL_REQUESTED] = False
                if query.message:
                    try:
                        await query.message.edit_text("No hay una operacion activa para cancelar.", reply_markup=None)
                    except Exception:
                        pass
                return

            context.user_data[KEY_CANCEL_REQUESTED] = True
            self._schedule_cancel_settle_watcher(update, context, user.id if user else None)
            if query.message:
                try:
                    await query.message.edit_text(
                        "⏳ Cancelando...\nLa operacion se detendra en el siguiente punto seguro.",
                        reply_markup=None,
                    )
                except Exception:
                    pass
            return

        if data.startswith(f"{CB_LISTO}:"):
            action_id = data.split(":", 1)[1]
            self._forget_result_action(context, action_id)
            if query.message:
                try:
                    await query.message.edit_reply_markup(reply_markup=favorite_keyboard())
                except Exception:
                    pass
            return

        if data.startswith(f"{CB_ENCRYPTAR}:"):
            action_id = data.split(":", 1)[1]
            await self._begin_result_action(update, context, "encrypt", action_id)
            return

        if data.startswith(f"{CB_RENOMBRAR}:"):
            action_id = data.split(":", 1)[1]
            await self._begin_result_action(update, context, "rename", action_id)
            return

        if data == CB_FAVORITO:
            if query.message:
                ok = await self._send_to_favorites_channel(update, context, query.message.message_id)
                if ok:
                    try:
                        await query.answer("Enviado a favoritos", cache_time=0)
                    except Exception:
                        pass
            return

        if data == CB_COMPRIMIR:
            await self.ask_compress(update, context)
            return
        if data == CB_EXTRAER:
            await self.ask_extract(update, context)
            return
        if data == CB_LIMPIAR:
            await self.clear_chat_and_files(update, context)
            return
        if data == CB_MENU_PRINCIPAL:
            await self.start_handler(update, context)
            return
        if data == CB_CANCELAR:
            await self.cancel_flow(update, context)
            return

        if data == CB_FINALIZAR_COMPRESION and mode == MODE_WAIT_COMPRESS_FILES:
            await self.finalize_compression(update, context)
            return

        if data == CB_FINALIZAR_EXTRACCION and mode == MODE_WAIT_EXTRACT_ARCHIVES:
            await self.finalize_extraction(update, context)
            return

        if query.message:
            sent = await query.message.reply_text(format_bot_text("Accion no disponible en el estado actual."), reply_markup=start_keyboard())
            remember_message(update, context, sent.message_id)

    async def file_router(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return

        if not await self.ensure_authorized(update, context):
            return

        ensure_defaults(context)
        chat = update.effective_chat
        start_message_id = context.user_data.get(KEY_START_MESSAGE_ID)
        if chat and start_message_id:
            await self._safe_delete_message(context, chat.id, int(start_message_id))
            context.user_data[KEY_START_MESSAGE_ID] = None

        mode = context.user_data.get(KEY_MODE, MODE_IDLE)
        file_info = detect_message_file(update.message)

        if not file_info:
            await tracked_reply(update, context, "Tipo de archivo no soportado por este flujo.")
            return

        if mode == MODE_WAIT_COMPRESS_FILES:
            await self.queue_file_for_compression(update, context, file_info)
            return

        if mode in (MODE_WAIT_EXTRACT_ARCHIVES, MODE_WAIT_PASSWORD):
            if not is_archive_file(file_info["file_name"]):
                await tracked_reply(update, context, "Ese archivo no parece comprimido. Envia ZIP/RAR/TAR/GZ/BZ2/XZ.", reply_markup=extract_keyboard())
                return

            if mode == MODE_WAIT_PASSWORD:
                context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES
            await self.queue_file_for_extraction(update, context, file_info)
            return

        await tracked_reply(update, context, "Primero elige una opcion desde /start o usando los botones.", reply_markup=start_keyboard())

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.exception("Unhandled exception in update processing", exc_info=context.error)


def build_application(token: str, cfg: BotConfig) -> Application:
    handlers = BotHandlers(cfg)
    defaults = Defaults(parse_mode=ParseMode.HTML)
    request = HTTPXRequest(
        read_timeout=cfg.telegram_read_timeout,
        write_timeout=cfg.telegram_write_timeout,
        connect_timeout=cfg.telegram_connect_timeout,
        pool_timeout=cfg.telegram_pool_timeout,
        connection_pool_size=max(8, cfg.max_download_concurrency + cfg.max_upload_concurrency + 2),
        httpx_kwargs={"trust_env": cfg.telegram_use_env_proxy},
    )
    builder = (
        Application.builder()
        .token(token)
        .defaults(defaults)
        .request(request)
        .get_updates_request(request)
        .concurrent_updates(cfg.max_concurrent_updates)
    )
    if cfg.telegram_api_base_url:
        builder = builder.base_url(cfg.telegram_api_base_url)
    if cfg.telegram_api_file_base_url:
        builder = builder.base_file_url(cfg.telegram_api_file_base_url)
    app = builder.build()
    app.add_handler(CommandHandler("start", handlers.start_handler))
    app.add_handler(CommandHandler("status", handlers.status_handler))
    app.add_handler(CallbackQueryHandler(handlers.callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.text_router))
    app.add_handler(MessageHandler(filters.ATTACHMENT, handlers.file_router))
    app.add_error_handler(handlers.error_handler)
    return app


def run_bot(cfg: BotConfig) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en variables de entorno")

    async def _runner() -> None:
        app = build_application(token, cfg)
        for attempt in range(1, cfg.startup_max_attempts + 1):
            try:
                await app.initialize()
                break
            except (TimedOut, NetworkError) as exc:
                logger.warning(
                    "Fallo de red al iniciar (%s/%s): %s",
                    attempt,
                    cfg.startup_max_attempts,
                    exc,
                )
                try:
                    await app.shutdown()
                except Exception:
                    pass

                if attempt >= cfg.startup_max_attempts:
                    raise RuntimeError(
                        "No se pudo conectar a Telegram durante el inicio. Verifica red/proxy y reintenta."
                    ) from exc

                await asyncio.sleep(cfg.startup_retry_delay)
                app = build_application(token, cfg)

        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        try:
            await asyncio.Event().wait()
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    asyncio.run(_runner())
