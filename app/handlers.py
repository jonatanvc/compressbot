import asyncio
import logging
import os
import re
import shutil
import subprocess
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

    def _download_timeout_seconds(self) -> float:
        base = self.cfg.telegram_read_timeout + self.cfg.telegram_connect_timeout + self.cfg.telegram_pool_timeout
        return max(45.0, min(600.0, base + 30.0))

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
        async def _download_to_drive_once() -> None:
            if target_path.exists():
                try:
                    target_path.unlink()
                except Exception:
                    pass

            async def _inner() -> None:
                tg_file = await context.bot.get_file(
                    file_id,
                    read_timeout=self.cfg.telegram_read_timeout,
                    connect_timeout=self.cfg.telegram_connect_timeout,
                    pool_timeout=self.cfg.telegram_pool_timeout,
                )
                await tg_file.download_to_drive(
                    custom_path=str(target_path),
                    read_timeout=self.cfg.telegram_read_timeout,
                    write_timeout=self.cfg.telegram_write_timeout,
                    connect_timeout=self.cfg.telegram_connect_timeout,
                    pool_timeout=self.cfg.telegram_pool_timeout,
                )

            async def _with_polling() -> None:
                task = asyncio.create_task(_inner())
                try:
                    while not task.done():
                        if on_progress:
                            try:
                                current_size = target_path.stat().st_size if target_path.exists() else 0
                            except Exception:
                                current_size = 0
                            await on_progress(max(0, int(current_size)))
                        await asyncio.sleep(0.35)
                    await task
                finally:
                    if not task.done():
                        task.cancel()
                if on_progress:
                    try:
                        final_size = target_path.stat().st_size if target_path.exists() else 0
                    except Exception:
                        final_size = 0
                    await on_progress(max(0, int(final_size)))

            await asyncio.wait_for(_with_polling(), timeout=self._download_timeout_seconds())

        try:
            await retry_async_with_cancel(
                self.cfg,
                label,
                _download_to_drive_once,
                cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
            )
        except Exception as first_exc:
            if "file is too big" in str(first_exc).lower():
                raise RuntimeError(
                    "Telegram rechazo la descarga por tamano (File is too big). "
                    "Para manejar archivos muy grandes, configura un servidor local de Bot API "
                    "y define TELEGRAM_API_BASE_URL / TELEGRAM_API_FILE_BASE_URL."
                ) from first_exc
            if not self._is_ssl_record_error(first_exc):
                raise

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
        try:
            for _ in range(180):
                await asyncio.sleep(0.5)
                progress_state: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
                has_progress = bool(progress_state.get("stage"))
                user_busy = bool(user_id and get_user_lock(user_id).locked())

                if user_busy:
                    continue

                if has_progress:
                    await self._cleanup_progress_message(update, context)
                context.user_data[KEY_CANCEL_REQUESTED] = False
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

    async def _cleanup_progress_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
        chat = update.effective_chat
        if not chat:
            return
        progress: dict[str, Any] = context.user_data.get(KEY_PROGRESS, {})
        msg_id = progress.get("message_id")
        if msg_id:
            await self._safe_delete_message(context, chat.id, msg_id)
        context.user_data[KEY_PROGRESS] = {}
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
        if msg_id:
            await self._safe_delete_message(context, chat.id, int(msg_id))
        context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = None

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
        context.user_data[KEY_PENDING_FILE_ID] = None
        context.user_data[KEY_PENDING_FILE_NAME] = None
        context.user_data[KEY_PENDING_SOURCE_MESSAGE_ID] = None

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
                await upsert_progress(
                    update,
                    context,
                    self.cfg,
                    "Descomprimiendo archivos",
                    extract_bytes_done,
                    total_extract_bytes,
                    f"Procesando {index}/{len(targets)}: {local_archive.name}",
                    cancellable=True,
                )
                heartbeat_task = self._start_progress_heartbeat(update, context)
                try:
                    async with get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs):
                        await run_extract_in_thread(
                            local_archive,
                            out_dir,
                            password=current_password,
                            cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                        )
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
                await tracked_reply(
                    update,
                    context,
                    f"El archivo '{local_archive.name}' requiere password. Escribela para continuar.",
                    reply_markup=waiting_upload_keyboard(),
                )
                prompt_id = context.user_data.get(KEY_MESSAGE_IDS, [])[-1] if context.user_data.get(KEY_MESSAGE_IDS) else None
                context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = prompt_id
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
                await tracked_reply(
                    update,
                    context,
                    "Password incorrecta. Escribe una nueva password para reintentar.",
                    reply_markup=waiting_upload_keyboard(),
                )
                prompt_id = context.user_data.get(KEY_MESSAGE_IDS, [])[-1] if context.user_data.get(KEY_MESSAGE_IDS) else None
                context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = prompt_id
                return False
            except (MissingRarBackendError, Missing7zBackendError) as exc:
                await tracked_reply(update, context, str(exc), reply_markup=extract_keyboard())
                continue
            except Exception as exc:
                await tracked_reply(update, context, f"No se pudo extraer '{local_archive.name}'. Detalle: {exc}", reply_markup=extract_keyboard())
                continue

            extracted_files.extend(str(path) for path in collect_extracted_files(out_dir))
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
                f"Extraido {index}/{len(targets)}: {local_archive.name}",
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
    ) -> str:
        ensure_defaults(context)
        final_action_id = action_id or uuid.uuid4().hex[:10]
        result_actions: dict[str, dict[str, Any]] = context.user_data[KEY_RESULT_ACTIONS]
        result_actions[final_action_id] = {
            "file_id": file_id,
            "file_name": file_name,
            "message_id": message_id,
        }
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

        text = (
            "📊 <b>Panel de Estado</b>\n\n"
            f"• Modo actual: <code>{mode}</code>\n"
            f"• Cola de compresión: <b>{len(files_to_compress)}</b> archivo(s)\n"
            f"• Cola de extracción: <b>{len(archives_to_extract)}</b> archivo(s)\n"
            f"• Pendientes de envío: <b>{len(extracted_files)}</b>\n"
            f"• Progreso activo: <b>{progress_stage}</b> ({progress_percent:.1f}%)\n"
            f"• Concurrencia descarga/subida: <b>{self.cfg.max_download_concurrency}/{self.cfg.max_upload_concurrency}</b>\n"
            f"• Reintentos máximos: <b>{self.cfg.max_retries}</b>\n"
            f"• Checksum SHA-256: <b>{'activo' if self.cfg.enable_checksum else 'inactivo'}</b>\n"
            f"• Operación en curso: <b>{'sí' if busy else 'no'}</b>"
        )
        await tracked_reply(update, context, text, reply_markup=start_keyboard(), is_html=True)

    async def ask_compress(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        ensure_defaults(context)
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
        if user and (get_user_lock(user.id).locked() or bool(context.user_data.get(KEY_PROGRESS))):
            context.user_data[KEY_CANCEL_REQUESTED] = True
            self._schedule_cancel_settle_watcher(update, context, user.id)
            await tracked_reply(update, context, "⏳ Cancelando... la operación se detendrá en el siguiente punto seguro.", reply_markup=start_keyboard())
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
                                    inflight_download_bytes[item_key] = max(0, min(int(item.get("file_size", 0)), size_now))
                                    partial_sum = sum(inflight_download_bytes.values())
                                    current_total = min(
                                        max(total_download_bytes, 1),
                                        max(0, downloaded_bytes + partial_sum),
                                    )
                                    await upsert_progress(
                                        update,
                                        context,
                                        self.cfg,
                                        "Descargando archivos",
                                        current_total,
                                        max(total_download_bytes, current_total, 1),
                                        f"{downloaded_count}/{len(files_to_compress)} archivos | {format_bytes(current_total)} descargados",
                                        cancellable=True,
                                    )

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
                            async with progress_lock:
                                failed_downloads.append(item)
                        except Exception:
                            async with progress_lock:
                                inflight_download_bytes.pop(str(item["file_id"]), None)
                            async with progress_lock:
                                failed_downloads.append(item)

                await asyncio.gather(*(download_one(item) for item in files_to_compress))
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

                with zipfile.ZipFile(
                    archive_path,
                    "w",
                    compression=zipfile.ZIP_DEFLATED,
                    compresslevel=self.cfg.zip_compress_level,
                ) as zf:
                    for index, file_path in enumerate(files_for_zip, start=1):
                        self._raise_if_cancel_requested(context)
                        zf.write(file_path, arcname=file_path.name)
                        compressed_progress += file_path.stat().st_size
                        await upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Comprimiendo archivos",
                            compressed_progress,
                            max(total_compress_bytes, compressed_progress, 1),
                            f"{index}/{len(files_for_zip)} archivos | {format_bytes(compressed_progress)} procesados",
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
                output_parts = await asyncio.to_thread(
                    split_file_for_upload,
                    archive_path,
                    self.cfg,
                    lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                )
                output_size = archive_path.stat().st_size
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
                )

                sent_ids: list[int] = []
                for part_index, part in enumerate(output_parts, start=1):
                    self._raise_if_cancel_requested(context)
                    caption = None
                    if len(output_parts) == 1:
                        caption = f"Compresion completada: {len(files_to_compress)} archivo(s) procesados."
                    elif part_index == 1:
                        caption = f"Compresion completada en {len(output_parts)} partes. Total procesado: {len(files_to_compress)} archivo(s)."
                    action_id_for_single = uuid.uuid4().hex[:10] if len(output_parts) == 1 else None

                    async def _send_part():
                        with part.open("rb") as archive_file:
                            sent_local = await asyncio.wait_for(
                                context.bot.send_document(
                                    chat.id,
                                    document=archive_file,
                                    filename=part.name,
                                    caption=caption,
                                    reply_markup=compressed_result_keyboard(action_id_for_single)
                                    if action_id_for_single
                                    else favorite_keyboard(),
                                ),
                                timeout=self.cfg.telegram_operation_timeout,
                            )
                        return sent_local

                    # Evita duplicados: para envios no idempotentes solo se reintenta en rate-limit.
                    for attempt in range(3):
                        self._raise_if_cancel_requested(context)
                        try:
                            sent = await _send_part()
                            break
                        except RetryAfter as retry_exc:
                            self._raise_if_cancel_requested(context)
                            await asyncio.sleep(min(2.0, float(retry_exc.retry_after) + 0.2))
                    else:
                        raise RuntimeError(f"No se pudo subir la parte {part.name} por limite de Telegram")
                    sent_ids.append(sent.message_id)
                    if len(output_parts) == 1 and sent.document:
                        self._register_result_action(
                            context,
                            file_id=sent.document.file_id,
                            file_name=sent.document.file_name or part.name,
                            message_id=sent.message_id,
                            action_id=action_id_for_single,
                        )
                    uploaded_bytes_parts += max(0, part.stat().st_size)
                    await upsert_progress(
                        update,
                        context,
                        self.cfg,
                        "Subiendo resultado",
                        min(uploaded_bytes_parts, total_upload_bytes_parts),
                        total_upload_bytes_parts,
                        f"{part_index}/{len(output_parts)} partes subidas",
                        cancellable=True,
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
                if compression_summary_text:
                    await tracked_reply(update, context, compression_summary_text)
                await self._stop_progress_heartbeat(heartbeat_task)
                await self._cleanup_progress_message(update, context)
                cleanup_user_workspace(user.id, recreate=False)
                heavy_gate.release()

        context.user_data[KEY_FILES_TO_COMPRESS] = []
        context.user_data[KEY_MODE] = MODE_IDLE
        context.user_data[KEY_COMPRESS_QUEUE_MESSAGE_ID] = None
        context.user_data[KEY_LAST_COMPRESS_QUEUE_TEXT] = ""

    async def process_extract_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE, password: str | None = None) -> bool:
        should_cleanup_progress = False
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
                            inflight_extract_download_bytes[item_key] = max(0, min(int(item.get("file_size", 0)), size_now))
                            partial_sum = sum(inflight_extract_download_bytes.values())
                            current_total = min(
                                max(total_download_bytes, 1),
                                max(0, downloaded_bytes + partial_sum),
                            )
                            await upsert_progress(
                                update,
                                context,
                                self.cfg,
                                "Descargando comprimidos",
                                current_total,
                                max(total_download_bytes, current_total, 1),
                                (
                                    f"{downloaded_archives}/{initial_queue_total} descargados | "
                                    f"{format_bytes(current_total)} de {format_bytes(max(total_download_bytes, 1))}"
                                ),
                                cancellable=True,
                            )

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
                    async with progress_lock:
                        failed_download_items.append(item)
                except Exception:
                    async with progress_lock:
                        inflight_extract_download_bytes.pop(str(item["file_id"]), None)
                    async with progress_lock:
                        failed_download_items.append(item)

        try:
            async with heavy_gate:
                await asyncio.gather(*(_download_extract_item(item) for item in list(queue)))
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
                completed = await self._extract_local_targets(update, context, targets, initial_queue_total, password)
                if not completed:
                    return False

            context.user_data[KEY_MODE] = MODE_IDLE

            if not extracted_files:
                cleanup_user_workspace(user.id, recreate=False)
                context.user_data[KEY_EXTRACT_TOTAL_RECEIVED] = 0
                context.user_data[KEY_EXTRACT_ARCHIVES_DONE] = 0
                await upsert_progress(update, context, self.cfg, "Proceso completado", 1, 1, "No se encontraron archivos extraibles.", force=True)
                await tracked_reply(update, context, "Proceso finalizado: no se detectaron archivos extraibles en los comprimidos enviados.")
                should_cleanup_progress = True
                return True

            paths_to_send = [Path(file_str) for file_str in extracted_files if Path(file_str).exists()]
            total_upload_parts = sum(expected_parts_for_size(path.stat().st_size, self.cfg) for path in paths_to_send)
            sent_parts = 0
            total_upload_bytes = sum(path.stat().st_size for path in paths_to_send)
            uploaded_bytes = 0

            await upsert_progress(update, context, self.cfg, "Subiendo archivos extraidos", 0, max(total_upload_parts, 1), f"0/{max(total_upload_parts, 1)} partes | {format_bytes(total_upload_bytes)} para subir", force=True, cancellable=True)

            upload_sem = asyncio.Semaphore(self.cfg.max_upload_concurrency)
            progress_lock = asyncio.Lock()

            async def upload_extracted_path(file_path: Path) -> None:
                nonlocal sent_parts, uploaded_bytes
                self._raise_if_cancel_requested(context)
                async with upload_sem:
                    self._raise_if_cancel_requested(context)
                    validate_file_ready(file_path)
                    sent_ids = await send_path_with_size_control(
                        context,
                        self.cfg,
                        chat.id,
                        file_path,
                        reply_markup=favorite_keyboard(),
                        caption_footer=f"<code>{file_path.name}</code>",
                        cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                    )
                    async with progress_lock:
                        for msg_id in sent_ids:
                            remember_message(update, context, msg_id)
                        sent_parts += len(sent_ids)
                        uploaded_bytes += max(0, file_path.stat().st_size)
                        await upsert_progress(
                            update,
                            context,
                            self.cfg,
                            "Subiendo archivos extraidos",
                            min(uploaded_bytes, max(total_upload_bytes, 1)),
                            max(total_upload_bytes, 1),
                            f"{sent_parts}/{max(total_upload_parts, 1)} partes subidas",
                            cancellable=True,
                        )

            await asyncio.gather(*(upload_extracted_path(file_path) for file_path in paths_to_send))

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
                f"Archivos enviados: {len(paths_to_send)}\n"
                f"Partes subidas: {sent_parts}\n"
                f"Tamano total enviado: {format_bytes(total_upload_bytes)}"
            )
            await tracked_reply(update, context, summary_text)
            should_cleanup_progress = True
            return True
        finally:
            await self._stop_progress_heartbeat(heartbeat_task)
            if locals().get("should_cleanup_progress", False):
                await self._cleanup_progress_message(update, context)

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
                await self.process_extract_queue(update, context)
            except RuntimeError as exc:
                if "cancelada" in str(exc).lower():
                    context.user_data[KEY_MODE] = MODE_IDLE
                    context.user_data[KEY_ARCHIVES_TO_EXTRACT] = []
                    context.user_data[KEY_PENDING_LOCAL_TARGETS] = []
                    context.user_data[KEY_LAST_EXTRACT_QUEUE_TEXT] = ""
                    await self._cleanup_progress_message(update, context)
                    await tracked_reply(update, context, "Extraccion cancelada por el usuario.", reply_markup=start_keyboard())
                    return
                context.user_data[KEY_MODE] = MODE_IDLE
                await self._cleanup_progress_message(update, context)
                await tracked_reply(update, context, f"No se pudo completar la extraccion. Detalle tecnico: {exc}", reply_markup=start_keyboard())
                return
            except Exception as exc:
                context.user_data[KEY_MODE] = MODE_IDLE
                await self._cleanup_progress_message(update, context)
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

        try:
            async with get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs):
                await run_extract_in_thread(
                    archive_path,
                    output_dir,
                    password=password,
                    cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
                )
        except InvalidPasswordError:
            context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
            await tracked_reply(update, context, "Password incorrecta. Intenta nuevamente.")
            ids = context.user_data.get(KEY_MESSAGE_IDS, [])
            context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = ids[-1] if ids else None
            return
        except PasswordRequiredError:
            context.user_data[KEY_MODE] = MODE_WAIT_PASSWORD
            await tracked_reply(update, context, "El archivo sigue solicitando password. Intenta de nuevo.")
            ids = context.user_data.get(KEY_MESSAGE_IDS, [])
            context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = ids[-1] if ids else None
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
                await tracked_reply(update, context, "Password incorrecta o archivo cifrado inconsistente. Intenta nuevamente.")
                ids = context.user_data.get(KEY_MESSAGE_IDS, [])
                context.user_data[KEY_PASSWORD_PROMPT_MESSAGE_ID] = ids[-1] if ids else None
                return
            raise
        except Exception as exc:
            context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES
            context.user_data[KEY_CURRENT_ARCHIVE] = None
            await tracked_reply(update, context, f"Error al extraer usando password. Detalle: {exc}", reply_markup=extract_keyboard())
            return

        extracted_files: list[str] = context.user_data[KEY_EXTRACTED_FILES]
        extracted_files.extend(str(path) for path in collect_extracted_files(output_dir))
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
                await self._cleanup_progress_message(update, context)
                await tracked_reply(update, context, "Extraccion cancelada por el usuario.", reply_markup=start_keyboard())
                return
            context.user_data[KEY_MODE] = MODE_WAIT_EXTRACT_ARCHIVES
            await self._cleanup_progress_message(update, context)
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
    ) -> None:
        """Convierte un ZIP a 7z cifrado con password unica y header encryption."""
        exe = self._get_7z_executable()

        def _run_7z(cmd: list[str], cwd: Path) -> tuple[int, str]:
            proc = subprocess.Popen(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            started = time.monotonic()
            hard_timeout = max(float(self.cfg.telegram_operation_timeout) * 6.0, 900.0)
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
                    time.sleep(0.4)
                stdout, stderr = proc.communicate()
            except Exception:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=3.0)
                raise
            return proc.returncode, (stderr or stdout or "").strip()

        work_dir = encrypted_7z.parent / f"__enc_build_{uuid.uuid4().hex}"
        extracted_dir = work_dir / "input"
        work_dir.mkdir(parents=True, exist_ok=True)
        extracted_dir.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(source_zip, "r") as zf:
                zf.extractall(extracted_dir)

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
            ]
            code, detail = _run_7z(cmd, extracted_dir)
            if code != 0:
                detail_lower = detail.lower()
                if "errno=2" in detail_lower or "no such file or directory" in detail_lower:
                    # Fallback robusto: encapsula el ZIP original en 7z cifrado cuando hay entradas inconsistentes.
                    wrap_cmd = [
                        exe,
                        "a",
                        "-t7z",
                        str(encrypted_7z),
                        source_zip.name,
                        f"-p{password}",
                        "-mhe=on",
                        "-y",
                    ]
                    wrap_code, wrap_detail = _run_7z(wrap_cmd, source_zip.parent)
                    if wrap_code == 0:
                        return
                    raise RuntimeError(f"Fallo al crear 7z cifrado (fallback ZIP): {wrap_detail}")

                raise RuntimeError(f"Fallo al crear 7z cifrado: {detail}")
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    def _assert_7z_is_encrypted(self, encrypted_7z: Path, password: str) -> None:
        """Valida que el 7z exige password y que con la password correcta es legible."""
        exe = self._get_7z_executable()

        # Sin password debe fallar por archivo cifrado/cabeceras cifradas.
        no_pwd = subprocess.run(
            [exe, "t", str(encrypted_7z), "-p-", "-y"],
            capture_output=True,
            text=True,
        )
        no_pwd_output = f"{no_pwd.stdout}\n{no_pwd.stderr}".lower()
        if no_pwd.returncode == 0:
            raise RuntimeError("El archivo 7z no esta protegido: se pudo leer sin password")
        if not any(token in no_pwd_output for token in ["wrong password", "can not open encrypted archive", "password"]):
            raise RuntimeError("No se pudo verificar proteccion por password en el 7z generado")

        # Con password correcta debe pasar.
        with_pwd = subprocess.run(
            [exe, "t", str(encrypted_7z), f"-p{password}", "-y"],
            capture_output=True,
            text=True,
        )
        if with_pwd.returncode != 0:
            detail = (with_pwd.stderr or with_pwd.stdout or "").strip()
            raise RuntimeError(f"No se pudo validar el 7z con la password indicada: {detail}")

    async def _send_result_with_actions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: Path, caption: str) -> None:
        chat = update.effective_chat
        if not chat:
            return

        if file_path.stat().st_size > self.cfg.telegram_max_upload_bytes:
            sent_ids = await send_path_with_size_control(
                context,
                self.cfg,
                chat.id,
                file_path,
                reply_markup=favorite_keyboard(),
                cancel_requested=lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
            )
            for msg_id in sent_ids:
                remember_message(update, context, msg_id)
            await tracked_reply(
                update,
                context,
                "Resultado enviado en varias partes. Puedes usar 'Anadir a favoritos' en cualquiera de las partes.",
                reply_markup=start_keyboard(),
            )
            return

        async def _send_one():
            action_id = uuid.uuid4().hex[:10]
            with file_path.open("rb") as payload:
                sent_local = await context.bot.send_document(
                    chat.id,
                    document=payload,
                    filename=file_path.name,
                    caption=caption,
                    reply_markup=compressed_result_keyboard(action_id),
                )
            return sent_local, action_id

        sent, action_id = await retry_async(self.cfg, f"Subida {file_path.name}", _send_one)
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
        source_path = unique_target_path(post_dir, source_name)
        heartbeat_task: asyncio.Task[Any] | None = None
        heavy_gate = get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs)
        
        await heavy_gate.acquire()
        try:
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", 0, 1024, "Preparando descarga", force=True, cancellable=True)
            heartbeat_task = self._start_progress_heartbeat(update, context)
            await self._download_pending_file(context, source_path)
            self._raise_if_cancel_requested(context)
            source_size = max(1, source_path.stat().st_size)
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", source_size, source_size, "Archivo descargado. Preparando cifrado 7z", force=True, cancellable=True)
            
            if source_path.suffix.lower() != ".zip":
                raise RuntimeError("Solo se permite encryptar archivos ZIP en este flujo")

            temp_encrypted = post_dir / f"__enc_{uuid.uuid4().hex}.7z"
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", 0, source_size, "Cifrando contenido", force=True, cancellable=True)
            await asyncio.to_thread(
                self._encrypt_zip_as_7z,
                source_path,
                temp_encrypted,
                password,
                lambda: bool(context.user_data.get(KEY_CANCEL_REQUESTED, False)),
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
            await upsert_progress(update, context, self.cfg, "Encryptando archivo", encrypted_size, encrypted_size, "Proceso finalizado correctamente", force=True)
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
            await self._cleanup_progress_message(update, context)
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
        source_path = unique_target_path(post_dir, original_name)
        heartbeat_task: asyncio.Task[Any] | None = None
        heavy_gate = get_global_heavy_job_semaphore(self.cfg.max_parallel_heavy_jobs)

        await heavy_gate.acquire()
        try:
            await upsert_progress(update, context, self.cfg, "Renombrando archivo", 0, 1024, "Preparando descarga", force=True, cancellable=True)
            heartbeat_task = self._start_progress_heartbeat(update, context)
            await self._download_pending_file(context, source_path)
            self._raise_if_cancel_requested(context)
            source_size = max(1, source_path.stat().st_size)
            await upsert_progress(update, context, self.cfg, "Renombrando archivo", source_size, source_size, "Archivo descargado. Aplicando nuevo nombre", force=True, cancellable=True)

            renamed_path = source_path.with_name(requested_name)
            source_path.rename(renamed_path)
            validate_file_ready(renamed_path)

            renamed_size = max(1, renamed_path.stat().st_size)
            await upsert_progress(update, context, self.cfg, "Subiendo archivo renombrado", 0, renamed_size, "Iniciando subida", force=True, cancellable=True)

            await self._send_result_with_actions(update, context, renamed_path, "Archivo renombrado correctamente.")
            await upsert_progress(update, context, self.cfg, "Renombrando archivo", renamed_size, renamed_size, "Proceso finalizado correctamente", force=True)
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
            await self._cleanup_progress_message(update, context)
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
