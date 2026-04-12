import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BotConfig:
    telegram_api_id: int | None
    telegram_api_hash: str | None
    owner_user_id: int | None
    telegram_max_upload_bytes: int
    upload_part_bytes: int
    single_upload_max_bytes: int
    max_download_concurrency: int
    max_upload_concurrency: int
    max_retries: int
    retry_base_delay: float
    enable_checksum: bool
    checksum_max_bytes: int
    allowed_user_ids: set[int]
    only_private_chat: bool
    progress_bar_size: int
    progress_min_update_seconds: float
    zip_compress_level: int
    max_concurrent_updates: int
    max_parallel_heavy_jobs: int
    telegram_connect_timeout: float
    telegram_read_timeout: float
    telegram_write_timeout: float
    telegram_pool_timeout: float
    telegram_operation_timeout: float
    telegram_use_env_proxy: bool
    telegram_api_base_url: str | None
    telegram_api_file_base_url: str | None
    startup_max_attempts: int
    startup_retry_delay: float
    favorites_channel_id: int | str | None


def _parse_bool(raw: str, default: bool) -> bool:
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_user_id_list(raw: str) -> set[int]:
    if not raw:
        return set()
    values: set[int] = set()
    for chunk in raw.replace(";", ",").split(","):
        token = chunk.strip()
        if not token:
            continue
        if token.startswith("#"):
            continue
        if token.isdigit():
            values.add(int(token))
            continue
        if token.startswith("-") and token[1:].isdigit():
            # No deberia ocurrir para user_id, pero lo aceptamos igual para evitar sorpresas.
            values.add(int(token))
            continue
    return values


def load_config() -> BotConfig:
    cpu_count = max(1, (os.cpu_count() or 1))
    default_download_concurrency = 1 if cpu_count <= 1 else min(3, cpu_count * 2)
    default_upload_concurrency = 1 if cpu_count <= 1 else min(2, cpu_count)
    default_updates = 2 if cpu_count <= 1 else max(8, cpu_count * 3)
    default_parallel_heavy_jobs = 1 if cpu_count <= 2 else 2

    owner_raw = os.getenv("OWNER_USER_ID", "").strip()
    owner_user_id = int(owner_raw) if owner_raw.isdigit() else None
    allowed = _parse_user_id_list(os.getenv("ALLOWED_USER_IDS", ""))
    if owner_user_id is not None:
        allowed.add(owner_user_id)
    api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
    telegram_api_id = int(api_id_raw) if api_id_raw.isdigit() else None
    telegram_api_hash = os.getenv("TELEGRAM_API_HASH", "").strip() or None
    favorites_channel_raw = os.getenv("FAVORITES_CHANNEL_ID", "").strip()
    if favorites_channel_raw.isdigit() or (
        favorites_channel_raw.startswith("-") and favorites_channel_raw[1:].isdigit()
    ):
        favorites_channel_id: int | str | None = int(favorites_channel_raw)
    else:
        favorites_channel_id = favorites_channel_raw or None

    api_base_url = os.getenv("TELEGRAM_API_BASE_URL", "").strip() or None
    api_file_base_url = os.getenv("TELEGRAM_API_FILE_BASE_URL", "").strip() or None
    telegram_max_upload_bytes = int(os.getenv("TELEGRAM_MAX_UPLOAD_BYTES", str(4000 * 1024 * 1024)))
    upload_part_bytes = int(os.getenv("UPLOAD_PART_BYTES", str((2 * 1024 * 1024 * 1024) - (8 * 1024 * 1024))))
    # Limite de subida directa: archivos menores se suben sin dividir.
    # Con Bot API local y Telegram Premium se pueden subir hasta 4 GiB directamente.
    default_single_limit = max(1, telegram_max_upload_bytes - (16 * 1024 * 1024))
    single_upload_max_bytes = int(os.getenv("SINGLE_UPLOAD_MAX_BYTES", str(default_single_limit)))

    return BotConfig(
        telegram_api_id=telegram_api_id,
        telegram_api_hash=telegram_api_hash,
        owner_user_id=owner_user_id,
        telegram_max_upload_bytes=telegram_max_upload_bytes,
        upload_part_bytes=upload_part_bytes,
        single_upload_max_bytes=max(1, min(single_upload_max_bytes, telegram_max_upload_bytes)),
        max_download_concurrency=max(1, int(os.getenv("MAX_DOWNLOAD_CONCURRENCY", str(default_download_concurrency)))),
        max_upload_concurrency=max(1, int(os.getenv("MAX_UPLOAD_CONCURRENCY", str(default_upload_concurrency)))),
        max_retries=max(1, int(os.getenv("MAX_RETRIES", "4"))),
        retry_base_delay=max(0.2, float(os.getenv("RETRY_BASE_DELAY", "0.8"))),
        enable_checksum=_parse_bool(os.getenv("ENABLE_CHECKSUM", "1"), True),
        checksum_max_bytes=max(0, int(os.getenv("CHECKSUM_MAX_BYTES", str(1024 * 1024 * 1024)))),
        allowed_user_ids=allowed,
        only_private_chat=_parse_bool(os.getenv("ONLY_PRIVATE_CHAT", "1"), True),
        progress_bar_size=16,
        progress_min_update_seconds=1.5,
        zip_compress_level=min(9, max(0, int(os.getenv("ZIP_COMPRESS_LEVEL", "1")))),
        max_concurrent_updates=max(2, int(os.getenv("MAX_CONCURRENT_UPDATES", str(default_updates)))),
        max_parallel_heavy_jobs=max(1, int(os.getenv("MAX_PARALLEL_HEAVY_JOBS", str(default_parallel_heavy_jobs)))),
        telegram_connect_timeout=max(30.0, float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "60"))),
        telegram_read_timeout=max(30.0, float(os.getenv("TELEGRAM_READ_TIMEOUT", "120"))),
        telegram_write_timeout=max(30.0, float(os.getenv("TELEGRAM_WRITE_TIMEOUT", "120"))),
        telegram_pool_timeout=max(30.0, float(os.getenv("TELEGRAM_POOL_TIMEOUT", "60"))),
        telegram_operation_timeout=max(
            600.0,
            float(
                os.getenv(
                    "TELEGRAM_OPERATION_TIMEOUT",
                    "3600",
                )
            ),
        ),
        telegram_use_env_proxy=_parse_bool(os.getenv("TELEGRAM_USE_ENV_PROXY", "0"), False),
        telegram_api_base_url=api_base_url,
        telegram_api_file_base_url=api_file_base_url,
        startup_max_attempts=max(1, int(os.getenv("STARTUP_MAX_ATTEMPTS", "8"))),
        startup_retry_delay=max(1.0, float(os.getenv("STARTUP_RETRY_DELAY", "5"))),
        favorites_channel_id=favorites_channel_id,
    )
