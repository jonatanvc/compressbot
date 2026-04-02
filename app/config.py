import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BotConfig:
    telegram_api_id: int | None
    telegram_api_hash: str | None
    telegram_max_upload_bytes: int
    upload_part_bytes: int
    max_download_concurrency: int
    max_upload_concurrency: int
    max_retries: int
    retry_base_delay: float
    enable_checksum: bool
    allowed_user_ids: set[int]
    only_private_chat: bool
    progress_bar_size: int
    progress_min_update_seconds: float
    telegram_connect_timeout: float
    telegram_read_timeout: float
    telegram_write_timeout: float
    telegram_pool_timeout: float
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


def load_config() -> BotConfig:
    allowed = {
        int(raw.strip())
        for raw in os.getenv("ALLOWED_USER_IDS", "").split(",")
        if raw.strip().isdigit()
    }
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
    return BotConfig(
        telegram_api_id=telegram_api_id,
        telegram_api_hash=telegram_api_hash,
        telegram_max_upload_bytes=int(os.getenv("TELEGRAM_MAX_UPLOAD_BYTES", str(1900 * 1024 * 1024))),
        upload_part_bytes=int(os.getenv("UPLOAD_PART_BYTES", str(1500 * 1024 * 1024))),
        max_download_concurrency=max(1, int(os.getenv("MAX_DOWNLOAD_CONCURRENCY", "4"))),
        max_upload_concurrency=max(1, int(os.getenv("MAX_UPLOAD_CONCURRENCY", "3"))),
        max_retries=max(1, int(os.getenv("MAX_RETRIES", "4"))),
        retry_base_delay=max(0.2, float(os.getenv("RETRY_BASE_DELAY", "0.8"))),
        enable_checksum=_parse_bool(os.getenv("ENABLE_CHECKSUM", "1"), True),
        allowed_user_ids=allowed,
        only_private_chat=_parse_bool(os.getenv("ONLY_PRIVATE_CHAT", "1"), True),
        progress_bar_size=16,
        progress_min_update_seconds=0.25,
        telegram_connect_timeout=max(5.0, float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "30"))),
        telegram_read_timeout=max(20.0, float(os.getenv("TELEGRAM_READ_TIMEOUT", "180"))),
        telegram_write_timeout=max(20.0, float(os.getenv("TELEGRAM_WRITE_TIMEOUT", "180"))),
        telegram_pool_timeout=max(5.0, float(os.getenv("TELEGRAM_POOL_TIMEOUT", "30"))),
        telegram_use_env_proxy=_parse_bool(os.getenv("TELEGRAM_USE_ENV_PROXY", "0"), False),
        telegram_api_base_url=api_base_url,
        telegram_api_file_base_url=api_file_base_url,
        startup_max_attempts=max(1, int(os.getenv("STARTUP_MAX_ATTEMPTS", "8"))),
        startup_retry_delay=max(1.0, float(os.getenv("STARTUP_RETRY_DELAY", "5"))),
        favorites_channel_id=favorites_channel_id,
    )
