import atexit
import logging
import os
import tempfile
from pathlib import Path
from dotenv import load_dotenv

from .config import load_config
from .handlers import run_bot


logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)


_INSTANCE_LOCK_FILE = Path(tempfile.gettempdir()) / "telegram_compress_bot_instance.pid"


def _process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _acquire_instance_lock() -> None:
    current_pid = os.getpid()
    if _INSTANCE_LOCK_FILE.exists():
        try:
            previous_pid = int(_INSTANCE_LOCK_FILE.read_text(encoding="utf-8").strip() or "0")
        except Exception:
            previous_pid = 0
        if previous_pid and previous_pid != current_pid and _process_exists(previous_pid):
            raise RuntimeError(
                f"Ya existe una instancia del bot ejecutandose (PID {previous_pid}). Cierra la instancia previa antes de iniciar otra."
            )

    _INSTANCE_LOCK_FILE.write_text(str(current_pid), encoding="utf-8")

    def _release() -> None:
        try:
            if _INSTANCE_LOCK_FILE.exists():
                stored_pid = int(_INSTANCE_LOCK_FILE.read_text(encoding="utf-8").strip() or "0")
                if stored_pid == current_pid:
                    _INSTANCE_LOCK_FILE.unlink(missing_ok=True)
        except Exception:
            pass

    atexit.register(_release)


def main() -> None:
    load_dotenv()
    _acquire_instance_lock()
    cfg = load_config()
    run_bot(cfg)


if __name__ == "__main__":
    main()
