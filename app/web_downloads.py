import asyncio
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import parse_qs, urlparse


_DRIVE_HOST_MARKERS = ("drive.google.com", "docs.google.com")
_URL_RE = re.compile(r"https?://[^\s<>()\[\]{}\"']+", re.IGNORECASE)


@dataclass(frozen=True)
class WebDownloadResult:
    source_url: str
    local_path: Path
    is_directory: bool


async def _poll_downloaded_bytes(
    root: Path,
    on_progress: Callable[[int], Awaitable[None]] | None,
    cancel_requested: Callable[[], bool] | None,
) -> None:
    if on_progress is None:
        return

    def _dir_size_bytes(path: Path) -> int:
        total = 0
        if not path.exists():
            return 0
        for file_path in path.rglob("*"):
            try:
                if file_path.is_file():
                    total += int(file_path.stat().st_size)
            except Exception:
                continue
        return max(0, total)

    last_value = -1
    while True:
        if cancel_requested and cancel_requested():
            return
        current = await asyncio.to_thread(_dir_size_bytes, root)
        if current != last_value:
            last_value = current
            try:
                await on_progress(current)
            except Exception:
                pass
        await asyncio.sleep(0.8)


def _is_google_drive_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    host = (parsed.netloc or "").lower()
    return any(marker in host for marker in _DRIVE_HOST_MARKERS)


def _looks_like_drive_folder(url: str) -> bool:
    lowered = url.lower()
    if "/folders/" in lowered or "/drive/folders/" in lowered:
        return True

    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        if "id" in qs and "folder" in lowered:
            return True
    except Exception:
        return False

    return False


def extract_google_drive_url(text: str) -> str | None:
    raw = (text or "").strip()
    if not raw:
        return None

    for match in _URL_RE.finditer(raw):
        url = match.group(0).rstrip(".,;)")
        if _is_google_drive_url(url):
            return url
    return None


async def download_google_drive(
    url: str,
    output_root: Path,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> WebDownloadResult:
    if not _is_google_drive_url(url):
        raise ValueError("El enlace no corresponde a Google Drive")

    try:
        import gdown  # type: ignore[import-not-found]
    except Exception as exc:
        raise RuntimeError(
            "Falta la dependencia 'gdown'. Instala requirements.txt actualizado para habilitar descargas de Google Drive."
        ) from exc

    output_root.mkdir(parents=True, exist_ok=True)

    if cancel_requested and cancel_requested():
        raise RuntimeError("Operacion cancelada por el usuario")

    download_workspace = output_root / f"gdrive_{uuid.uuid4().hex[:10]}"
    download_workspace.mkdir(parents=True, exist_ok=True)

    poll_task = asyncio.create_task(_poll_downloaded_bytes(download_workspace, on_progress, cancel_requested))

    try:
        if _looks_like_drive_folder(url):
            folder_out = download_workspace / "folder"
            folder_out.mkdir(parents=True, exist_ok=True)

            def _download_folder() -> list[str] | None:
                return gdown.download_folder(
                    url=url,
                    output=str(folder_out),
                    quiet=True,
                    use_cookies=False,
                    remaining_ok=True,
                )

            downloaded = await asyncio.to_thread(_download_folder)
            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")
            if not downloaded:
                raise RuntimeError("No se pudieron descargar elementos de la carpeta de Google Drive")

            valid_files = [Path(path) for path in downloaded if path and os.path.exists(path)]
            if not valid_files:
                raise RuntimeError("Google Drive no devolvio archivos descargables para esa carpeta")

            root = Path(os.path.commonpath([str(path) for path in valid_files]))
            if root.is_file():
                root = root.parent

            try:
                await on_progress(sum(int(path.stat().st_size) for path in valid_files)) if on_progress else asyncio.sleep(0)
            except Exception:
                pass

            return WebDownloadResult(source_url=url, local_path=root, is_directory=True)

        file_out = download_workspace / "file"
        file_out.mkdir(parents=True, exist_ok=True)

        def _download_file() -> str | None:
            return gdown.download(
                url=url,
                output=str(file_out),
                quiet=True,
                fuzzy=True,
                use_cookies=False,
            )

        downloaded_file = await asyncio.to_thread(_download_file)
        if cancel_requested and cancel_requested():
            raise RuntimeError("Operacion cancelada por el usuario")

        if not downloaded_file:
            raise RuntimeError("No se pudo descargar el archivo de Google Drive")

        local_file = Path(downloaded_file)
        if not local_file.exists() or not local_file.is_file():
            raise RuntimeError("El archivo descargado de Google Drive no es valido")

        if on_progress:
            try:
                await on_progress(int(local_file.stat().st_size))
            except Exception:
                pass

        return WebDownloadResult(source_url=url, local_path=local_file, is_directory=False)
    finally:
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
