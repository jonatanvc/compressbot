import asyncio
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse


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


def extract_drive_file_id(url: str) -> str | None:
    """Extracts Google Drive file ID from any known URL format."""
    # /file/d/FILE_ID/...
    m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    # ?id=FILE_ID or &id=FILE_ID  (uc?export=download&id=...)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    return None


def extract_drive_folder_id(url: str) -> str | None:
    """Extracts Google Drive folder ID from any known URL format."""
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    return None


def is_google_drive_folder_url(url: str) -> bool:
    return extract_drive_folder_id(url) is not None


def _is_html_error_page(file_path: Path) -> bool:
    """Returns True if the file looks like an HTML page (Drive quota/error response)."""
    try:
        if file_path.stat().st_size > 2_000_000:
            return False
        with file_path.open("rb") as fh:
            header = fh.read(1024)
        snippet = header.decode("utf-8", errors="ignore").lower()
        return "<!doctype html" in snippet or "<html" in snippet
    except Exception:
        return False


def _find_downloaded_file(workspace: Path, snapshots_before: set[Path]) -> Path | None:
    """Finds the file downloaded into *workspace* by comparing before/after snapshots."""
    try:
        all_now = {f for f in workspace.rglob("*") if f.is_file()}
    except Exception:
        return None
    new_files = sorted(all_now - snapshots_before, key=lambda f: f.stat().st_size, reverse=True)
    valid = [f for f in new_files if not _is_html_error_page(f)]
    return valid[0] if valid else None


async def fetch_gdrive_file_size_async(file_id: str) -> int:
    """Intenta obtener el tamaño real del archivo antes de descargarlo. Retorna 0 si no es posible."""
    try:
        import httpx  # type: ignore[import-not-found]

        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        _ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=8.0, read=8.0, write=5.0, pool=5.0),
            headers=_ua,
        ) as client:
            # HEAD primero — sin descargar cuerpo.
            try:
                r = await client.head(url)
                cl = r.headers.get("content-length", "")
                if cl.isdigit() and int(cl) > 0:
                    return int(cl)
            except Exception:
                pass
            # GET en streaming — leer solo cabeceras.
            try:
                async with client.stream("GET", url) as r:
                    cl = r.headers.get("content-length", "")
                    if cl.isdigit() and int(cl) > 0:
                        return int(cl)
            except Exception:
                pass
    except Exception:
        pass
    return 0


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
        # ── Descarga de carpeta ─────────────────────────────────────────────
        if is_google_drive_folder_url(url):
            folder_id = extract_drive_folder_id(url)
            folder_url = f"https://drive.google.com/drive/folders/{folder_id}" if folder_id else url
            # Pasar download_workspace como output: gdown crea un subdirectorio
            # con el nombre real de la carpeta de Google Drive dentro de él.

            def _download_folder() -> list[str] | None:
                return gdown.download_folder(
                    url=folder_url,
                    output=str(download_workspace),
                    quiet=True,
                    remaining_ok=True,
                )

            try:
                downloaded_files: list[str] | None = await asyncio.to_thread(_download_folder)
            except Exception as _gdown_exc:
                _em = str(_gdown_exc).lower()
                if "only the owner" in _em or "editors can download" in _em:
                    raise RuntimeError(
                        "No se puede descargar esta carpeta de Google Drive. "
                        "El propietario ha restringido las descargas (la carpeta no es publica). "
                        "Pide al propietario que cambie el acceso a 'Cualquier persona con el enlace'."
                    ) from None
                if "quota" in _em or "exceeded" in _em:
                    raise RuntimeError(
                        "Se ha superado la cuota de descarga de Google Drive. "
                        "Intenta de nuevo mas tarde o usa un enlace alternativo."
                    ) from None
                raise RuntimeError(f"Error al descargar la carpeta de Google Drive: {_gdown_exc}") from _gdown_exc
            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")

            # Detectar la subcarpeta real a partir de las rutas devueltas por gdown.
            # gdown.download_folder devuelve la lista de archivos descargados con su
            # ruta absoluta; el primer componente relativo a download_workspace es el
            # nombre real de la carpeta de Google Drive.
            real_folder: Path = download_workspace
            if downloaded_files:
                for _fp in downloaded_files:
                    try:
                        _rel = Path(_fp).relative_to(download_workspace)
                        if _rel.parts:
                            _candidate = download_workspace / _rel.parts[0]
                            if _candidate.is_dir():
                                real_folder = _candidate
                                break
                    except (ValueError, IndexError):
                        continue

            # Fallback: buscar cualquier subdirectorio creado dentro del workspace.
            if real_folder == download_workspace:
                _subdirs = [d for d in download_workspace.iterdir() if d.is_dir()]
                if _subdirs:
                    real_folder = max(
                        _subdirs,
                        key=lambda d: sum(f.stat().st_size for f in d.rglob("*") if f.is_file()),
                    )

            all_files = [f for f in real_folder.rglob("*") if f.is_file() and not _is_html_error_page(f)]
            if not all_files:
                raise RuntimeError(
                    "No se pudieron descargar archivos de la carpeta de Google Drive. "
                    "Verifica que la carpeta sea publica y no este vacia."
                )

            total_size = sum(int(f.stat().st_size) for f in all_files)
            if on_progress:
                try:
                    await on_progress(total_size)
                except Exception:
                    pass

            return WebDownloadResult(source_url=url, local_path=real_folder, is_directory=True)

        # ── Descarga de archivo ─────────────────────────────────────────────
        file_id = extract_drive_file_id(url)

        # Tomar snapshot antes de descargar para detectar el archivo nuevo sin
        # depender del path retornado por gdown (que puede apuntar a un directorio
        # o a una ruta incorrecta cuando el output ya existe como carpeta).
        try:
            snapshots_before: set[Path] = {f for f in download_workspace.rglob("*") if f.is_file()}
        except Exception:
            snapshots_before = set()

        def _download_file() -> str | None:
            if file_id:
                # Usar el parámetro 'id' es más fiable que depender de fuzzy parsing.
                return gdown.download(
                    id=file_id,
                    output=str(download_workspace) + os.sep,
                    quiet=True,
                )
            return gdown.download(
                url=url,
                output=str(download_workspace) + os.sep,
                quiet=True,
                fuzzy=True,
            )

        try:
            gdown_returned_path = await asyncio.to_thread(_download_file)
        except Exception as _gdown_exc:
            _em = str(_gdown_exc).lower()
            if "only the owner" in _em or "editors can download" in _em:
                raise RuntimeError(
                    "No se puede descargar este archivo de Google Drive. "
                    "El propietario ha restringido las descargas (el archivo no es publico). "
                    "Pide al propietario que cambie el acceso a 'Cualquier persona con el enlace'."
                ) from None
            if "quota" in _em or "exceeded" in _em:
                raise RuntimeError(
                    "Se ha superado la cuota de descarga de Google Drive. "
                    "Intenta de nuevo mas tarde o usa un enlace alternativo."
                ) from None
            raise RuntimeError(f"Error al descargar el archivo de Google Drive: {_gdown_exc}") from _gdown_exc
        if cancel_requested and cancel_requested():
            raise RuntimeError("Operacion cancelada por el usuario")

        # Localizar el archivo descargado:
        # 1. Intentar la ruta que gdown reportó (puede ser incorrecta).
        # 2. Buscar archivos nuevos aparecidos en el workspace.
        local_file: Path | None = None

        if gdown_returned_path:
            candidate = Path(gdown_returned_path)
            if candidate.is_file() and not _is_html_error_page(candidate):
                local_file = candidate

        if local_file is None:
            local_file = _find_downloaded_file(download_workspace, snapshots_before)

        if local_file is None:
            # Comprobar si gdown dejó un HTML de error en su lugar
            any_new = {f for f in download_workspace.rglob("*") if f.is_file()} - snapshots_before
            if any_new and all(_is_html_error_page(f) for f in any_new):
                raise RuntimeError(
                    "Google Drive devolvio una pagina de error o confirmacion. "
                    "El archivo puede requerir permisos, estar restringido o necesitar aceptar un aviso de virus. "
                    "Asegurate de que el enlace es publico (cualquier persona con el enlace puede verlo)."
                )
            raise RuntimeError(
                "No se pudo descargar el archivo de Google Drive. "
                "Verifica que el enlace sea publico y que el archivo no haya sido eliminado."
            )

        if _is_html_error_page(local_file):
            try:
                local_file.unlink(missing_ok=True)
            except Exception:
                pass
            raise RuntimeError(
                "Google Drive devolvio una pagina de error en lugar del archivo. "
                "El enlace puede no ser publico o el archivo puede requerir autenticacion."
            )

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
