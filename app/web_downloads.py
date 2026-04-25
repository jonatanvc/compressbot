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
    total_bytes: int = 0


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


async def _download_gdrive_httpx(
    file_id: str,
    dest_dir: Path,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> Path:
    """
    Alternativa a gdown: descarga via drive.usercontent.google.com.
    Se usa cuando gdown falla por rate-limit.
    """
    import httpx  # type: ignore[import-not-found]

    _ua = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    # drive.usercontent.google.com no aplica el rate-limit de la UI web
    dl_url = (
        f"https://drive.usercontent.google.com/download"
        f"?id={file_id}&export=download&authuser=0&confirm=t"
    )

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(connect=15.0, read=600.0, write=30.0, pool=5.0),
        headers=_ua,
    ) as client:
        async with client.stream("GET", dl_url) as resp:
            resp.raise_for_status()

            # Detectar si devolvió una página HTML (error/confirmación)
            ct = resp.headers.get("content-type", "").lower()
            if "text/html" in ct:
                raise RuntimeError(
                    "Google Drive devolvio una pagina HTML en lugar del archivo. "
                    "El archivo puede requerir autenticacion o no ser publico."
                )

            # Nombre del archivo desde Content-Disposition
            cd = resp.headers.get("content-disposition", "")
            filename = "gdrive_file"
            if 'filename="' in cd:
                _fn = cd.split('filename="', 1)[1].split('"', 1)[0].strip()
                if _fn:
                    filename = _fn
            elif "filename*=utf-8''" in cd.lower():
                _fn = cd.lower().split("filename*=utf-8''", 1)[1].split(";", 1)[0].strip()
                if _fn:
                    filename = _fn

            dest_path = dest_dir / re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", filename)
            downloaded = 0

            with dest_path.open("wb") as fh:
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    if cancel_requested and cancel_requested():
                        raise RuntimeError("Operacion cancelada por el usuario")
                    fh.write(chunk)
                    downloaded += len(chunk)
                    if on_progress:
                        try:
                            await on_progress(downloaded)
                        except Exception:
                            pass

    if not dest_path.exists() or dest_path.stat().st_size == 0:
        raise RuntimeError("La descarga directa de Google Drive resulto en un archivo vacio.")

    if _is_html_error_page(dest_path):
        dest_path.unlink(missing_ok=True)
        raise RuntimeError(
            "Google Drive devolvio una pagina de error en lugar del archivo. "
            "Verifica que el enlace sea publico."
        )

    return dest_path


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

            _gdown_folder_exc: Exception | None = None
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
                # Para rate-limit y otros errores: guardar excepcion y continuar
                # con los archivos que gdown pudo descargar antes de fallar.
                _gdown_folder_exc = _gdown_exc
                downloaded_files = None
            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")

            # Detectar la carpeta real descargada por gdown.
            # gdown puede crear:
            #   a) download_workspace/NombreCarpeta/...  → usar NombreCarpeta
            #   b) download_workspace/Sub1/..., Sub2/... → usar download_workspace
            #      (los archivos quedan directamente en el workspace sin carpeta raíz)
            real_folder: Path = download_workspace
            if downloaded_files:
                top_level_items: set[Path] = set()
                for _fp in downloaded_files:
                    try:
                        _rel = Path(_fp).relative_to(download_workspace)
                        if _rel.parts:
                            top_level_items.add(download_workspace / _rel.parts[0])
                    except (ValueError, IndexError):
                        continue

                # Solo hay UNA carpeta de primer nivel → esa es la raíz real
                if len(top_level_items) == 1:
                    _candidate = next(iter(top_level_items))
                    if _candidate.is_dir():
                        real_folder = _candidate
                # Más de una → los archivos/carpetas están directamente en download_workspace

            # Fallback: buscar subdirectorios si downloaded_files estaba vacío
            if real_folder == download_workspace:
                _subdirs = [d for d in download_workspace.iterdir() if d.is_dir()]
                if len(_subdirs) == 1:
                    real_folder = _subdirs[0]

            all_files = [f for f in real_folder.rglob("*") if f.is_file() and not _is_html_error_page(f)]
            if not all_files:
                # No hay archivos — ahora sí es un error real
                if _gdown_folder_exc is not None:
                    _em = str(_gdown_folder_exc).lower()
                    if (
                        "cannot retrieve the public link" in _em
                        or "many accesses" in _em
                        or "have had many" in _em
                        or "too many" in _em
                    ):
                        raise RuntimeError(
                            "Google Drive ha bloqueado temporalmente la descarga por demasiados accesos recientes. "
                            "Espera unos minutos e intentalo de nuevo."
                        ) from None
                    raise RuntimeError(
                        f"Error al descargar la carpeta de Google Drive: {_gdown_folder_exc}"
                    ) from _gdown_folder_exc
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
            _is_rate_limited = (
                "cannot retrieve the public link" in _em
                or "many accesses" in _em
                or "have had many" in _em
                or "too many" in _em
            )
            if _is_rate_limited and file_id:
                # Fallback: descarga directa via drive.usercontent.google.com
                try:
                    local_file = await _download_gdrive_httpx(
                        file_id, download_workspace, on_progress, cancel_requested
                    )
                    if on_progress:
                        try:
                            await on_progress(int(local_file.stat().st_size))
                        except Exception:
                            pass
                    return WebDownloadResult(
                        source_url=url, local_path=local_file, is_directory=False
                    )
                except Exception:
                    raise RuntimeError(
                        "Google Drive ha bloqueado temporalmente la descarga por demasiados accesos recientes. "
                        "Espera unos minutos e intentalo de nuevo."
                    ) from None
            if _is_rate_limited:
                raise RuntimeError(
                    "Google Drive ha bloqueado temporalmente la descarga por demasiados accesos recientes. "
                    "Espera unos minutos e intentalo de nuevo."
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


# ══════════════════════════════════════════════════════════════════════════════
# Mega.nz
# ══════════════════════════════════════════════════════════════════════════════

_MEGA_HOST_MARKERS = ("mega.nz", "mega.co.nz")


def _is_mega_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.netloc or "").lower()
    return any(marker in host for marker in _MEGA_HOST_MARKERS)


def _is_mega_folder_url(url: str) -> bool:
    """Devuelve True si la URL apunta a una carpeta de Mega."""
    lower = url.lower()
    return "/folder/" in lower or "/#f!" in lower


def extract_mega_url(text: str) -> str | None:
    """Devuelve el primer enlace de Mega encontrado en *text*, o None."""
    raw = (text or "").strip()
    if not raw:
        return None
    for match in _URL_RE.finditer(raw):
        url = match.group(0).rstrip(".,;)")
        if _is_mega_url(url):
            return url
    return None


def _classify_mega_error(msg: str) -> str | None:
    """Convierte mensajes de error de mega.py en texto amigable en español."""
    m = msg.lower()
    if "not available" in m or "not found" in m or "invalid" in m or "bad node" in m:
        return (
            "No se pudo acceder al enlace de Mega. "
            "El archivo puede haber sido eliminado o el enlace no es valido."
        )
    if "quota" in m or "bandwidth" in m or "egress" in m or "transfer limit" in m:
        return (
            "Se ha superado la cuota de transferencia de Mega. "
            "Intenta de nuevo mas tarde."
        )
    if "decryption" in m or "key" in m or "aes" in m:
        return (
            "No se pudo desencriptar el archivo de Mega. "
            "Verifica que el enlace incluya la clave de descifrado (#...)."
        )
    return None


async def download_mega(
    url: str,
    output_root: Path,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> WebDownloadResult:
    """Descarga un archivo o carpeta pública de Mega.nz."""
    if not _is_mega_url(url):
        raise ValueError("El enlace no corresponde a Mega")

    try:
        import asyncio as _asyncio_check
        if not hasattr(_asyncio_check, "coroutine"):
            def _noop_coroutine_check(fn):  # type: ignore[misc]
                return fn
            _asyncio_check.coroutine = _noop_coroutine_check  # type: ignore[attr-defined]
        import mega as _mega_module  # noqa: F401  -- solo verificar que el paquete existe
    except (ImportError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            "Falta la dependencia 'mega.py'. Instala requirements.txt actualizado para habilitar descargas de Mega."
        ) from exc

    output_root.mkdir(parents=True, exist_ok=True)

    if cancel_requested and cancel_requested():
        raise RuntimeError("Operacion cancelada por el usuario")

    download_workspace = output_root / f"mega_{uuid.uuid4().hex[:10]}"
    download_workspace.mkdir(parents=True, exist_ok=True)

    is_folder = _is_mega_folder_url(url)
    poll_task = asyncio.create_task(_poll_downloaded_bytes(download_workspace, on_progress, cancel_requested))

    try:
        def _do_download() -> str | None:
            # tenacity==5.x (dependencia de mega.py) usa @asyncio.coroutine que fue
            # eliminado en Python 3.11. Añadimos un shim antes de importar mega.
            import asyncio as _asyncio
            if not hasattr(_asyncio, "coroutine"):
                def _noop_coroutine(fn):  # type: ignore[misc]
                    return fn
                _asyncio.coroutine = _noop_coroutine  # type: ignore[attr-defined]

            from mega import Mega as _Mega  # type: ignore[import-not-found]
            mega_client = _Mega()
            m = mega_client.login()  # login anonimo para enlaces publicos
            dest = m.download_url(url, str(download_workspace))
            return str(dest) if dest else None

        try:
            returned_path_str = await asyncio.to_thread(_do_download)
        except Exception as _mega_exc:
            friendly = _classify_mega_error(str(_mega_exc))
            if friendly:
                raise RuntimeError(friendly) from None
            raise RuntimeError(f"Error al descargar desde Mega: {_mega_exc}") from _mega_exc

        if cancel_requested and cancel_requested():
            raise RuntimeError("Operacion cancelada por el usuario")

        if is_folder:
            # mega.py crea un subdirectorio con el nombre real de la carpeta.
            created_subdirs = [d for d in download_workspace.iterdir() if d.is_dir()]
            if created_subdirs:
                real_folder = max(
                    created_subdirs,
                    key=lambda d: sum(f.stat().st_size for f in d.rglob("*") if f.is_file()),
                )
            else:
                real_folder = download_workspace

            all_files = [f for f in real_folder.rglob("*") if f.is_file()]
            if not all_files:
                raise RuntimeError(
                    "No se encontraron archivos en la carpeta de Mega. "
                    "Verifica que el enlace sea publico y que la carpeta no este vacia."
                )

            total_size = sum(int(f.stat().st_size) for f in all_files)
            if on_progress:
                try:
                    await on_progress(total_size)
                except Exception:
                    pass

            return WebDownloadResult(source_url=url, local_path=real_folder, is_directory=True)

        # ── Archivo ────────────────────────────────────────────────────────
        local_file: Path | None = None
        if returned_path_str:
            candidate = Path(returned_path_str)
            if candidate.is_file():
                local_file = candidate

        if local_file is None:
            # Buscar el archivo más grande que apareció en el workspace.
            all_now = [f for f in download_workspace.rglob("*") if f.is_file()]
            if all_now:
                local_file = max(all_now, key=lambda f: f.stat().st_size)

        if local_file is None:
            raise RuntimeError(
                "No se pudo descargar el archivo de Mega. "
                "Verifica que el enlace sea valido y publico."
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

# Terabox
_TERABOX_HOST_MARKERS = (
    "terabox.com", "teraboxapp.com", "1024terabox.com",
    "teraboxlink.com", "terasharelink.com", "4funbox.com",
    "mirrobox.com", "nephobox.com", "freeterabox.com",
    "terabox.app", "terabox.fun",
)


def _is_terabox_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.netloc or "").lower().lstrip("www.")
    return any(host == m or host.endswith("." + m) for m in _TERABOX_HOST_MARKERS)


def extract_terabox_url(text: str) -> str | None:
    """Devuelve el primer enlace de Terabox encontrado en *text*, o None."""
    raw = (text or "").strip()
    if not raw:
        return None
    for match in _URL_RE.finditer(raw):
        url = match.group(0).rstrip(".,;)")
        if _is_terabox_url(url):
            return url
    return None


_TERABOX_DIRECT_EXTS = (
    ".exe", ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz",
    ".mp4", ".mkv", ".avi", ".mov", ".mp3", ".flac", ".aac",
    ".pdf", ".apk", ".iso", ".dmg", ".msi", ".deb", ".rpm",
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
)


def _is_terabox_direct_url(url: str) -> bool:
    """Devuelve True si la URL ya es un enlace CDN directo (no una pagina de comparticion)."""
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        query = parsed.query.lower()
        has_file_ext = any(path.endswith(ext) for ext in _TERABOX_DIRECT_EXTS)
        has_sign = "sign=" in query
        return has_file_ext or has_sign
    except Exception:
        return False


_TERABOX_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


async def _try_terabox_native_api(share_url: str) -> tuple[str, str] | None:
    """
    Resuelve via la API interna de Terabox sin depender de ningun servicio externo.
    Llama directamente a los endpoints del dominio del enlace compartido.
    Funciona aunque los resolvers externos (ytshorts, teraboxdl) tengan problemas de DNS.
    """
    import httpx  # type: ignore[import-not-found]
    import time as _time

    parsed = urlparse(share_url)
    host = parsed.netloc.lower()

    # Extraer surl del path /s/XXXXXX
    surl_m = re.search(r"/s/([A-Za-z0-9_-]+)", parsed.path)
    if not surl_m:
        return None
    surl = surl_m.group(1)

    ua_headers = {
        "User-Agent": _TERABOX_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(connect=15.0, read=30.0, write=10.0, pool=5.0),
        headers=ua_headers,
    ) as client:
        # Paso 1: cargar la pagina del enlace para obtener cookies y tokens
        try:
            page_resp = await client.get(share_url)
            final_host = urlparse(str(page_resp.url)).netloc or host
            base = f"https://{final_host}"
        except Exception:
            return None

        # Extraer jsToken del JS embebido en la pagina
        js_token = ""
        for _pat in [
            r'["\']jsToken["\']\s*[,:]\s*["\']([^"\']{10,})["\']',
            r'locals\.mset\([^)]*["\']jsToken["\']\s*:\s*["\']([^"\']+)["\']',
            r'"bdstoken"\s*:\s*"([^"]+)"',
        ]:
            _m = re.search(_pat, page_resp.text)
            if _m:
                js_token = _m.group(1)
                break

        # Extraer sign
        sign = ""
        _sm = re.search(
            r'["\']sign["\']\s*[,:]\s*["\']([A-Za-z0-9%+/=]{10,})["\']',
            page_resp.text,
        )
        if _sm:
            sign = _sm.group(1)

        timestamp = str(int(_time.time()))

        # Paso 2: obtener info del archivo via shorturlinfo API
        try:
            info_resp = await client.get(
                f"{base}/api/shorturlinfo",
                params={"app_id": "250528", "shorturl": f"/s/{surl}", "root": "1"},
                headers={"Referer": share_url},
            )
            info = info_resp.json()
        except Exception:
            return None

        if info.get("errno") != 0 or not info.get("list"):
            return None

        item = info["list"][0]
        fs_id = item.get("fs_id")
        filename: str = item.get("server_filename", "terabox_file")
        if not fs_id:
            return None

        # Paso 3: obtener enlace de descarga directa
        dl_params: dict = {
            "app_id": "250528",
            "channel": "dubox",
            "clienttype": "0",
            "jsToken": js_token,
            "fid_list": f"[{fs_id}]",
        }
        if sign:
            dl_params["sign"] = sign
            dl_params["timestamp"] = timestamp

        try:
            dl_resp = await client.get(
                f"{base}/api/download",
                params=dl_params,
                headers={"Referer": share_url},
            )
            dl_data = dl_resp.json()
        except Exception:
            return None

        if dl_data.get("errno") != 0:
            return None

        dlink: str = dl_data.get("dlink", "")
        if not dlink and dl_data.get("list"):
            dlink = (dl_data["list"][0] or {}).get("dlink", "")

        return (dlink, filename) if dlink else None


async def _try_teraboxdl_site(share_url: str) -> tuple[str, str] | None:
    """
    Intenta resolver via teraboxdl.site/api/proxy.
    Devuelve (download_url, filename) o None si no tiene exito.
    """
    import httpx  # type: ignore[import-not-found]

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(connect=12.0, read=25.0, write=10.0, pool=5.0),
        headers={"User-Agent": _TERABOX_UA, "Referer": "https://teraboxdl.site/"},
    ) as client:
        resp = await client.post(
            "https://teraboxdl.site/api/proxy", json={"url": share_url}
        )
        resp.raise_for_status()
        data = resp.json()

    if data.get("errno") != 0 or not data.get("list"):
        return None
    item = data["list"][0]
    filename: str = item.get("server_filename", "terabox_file")
    dl_url: str = item.get("direct_link", "")
    return (dl_url, filename) if dl_url else None


async def _try_ytshorts_api(share_url: str) -> tuple[str, str] | None:
    """
    Intenta resolver via ytshorts.savetube.me/api/v1/terabox-downloader.
    Devuelve (download_url, filename) o None si no tiene exito.
    """
    import httpx  # type: ignore[import-not-found]

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(connect=15.0, read=30.0, write=10.0, pool=5.0),
        headers={"User-Agent": _TERABOX_UA, "Referer": "https://www.terabox.com/"},
    ) as client:
        resp = await client.post(
            "https://ytshorts.savetube.me/api/v1/terabox-downloader",
            json={"url": share_url},
        )
        resp.raise_for_status()
        data = resp.json()

    try:
        item = data["response"][0]
        filename: str = item.get("title", "terabox_file")
        resolutions: dict = item.get("resolutions", {})
        dl_url: str = (
            resolutions.get("Fast Download")
            or resolutions.get("HD Video")
            or resolutions.get("SD Video")
            or next(iter(resolutions.values()), "")
        )
        return (dl_url, filename) if dl_url else None
    except (KeyError, IndexError, TypeError):
        return None


async def _terabox_resolve_direct_url(share_url: str) -> tuple[str, str]:
    """
    Resuelve un enlace compartido de Terabox a (download_url, filename).
    Prueba multiples APIs en orden hasta obtener resultado.
    """
    errors: list[str] = []

    # API 1 (nativa): llama directamente a los endpoints de Terabox — no depende de DNS externo
    try:
        result = await _try_terabox_native_api(share_url)
        if result:
            return result
    except Exception as exc:
        errors.append(f"native: {exc}")

    # API 2: teraboxdl.site
    try:
        result = await _try_teraboxdl_site(share_url)
        if result:
            return result
    except Exception as exc:
        errors.append(f"teraboxdl.site: {exc}")

    # API 3: ytshorts.savetube.me
    try:
        result = await _try_ytshorts_api(share_url)
        if result:
            return result
    except Exception as exc:
        errors.append(f"ytshorts: {exc}")

    raise RuntimeError(
        "No se pudo resolver el enlace de Terabox con ninguna API disponible. "
        f"Detalle: {'; '.join(errors) or 'sin respuesta'}"
    )


async def download_terabox(
    url: str,
    output_root: Path,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> WebDownloadResult:
    """Descarga un archivo publico de Terabox resolviendo el enlace compartido."""
    if not _is_terabox_url(url):
        raise ValueError("El enlace no corresponde a Terabox")

    try:
        import httpx  # type: ignore[import-not-found]
    except (ImportError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            "Falta la dependencia 'httpx'. Instala requirements.txt actualizado."
        ) from exc

    output_root.mkdir(parents=True, exist_ok=True)

    if cancel_requested and cancel_requested():
        raise RuntimeError("Operacion cancelada por el usuario")

    # Si la URL ya es un enlace CDN directo, no pasar por la API de resolucion
    if _is_terabox_direct_url(url):
        direct_url = url
        filename = Path(urlparse(url).path).name or "terabox_file"
    else:
        try:
            direct_url, filename = await _terabox_resolve_direct_url(url)
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Error al resolver el enlace de Terabox: {exc}") from exc

    safe_filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", filename).strip() or "terabox_file"
    dest_path = output_root / safe_filename

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.terabox.com/",
    }

    downloaded = 0
    total_size = 0

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=300.0, write=30.0, pool=5.0),
            headers=headers,
        ) as client:
            async with client.stream("GET", direct_url) as resp:
                resp.raise_for_status()
                cl = resp.headers.get("content-length", "")
                if cl.isdigit() and int(cl) > 0:
                    total_size = int(cl)

                with dest_path.open("wb") as fh:
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        if cancel_requested and cancel_requested():
                            raise RuntimeError("Operacion cancelada por el usuario")
                        fh.write(chunk)
                        downloaded += len(chunk)
                        if on_progress:
                            try:
                                await on_progress(downloaded)
                            except Exception:
                                pass
    except RuntimeError:
        raise
    except Exception as exc:
        _em = str(exc).lower()
        if "403" in _em or "forbidden" in _em or "401" in _em or "unauthorized" in _em:
            raise RuntimeError(
                "Acceso denegado al descargar desde Terabox. "
                "El enlace puede haber expirado o el archivo no es publico."
            ) from None
        if "quota" in _em or "bandwidth" in _em:
            raise RuntimeError(
                "Se ha superado la cuota de Terabox. Intenta mas tarde."
            ) from None
        raise RuntimeError(f"Error al descargar desde Terabox: {exc}") from exc

    if not dest_path.exists() or dest_path.stat().st_size == 0:
        raise RuntimeError("La descarga de Terabox resulto en un archivo vacio.")

    final_size = int(dest_path.stat().st_size)
    if on_progress:
        try:
            await on_progress(final_size)
        except Exception:
            pass

    return WebDownloadResult(
        source_url=url,
        local_path=dest_path,
        is_directory=False,
        total_bytes=final_size,
    )
