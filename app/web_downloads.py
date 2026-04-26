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


def _extract_gdrive_confirm_url(html: str, file_id: str) -> str | None:
    """
    Parsea la pagina HTML de confirmacion de Google Drive y extrae
    la URL de descarga real (con token confirm= o uuid=).
    Admite multiples formatos de la pagina de confirmacion de Google.
    """
    # Patron 1: enlace /uc?export=download&confirm=TOKEN&id=...
    m = re.search(
        r'href="(/uc\?[^"]*export=download[^"]*confirm=[^"&]+[^"]*)"', html
    )
    if m:
        return "https://drive.google.com" + m.group(1).replace("&amp;", "&")

    # Patron 2: enlace completo drive.usercontent.google.com con confirm
    m = re.search(
        r'href="(https://drive\.usercontent\.google\.com/download[^"]*confirm=[^"]+[^"]*)"',
        html,
    )
    if m:
        return m.group(1).replace("&amp;", "&")

    # Patron 3: input hidden name="confirm"
    m = re.search(r'name="confirm"\s+value="([^"]+)"', html)
    if m:
        confirm = m.group(1)
        return (
            f"https://drive.usercontent.google.com/download"
            f"?id={file_id}&export=download&confirm={confirm}&authuser=0"
        )

    # Patron 4: uuid embebido en el JSON del script
    m = re.search(r'"uuid"\s*:\s*"([^"]+)"', html)
    if m:
        uuid_val = m.group(1)
        return (
            f"https://drive.usercontent.google.com/download"
            f"?id={file_id}&export=download&confirm=t&uuid={uuid_val}&authuser=0"
        )

    # Patron 5: action del form de descarga
    m = re.search(r'action="(https?://[^"]*(?:drive\.google\.com|usercontent\.google\.com)[^"]*)"', html)
    if m:
        form_action = m.group(1).replace("&amp;", "&")
        if "export=download" in form_action or "id=" in form_action:
            return form_action

    # Patron 6: id="uc-download-link" (nueva UI de Google Drive)
    m = re.search(r'id=["\']uc-download-link["\'][^>]*href=["\']([^"\']+)["\']', html)
    if not m:
        m = re.search(r'href=["\']([^"\']+)["\'][^>]*id=["\']uc-download-link["\']', html)
    if m:
        url = m.group(1).replace("&amp;", "&")
        if url.startswith("/"):
            url = "https://drive.google.com" + url
        return url

    # Patron 7: cualquier href con confirm= (fallback generico)
    m = re.search(r'href=["\']([^"\']*confirm=[^"\']+)["\']', html)
    if m:
        url = m.group(1).replace("&amp;", "&")
        if url.startswith("/"):
            url = "https://drive.google.com" + url
        return url

    # Patron 8: window.location = "URL"
    m = re.search(r'window\.location\s*=\s*["\']([^"\']+)["\']', html)
    if m:
        url = m.group(1)
        if "download" in url or "export" in url or file_id in url:
            return url

    # Patron 9: downloadUrl en JSON embebido (escape unicode)
    m = re.search(r'"downloadUrl"\s*:\s*"([^"]+)"', html)
    if m:
        url = m.group(1).replace("\\u003d", "=").replace("\\u0026", "&")
        if file_id in url or "download" in url:
            return url

    # Patron 10: href con drive.usercontent.google.com y el file_id
    m = re.search(
        r'href=["\']([^"\']*drive\.usercontent\.google\.com[^"\']*' + re.escape(file_id) + r'[^"\']*)["\']',
        html,
    )
    if m:
        return m.group(1).replace("&amp;", "&")

    return None


async def _download_gdrive_httpx(
    file_id: str,
    dest_dir: Path,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> Path:
    """
    Descarga via drive.usercontent.google.com / drive.google.com.
    Maneja correctamente la pagina de confirmacion de Google (pagina HTML
    que aparece cuando hay demasiadas descargas o archivos grandes).
    Prueba varios endpoints en orden.
    """
    import httpx  # type: ignore[import-not-found]

    _ua = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    _CANDIDATES = [
        (
            f"https://drive.usercontent.google.com/download"
            f"?id={file_id}&export=download&authuser=0&confirm=t"
        ),
        f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t",
        f"https://docs.google.com/uc?export=download&id={file_id}&confirm=t",
    ]

    last_exc: Exception | None = None

    async def _write_stream(resp: "httpx.Response") -> Path:
        """Escribe un response streaming en disco y devuelve el Path."""
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
        return dest_path

    for initial_url in _CANDIDATES:
        dest_path: Path | None = None
        try:
            # Un unico cliente/sesion para todo el proceso — evita el 404 que
            # ocurria cuando la Fase 1 (GET de prueba) consumia la URL one-time
            # de Google y la Fase 2 (streaming) recibia 404.
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(connect=15.0, read=600.0, write=30.0, pool=5.0),
                headers=_ua,
            ) as client:
                async with client.stream("GET", initial_url) as resp:
                    resp.raise_for_status()
                    ct = resp.headers.get("content-type", "").lower()

                    if "text/html" in ct:
                        # Leer HTML en memoria sin cerrar la sesion
                        html = (await resp.aread()).decode("utf-8", errors="replace")
                        resolved = _extract_gdrive_confirm_url(html, file_id)
                        if not resolved:
                            # Bypass directo con confirm=t
                            resolved = (
                                f"https://drive.usercontent.google.com/download"
                                f"?id={file_id}&export=download&confirm=t&authuser=0"
                            )
                        # Segunda request dentro del mismo cliente/sesion
                        async with client.stream("GET", resolved) as resp2:
                            resp2.raise_for_status()
                            ct2 = resp2.headers.get("content-type", "").lower()
                            if "text/html" in ct2:
                                last_exc = RuntimeError(
                                    "Google Drive devolvio HTML en la descarga definitiva. "
                                    "El archivo puede no ser publico."
                                )
                                continue
                            dest_path = await _write_stream(resp2)
                    else:
                        dest_path = await _write_stream(resp)

            if dest_path is None or not dest_path.exists() or dest_path.stat().st_size == 0:
                last_exc = RuntimeError("La descarga resulto en un archivo vacio.")
                continue

            if _is_html_error_page(dest_path):
                dest_path.unlink(missing_ok=True)
                last_exc = RuntimeError(
                    "Google Drive devolvio una pagina de error en lugar del archivo. "
                    "Verifica que el enlace sea publico."
                )
                continue

            # Exito
            return dest_path

        except RuntimeError:
            raise
        except Exception as _exc:
            last_exc = _exc
            continue

    raise last_exc or RuntimeError(
        "No se pudo descargar el archivo de Google Drive con ninguno de los endpoints disponibles."
    )



async def _get_gdrive_folder_name(folder_id: str) -> str | None:
    """
    Intenta obtener el nombre real de una carpeta de Google Drive
    parseando el <title> de la pagina embeddedfolderview.
    Devuelve el nombre saneado o None si no se puede obtener.
    """
    import httpx  # type: ignore[import-not-found]

    try:
        embed_url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=10.0, read=15.0, write=5.0, pool=5.0),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            },
        ) as client:
            resp = await client.get(embed_url)
        html = resp.text
        m = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
        if m:
            raw = m.group(1).strip()
            # Quitar sufijo " - Google Drive" si aparece
            raw = re.sub(r"\s*[-\u2013\u2014]\s*Google Drive\s*$", "", raw).strip()
            if raw:
                # Sanear caracteres invalidos en nombres de fichero
                safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", raw).strip(" .")
                if safe:
                    return safe
    except Exception:
        pass
    return None


async def _download_gdrive_folder_usercontent(
    folder_id: str,
    dest_dir: Path,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[Path]:
    """
    Fallback para carpetas de Google Drive cuando gdown falla.
    Enumera archivos via embeddedfolderview y descarga cada uno
    via drive.usercontent.google.com, que no aplica rate-limit.
    """
    import httpx  # type: ignore[import-not-found]

    _ua_hdr = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    downloaded: list[Path] = []

    async def _list_folder(fid: str) -> list[tuple[str, str, bool]]:
        """Devuelve [(id, nombre, es_carpeta), ...] para el folder dado."""
        embed_url = f"https://drive.google.com/embeddedfolderview?id={fid}#list"
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=30.0, write=10.0, pool=5.0),
            headers=_ua_hdr,
        ) as client:
            resp = await client.get(embed_url)
        html = resp.text
        items: list[tuple[str, str, bool]] = []
        seen: set[str] = set()

        # Estructura real de embeddedfolderview:
        # <div class="flip-entry" id="entry-ID" ...>
        #   <a href="https://drive.google.com/drive/folders/ID" ...>
        #     ...
        #     <div class="flip-entry-title">NOMBRE</div>
        #   </a>
        # </div>
        # El nombre NO está directamente entre >...</a>, está en un div anidado.
        # Extraemos: href del <a> + texto del <div class="flip-entry-title"> siguiente.

        # Primero: recoger todos los <a href=...> con su ID
        href_to_id: dict[str, tuple[str, bool]] = {}
        for m in re.finditer(
            r'<a\s[^>]*href="https://drive\.google\.com/(drive/folders|file/d)/([a-zA-Z0-9_-]{20,})[^"]*"',
            html,
        ):
            kind, item_id = m.group(1), m.group(2)
            is_folder = kind == "drive/folders"
            if item_id not in href_to_id:
                href_to_id[item_id] = (item_id, is_folder)

        # Ahora: recorrer los bloques flip-entry para asociar id con titulo
        for entry_m in re.finditer(
            r'<div[^>]+id="entry-([a-zA-Z0-9_-]{20,})"[^>]*>(.*?)</div>\s*<div[^>]+flip-entry-last',
            html, re.DOTALL,
        ):
            entry_id = entry_m.group(1)
            block = entry_m.group(2)
            title_m = re.search(r'flip-entry-title[^>]*>([^<]+)<', block)
            title = title_m.group(1).strip() if title_m else entry_id
            href_m = re.search(
                r'href="https://drive\.google\.com/(drive/folders|file/d)/([a-zA-Z0-9_-]{20,})',
                block,
            )
            if href_m:
                kind, item_id = href_m.group(1), href_m.group(2)
                is_folder = kind == "drive/folders"
            else:
                # Usar el entry_id si no hay href (puede ser carpeta sin enlace explícito)
                item_id = entry_id
                is_folder = entry_id in {k for k, (_, f) in href_to_id.items() if f}

            if item_id != fid and item_id not in seen:
                seen.add(item_id)
                items.append((item_id, title or item_id, is_folder))

        # Si el parseo de flip-entry no encontró nada, fallback: solo por href
        if not items:
            for item_id, (_, is_folder) in href_to_id.items():
                if item_id != fid and item_id not in seen:
                    seen.add(item_id)
                    items.append((item_id, item_id, is_folder))

        return items

    async def _process_folder(fid: str, dest: Path, depth: int = 0) -> None:
        if depth > 6 or (cancel_requested and cancel_requested()):
            return
        dest.mkdir(parents=True, exist_ok=True)
        try:
            items = await _list_folder(fid)
        except Exception:
            return
        for item_id, item_name, is_folder in items:
            if cancel_requested and cancel_requested():
                return
            safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", item_name).strip() or item_id
            if is_folder:
                await _process_folder(item_id, dest / safe, depth + 1)
            else:
                try:
                    p = await _download_gdrive_httpx(item_id, dest, on_progress, cancel_requested)
                    downloaded.append(p)
                except Exception:
                    pass  # saltar archivos que fallen individualmente

    await _process_folder(folder_id, dest_dir)
    return downloaded


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
            if not folder_id:
                raise RuntimeError("No se pudo extraer el ID de la carpeta de Google Drive")

            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")

            # Método primario: embeddedfolderview + drive.usercontent.google.com
            # Enumera TODAS las subcarpetas correctamente y descarga sin rate-limit.
            folder_name = await _get_gdrive_folder_name(folder_id) or folder_id
            safe_folder_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", folder_name).strip(" .") or "descarga_drive"
            real_folder: Path = download_workspace / safe_folder_name
            real_folder.mkdir(parents=True, exist_ok=True)

            _usercontent_files: list[Path] = []
            _usercontent_exc: Exception | None = None
            try:
                _usercontent_files = await _download_gdrive_folder_usercontent(
                    folder_id, real_folder, on_progress, cancel_requested
                )
            except Exception as _exc:
                _usercontent_exc = _exc

            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")

            all_files = [f for f in real_folder.rglob("*") if f.is_file() and not _is_html_error_page(f)]

            # Método de respaldo: gdown (puede descargar parcial, pero mejor que nada)
            if not all_files:
                folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

                def _download_folder() -> list[str] | None:
                    return gdown.download_folder(
                        url=folder_url,
                        output=str(download_workspace),
                        quiet=True,
                        remaining_ok=True,
                    )

                _gdown_folder_exc: Exception | None = None
                try:
                    _gdown_files: list[str] | None = await asyncio.to_thread(_download_folder)
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
                    _gdown_folder_exc = _gdown_exc
                    _gdown_files = None

                if cancel_requested and cancel_requested():
                    raise RuntimeError("Operacion cancelada por el usuario")

                # Detectar carpeta real descargada por gdown
                if _gdown_files:
                    top_level_items: set[Path] = set()
                    for _fp in _gdown_files:
                        try:
                            _rel = Path(_fp).relative_to(download_workspace)
                            if _rel.parts:
                                top_level_items.add(download_workspace / _rel.parts[0])
                        except (ValueError, IndexError):
                            continue
                    if len(top_level_items) == 1:
                        _candidate = next(iter(top_level_items))
                        if _candidate.is_dir():
                            real_folder = _candidate
                    else:
                        real_folder = download_workspace
                else:
                    _subdirs = [d for d in download_workspace.iterdir() if d.is_dir()]
                    if len(_subdirs) == 1:
                        real_folder = _subdirs[0]
                    else:
                        real_folder = download_workspace

                all_files = [f for f in real_folder.rglob("*") if f.is_file() and not _is_html_error_page(f)]

            if not all_files:
                exc_detail = str(_usercontent_exc) if _usercontent_exc else "sin respuesta"
                raise RuntimeError(
                    "No se pudieron descargar archivos de la carpeta de Google Drive. "
                    f"Verifica que la carpeta sea publica y no este vacia. Detalle: {exc_detail}"
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
                # Fallback: descarga directa via endpoints alternativos con parseo de confirmacion
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
                except RuntimeError:
                    raise
                except Exception as _httpx_exc:
                    raise RuntimeError(
                        "Google Drive ha limitado las descargas desde esta IP. "
                        "Intenta de nuevo en unos minutos. "
                        f"(Detalle: {_httpx_exc})"
                    ) from None
            if _is_rate_limited:
                if file_id:
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
                    except RuntimeError:
                        raise
                    except Exception:
                        pass
                raise RuntimeError(
                    "Google Drive ha limitado las descargas desde esta IP. "
                    "Intenta de nuevo en unos minutos."
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
            # Comprobar si gdown dejó un HTML de error en su lugar:
            # intentar el fallback httpx antes de rendirse.
            any_new = {f for f in download_workspace.rglob("*") if f.is_file()} - snapshots_before
            if (any_new and all(_is_html_error_page(f) for f in any_new)) or not any_new:
                if file_id:
                    try:
                        # Limpiar el HTML descargado por gdown
                        for _hf in any_new:
                            try:
                                _hf.unlink(missing_ok=True)
                            except Exception:
                                pass
                        local_file = await _download_gdrive_httpx(
                            file_id, download_workspace, on_progress, cancel_requested
                        )
                    except RuntimeError:
                        raise
                    except Exception:
                        pass
            if local_file is None:
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


async def _download_mega_megatools(url: str, download_workspace: Path) -> None:
    """Descarga un archivo o carpeta de Mega usando megadl (megatools CLI)."""
    import shutil as _shutil
    megadl = _shutil.which("megadl") or _shutil.which("megadl.exe")
    if not megadl:
        raise RuntimeError("megatools no está instalado (comando megadl no encontrado)")

    proc = await asyncio.create_subprocess_exec(
        megadl, "--path", str(download_workspace), url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        err_text = (stderr or b"").decode(errors="replace").strip()
        raise RuntimeError(f"megadl falló (código {proc.returncode}): {err_text}")


async def _download_mega_megapy(url: str, download_workspace: Path) -> str | None:
    """Descarga un archivo de Mega usando mega.py."""
    def _do() -> str | None:
        import asyncio as _asyncio
        if not hasattr(_asyncio, "coroutine"):
            def _noop(fn):  # type: ignore[misc]
                return fn
            _asyncio.coroutine = _noop  # type: ignore[attr-defined]
        from mega import Mega as _Mega  # type: ignore[import-not-found]
        mega_client = _Mega()
        m = mega_client.login()
        dest = m.download_url(url, str(download_workspace))
        return str(dest) if dest else None

    try:
        return await asyncio.to_thread(_do)
    except Exception as _exc:
        friendly = _classify_mega_error(str(_exc))
        if friendly:
            raise RuntimeError(friendly) from None
        raise RuntimeError(f"Error al descargar desde Mega: {_exc}") from _exc


async def download_mega(
    url: str,
    output_root: Path,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> WebDownloadResult:
    """Descarga un archivo o carpeta pública de Mega.nz."""
    if not _is_mega_url(url):
        raise ValueError("El enlace no corresponde a Mega")

    output_root.mkdir(parents=True, exist_ok=True)

    if cancel_requested and cancel_requested():
        raise RuntimeError("Operacion cancelada por el usuario")

    download_workspace = output_root / f"mega_{uuid.uuid4().hex[:10]}"
    download_workspace.mkdir(parents=True, exist_ok=True)

    is_folder = _is_mega_folder_url(url)
    poll_task = asyncio.create_task(_poll_downloaded_bytes(download_workspace, on_progress, cancel_requested))

    try:
        if is_folder:
            # Para carpetas, usamos megatools (megadl) que soporta /folder/ correctamente.
            # Si megatools no esta instalado, intentamos mega.py como fallback.
            try:
                await _download_mega_megatools(url, download_workspace)
            except RuntimeError as _mt_exc:
                _mt_msg = str(_mt_exc)
                if "no está instalado" in _mt_msg or "no encontrado" in _mt_msg:
                    # Fallback a mega.py (puede no soportar todas las carpetas)
                    try:
                        await _download_mega_megapy(url, download_workspace)
                    except Exception as _py_exc:
                        raise RuntimeError(
                            f"megatools no está disponible y mega.py también falló: {_py_exc}"
                        ) from _py_exc
                else:
                    raise RuntimeError(_mt_msg) from _mt_exc

            if cancel_requested and cancel_requested():
                raise RuntimeError("Operacion cancelada por el usuario")

            # megatools descarga al directorio destino directamente; buscar el subdir creado.
            created_subdirs = [d for d in download_workspace.iterdir() if d.is_dir()]
            if created_subdirs:
                real_folder = max(
                    created_subdirs,
                    key=lambda d: sum(f.stat().st_size for f in d.rglob("*") if f.is_file()),
                )
            else:
                # megadl puede colocar archivos directamente en el workspace sin subdirectorio
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

        # ── Archivo individual ─────────────────────────────────────────────
        # Intentar primero con megatools; si falla, usar mega.py.
        import shutil as _shutil
        if _shutil.which("megadl") or _shutil.which("megadl.exe"):
            try:
                await _download_mega_megatools(url, download_workspace)
                if cancel_requested and cancel_requested():
                    raise RuntimeError("Operacion cancelada por el usuario")
                all_files = [f for f in download_workspace.rglob("*") if f.is_file()]
                if all_files:
                    local_file = max(all_files, key=lambda f: f.stat().st_size)
                    if on_progress:
                        try:
                            await on_progress(int(local_file.stat().st_size))
                        except Exception:
                            pass
                    return WebDownloadResult(source_url=url, local_path=local_file, is_directory=False)
            except Exception:
                pass  # fallback a mega.py

        # Fallback: mega.py
        try:
            import mega as _mega_module  # noqa: F401
        except (ImportError, ModuleNotFoundError) as exc:
            raise RuntimeError(
                "Falta la dependencia 'mega.py' y megatools no está disponible. "
                "No se puede descargar desde Mega."
            ) from exc

        returned_path_str = await _download_mega_megapy(url, download_workspace)

        if cancel_requested and cancel_requested():
            raise RuntimeError("Operacion cancelada por el usuario")

        local_file: Path | None = None
        if returned_path_str:
            candidate = Path(returned_path_str)
            if candidate.is_file():
                local_file = candidate

        if local_file is None:
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
