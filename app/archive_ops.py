import asyncio
import re
import shutil
import subprocess
import zipfile
from pathlib import Path

import pyzipper
import rarfile

from .exceptions import InvalidPasswordError, Missing7zBackendError, MissingRarBackendError, PasswordRequiredError


def _get_7z_executable() -> str:
    exe = shutil.which("7z") or shutil.which("7z.exe") or shutil.which("7za") or shutil.which("7za.exe")
    if exe:
        return exe

    candidates = [
        Path(r"C:\Program Files\7-Zip\7z.exe"),
        Path(r"C:\Program Files (x86)\7-Zip\7z.exe"),
        Path(r"C:\Program Files\7-Zip\7za.exe"),
        Path(r"C:\Program Files (x86)\7-Zip\7za.exe"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    raise Missing7zBackendError(
        "No hay backend para 7z. Instala 7-Zip y verifica que 7z.exe exista en Program Files o en PATH."
    )


def extract_zip_with_pyzipper(archive_path: Path, output_dir: Path, password: str | None = None) -> None:
    with pyzipper.AESZipFile(archive_path, "r") as zf:
        encrypted = any(info.flag_bits & 0x1 for info in zf.infolist())
        if encrypted and not password:
            raise PasswordRequiredError("El ZIP requiere password")

        if password:
            zf.setpassword(password.encode("utf-8"))

        try:
            zf.extractall(path=output_dir)
        except RuntimeError as exc:
            text = str(exc).lower()
            if "password" in text or "bad password" in text:
                raise InvalidPasswordError("Password invalida") from exc
            raise


def extract_zip(archive_path: Path, output_dir: Path, password: str | None = None) -> None:
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            encrypted = any(info.flag_bits & 0x1 for info in zf.infolist())
            if encrypted and not password:
                raise PasswordRequiredError("El ZIP requiere password")

            pwd = password.encode("utf-8") if password else None
            try:
                zf.extractall(path=output_dir, pwd=pwd)
            except RuntimeError as exc:
                text = str(exc).lower()
                if "password" in text:
                    raise InvalidPasswordError("Password invalida") from exc
                raise
    except NotImplementedError:
        # Fallback para ZIP con cifrado/metodo no soportado por zipfile (p.ej. AES/Deflate64).
        extract_zip_with_pyzipper(archive_path, output_dir, password=password)


def extract_rar(archive_path: Path, output_dir: Path, password: str | None = None) -> None:
    try:
        with rarfile.RarFile(archive_path) as rf:
            if rf.needs_password() and not password:
                raise PasswordRequiredError("El RAR requiere password")
            try:
                rf.extractall(path=output_dir, pwd=password)
            except Exception as exc:
                text = str(exc).lower()
                if "password" in text or "crc" in text:
                    raise InvalidPasswordError("Password invalida") from exc
                raise
    except rarfile.RarCannotExec as exc:
        raise MissingRarBackendError(
            "No hay backend para RAR. Instala WinRAR o 7-Zip y agrega su ejecutable al PATH."
        ) from exc


def extract_7z(archive_path: Path, output_dir: Path, password: str | None = None) -> None:
    exe = _get_7z_executable()

    cmd = [exe, "x", str(archive_path), f"-o{output_dir}", "-y"]
    if password:
        cmd.append(f"-p{password}")
    else:
        cmd.append("-p-")

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0:
        return

    output = f"{proc.stdout}\n{proc.stderr}".lower()
    if ("wrong password" in output) or ("can not open encrypted archive" in output):
        raise InvalidPasswordError("Password invalida")
    if ("password" in output) and not password:
        raise PasswordRequiredError("El 7z requiere password")

    raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "Fallo al extraer 7z")


def extract_archive(archive_path: Path, output_dir: Path, password: str | None = None) -> None:
    lower = archive_path.name.lower()

    if lower.endswith(".zip"):
        extract_zip(archive_path, output_dir, password=password)
        return

    if lower.endswith(".rar"):
        extract_rar(archive_path, output_dir, password=password)
        return

    if lower.endswith(".7z") or re.search(r"\.7z\.\d{3}$", lower):
        extract_7z(archive_path, output_dir, password=password)
        return

    if lower.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".gz", ".bz2", ".xz")):
        shutil.unpack_archive(str(archive_path), str(output_dir))
        return

    raise ValueError(f"Formato no soportado: {archive_path.name}")


async def run_extract_in_thread(archive_path: Path, output_dir: Path, password: str | None = None) -> None:
    await asyncio.to_thread(extract_archive, archive_path, output_dir, password)
