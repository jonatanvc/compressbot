# Bot de Compresion y Extraccion para Telegram

Bot con 3 botones:
- `Extraer`
- `Comprimir`
- `Limpiar`

Los controles del bot son solo `inline` (sin teclado de texto).

## Funciones

- `/start` muestra los botones principales.
- `/status` muestra estado actual (modo, colas, progreso, concurrencia y checksum).
- `Comprimir`:
  - Recibe archivos enviados o reenviados.
  - Al entrar en compresion muestra mensaje de espera (sin finalizar inmediato).
  - Al cargar archivos para comprimir usa un unico mensaje que se va editando con el total cargado (evita spam de mensajes).
  - Crea un unico `.zip` con todos los archivos recibidos.
  - Sube el archivo comprimido de vuelta al chat.
  - Muestra estado con barra de progreso: descarga, compresion y subida.
- `Extraer`:
  - Recibe uno o varios comprimidos (`zip`, `rar`, `tar`, `gz`, `bz2`, `xz`).
  - Al entrar en extraccion muestra mensaje de espera (sin finalizar inmediato).
  - Al cargar comprimidos usa un unico mensaje editable con total cargado.
  - Si detecta password, la solicita al usuario.
  - Al enviar password, se limpian el prompt anterior, el mensaje de password del usuario y la barra de progreso vieja.
  - Extrae y envia los archivos uno por uno en su formato (audio, video, imagen, documento, etc.).
  - Muestra estado con barra de progreso: descarga, descompresion y subida.
- `Limpiar`:
  - Intenta borrar los mensajes gestionados por el bot en el chat.
  - Elimina archivos temporales/residuales del usuario.
  - Tambien borra el root temporal cuando ya no quedan carpetas de trabajo.
  - Al pulsar botones inline, se eliminan mensajes anteriores del bot para mantener el chat limpio.

### Extensiones de documento explicitas

El bot maneja documentos en cualquier extension por defecto, y ademas reconoce de forma explicita:

- Texto y lectura: `.txt`, `.md`, `.rtf`, `.pdf`, `.epub`, `.djvu`
- Ofimatica: `.doc`, `.docx`, `.odt`, `.xls`, `.xlsx`, `.ods`, `.csv`, `.tsv`, `.ppt`, `.pptx`, `.odp`
- Datos y config: `.json`, `.xml`, `.yaml`, `.yml`, `.ini`, `.cfg`, `.log`, `.sql`, `.db`, `.sqlite`, `.sqlite3`
- Diseno y varios: `.psd`, `.ai`, `.sketch`, `.fig`
- Comprimidos como documento: `.zip`, `.rar`, `.7z`, `.tar`, `.gz`, `.bz2`, `.xz`

## Requisitos

- Python 3.11 o superior
- Token de bot de Telegram (creado con BotFather)
- Para extraer `.rar`: tener WinRAR o 7-Zip instalado y disponible en el `PATH`

## Instalacion

```bash
pip install -r requirements.txt
```

## Configuracion

Crea la variable de entorno en Windows PowerShell:

```powershell
$env:TELEGRAM_BOT_TOKEN="TU_TOKEN"
$env:TELEGRAM_API_ID="29731907"
$env:TELEGRAM_API_HASH="9dc7dc047bf497f26ef6fa4c6e64689f"
$env:ALLOWED_USER_IDS="123456789,987654321"
$env:ONLY_PRIVATE_CHAT="1"
```

Notas de seguridad:

- Si llegaste a usar un token real en archivos del proyecto, regeneralo en BotFather.
- Usa `ALLOWED_USER_IDS` para restringir el bot solo a tus usuarios.
- Con `ONLY_PRIVATE_CHAT=1` se bloquea el uso en grupos.

## Ejecutar

```bash
python bot.py
```

## Arquitectura Modular

- `bot.py`: wrapper de compatibilidad para ejecutar el bot como antes.
- `app/main.py`: punto de entrada principal del bot.
- `app/config.py`: carga y parseo de variables de entorno.
- `app/constants.py`: constantes de estados, botones y extensiones.
- `app/common.py`: utilidades de archivos, estado y helpers de integridad.
- `app/archive_ops.py`: extraccion de ZIP/RAR/TAR/GZ/BZ2/XZ.
- `app/telegram_ops.py`: progreso, reintentos y envio de archivos.
- `app/handlers.py`: handlers de Telegram y orquestacion de flujos.

## Deploy en Dokploy

Este proyecto es compatible con Dokploy usando el `Dockerfile` incluido.

### 1. Preparar el repositorio

1. Sube este proyecto a Git (sin subir `.env` real).
2. Verifica que `.dockerignore` incluya `.env` (ya esta configurado).
3. Usa `.env.example` como plantilla de variables.

### 2. Crear servicio en Dokploy

1. En Dokploy, crea un nuevo servicio desde tu repositorio Git.
2. Tipo de despliegue: `Dockerfile`.
3. Este bot no expone HTTP, asi que configuralo como servicio tipo worker/background (sin dominio ni puerto publico).

### 3. Variables de entorno requeridas

- `TELEGRAM_BOT_TOKEN`
- `ALLOWED_USER_IDS` (ejemplo: `123456789,987654321`)
- `ONLY_PRIVATE_CHAT=1`

Variables recomendadas:

- `FAVORITES_CHANNEL_ID` (ejemplo: `-1001234567890`)
- `MAX_DOWNLOAD_CONCURRENCY`
- `MAX_UPLOAD_CONCURRENCY`
- `MAX_RETRIES`
- `RETRY_BASE_DELAY`
- `ENABLE_CHECKSUM`
- `TELEGRAM_CONNECT_TIMEOUT`
- `TELEGRAM_READ_TIMEOUT`
- `TELEGRAM_WRITE_TIMEOUT`
- `TELEGRAM_POOL_TIMEOUT`

### 3.1 Configuracion exacta para archivos grandes en Dokploy

Si quieres que el bot maneje archivos grandes al maximo permitido por Telegram, despliega tambien un servicio de Bot API local en Dokploy y usa estas variables:

```dotenv
TELEGRAM_BOT_TOKEN=TU_TOKEN_DEL_BOT
TELEGRAM_API_ID=29731907
TELEGRAM_API_HASH=9dc7dc047bf497f26ef6fa4c6e64689f
ALLOWED_USER_IDS=123456789
ONLY_PRIVATE_CHAT=1
FAVORITES_CHANNEL_ID=-1003842253935

MAX_DOWNLOAD_CONCURRENCY=4
MAX_UPLOAD_CONCURRENCY=3
MAX_RETRIES=4
RETRY_BASE_DELAY=0.8
ENABLE_CHECKSUM=1

TELEGRAM_CONNECT_TIMEOUT=30
TELEGRAM_READ_TIMEOUT=180
TELEGRAM_WRITE_TIMEOUT=180
TELEGRAM_POOL_TIMEOUT=30

TELEGRAM_API_BASE_URL=http://botapi:8081/bot
TELEGRAM_API_FILE_BASE_URL=http://botapi:8081/file/bot
```

Puntos clave:

- `botapi` debe ser el nombre interno real del servicio Bot API local en Dokploy.
- Si tu servicio interno se llama distinto, cambia `botapi` por ese nombre.
- Si no despliegas Bot API local, deja `TELEGRAM_API_BASE_URL` y `TELEGRAM_API_FILE_BASE_URL` vacias.
- Esta configuracion es la que permite bajar, subir, comprimir, extraer, renombrar y reenviar archivos grandes con menos friccion, siempre dentro de las reglas de Telegram.

### 4. Build y arranque

1. Ejecuta deploy.
2. Revisa logs de arranque: debe iniciar sin errores y entrar en polling de Telegram.
3. Si falla por permisos de favoritos, agrega el bot como admin del canal y valida `FAVORITES_CHANNEL_ID`.

Notas:

- La imagen instala `p7zip-full` y `unrar-free` para soporte de extraccion `.7z` y `.rar`.
- No guardes tokens reales ni IDs sensibles en el repositorio.

## Notas

- La extraccion con password esta soportada en ZIP y RAR.
- Telegram tiene limites de velocidad; el bot aplica espera automatica cuando es necesario.
- Despues de comprimir o extraer, el bot limpia automaticamente los temporales del disco.
- Si un archivo final supera el limite de subida de Telegram, el bot lo divide en partes `.partNNN` y las envia automaticamente.

## Ajustes de rendimiento

Variables opcionales de entorno:

- `TELEGRAM_MAX_UPLOAD_BYTES`: limite maximo por archivo para subida (default aprox. 1.9 GB).
- `UPLOAD_PART_BYTES`: tamano de cada parte al dividir archivos grandes (default aprox. 1.5 GB).
- `MAX_DOWNLOAD_CONCURRENCY`: descargas simultaneas (default `4`).
- `MAX_UPLOAD_CONCURRENCY`: subidas simultaneas de archivos extraidos (default `3`).
- `TELEGRAM_CONNECT_TIMEOUT`: timeout de conexion a Telegram (default `30`).
- `TELEGRAM_READ_TIMEOUT`: timeout de lectura para descargas/subidas (default `180`).
- `TELEGRAM_WRITE_TIMEOUT`: timeout de escritura para descargas/subidas (default `180`).
- `TELEGRAM_POOL_TIMEOUT`: timeout del pool HTTP (default `30`).

### Archivos muy grandes (File is too big)

Si Telegram responde `File is too big` al descargar, renombrar, encriptar o extraer, no es un fallo del flujo del bot: es un limite del endpoint Bot API remoto.

Para soportar archivos de mayor tamano, usa Bot API local y define:

- `TELEGRAM_API_BASE_URL` (ejemplo: `http://botapi:8081/bot`)
- `TELEGRAM_API_FILE_BASE_URL` (ejemplo: `http://botapi:8081/file/bot`)

Con eso, el bot usara esas rutas para operaciones de archivo pesado.

## Modo Ultra-Seguro

El bot ahora aplica protecciones adicionales para minimizar errores:

- Reintentos automáticos en descarga y subida (`MAX_RETRIES`, default `4`).
- Espera progresiva entre reintentos (`RETRY_BASE_DELAY`, default `0.8`).
- Validaciones de archivo (existencia, tipo y tamano > 0).
- Checksum SHA-256 opcional para control de integridad (`ENABLE_CHECKSUM`, default activado).

Variables opcionales:

- `MAX_RETRIES`
- `RETRY_BASE_DELAY`
- `ENABLE_CHECKSUM` (`1`/`0`)

## Cola con Prioridad por Usuario

- En compresion y extraccion, los elementos en cola del usuario se priorizan por tamano (mas pequenos primero), para mejorar latencia percibida y reducir tiempos de bloqueo.
