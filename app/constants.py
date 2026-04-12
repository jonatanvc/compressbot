MODE_IDLE = "idle"
MODE_WAIT_COMPRESS_FILES = "wait_compress_files"
MODE_WAIT_EXTRACT_ARCHIVES = "wait_extract_archives"
MODE_WAIT_PASSWORD = "wait_password"
MODE_WAIT_ENCRYPT_PASSWORD = "wait_encrypt_password"
MODE_WAIT_RENAME = "wait_rename"

KEY_MODE = "mode"
KEY_FILES_TO_COMPRESS = "files_to_compress"
KEY_ARCHIVES_TO_EXTRACT = "archives_to_extract"
KEY_EXTRACTED_FILES = "extracted_files"
KEY_CURRENT_ARCHIVE = "current_archive"
KEY_KNOWN_PASSWORD = "known_password"
KEY_MESSAGE_IDS = "message_ids"
KEY_PROGRESS = "progress_state"
KEY_COMPRESS_QUEUE_MESSAGE_ID = "compress_queue_message_id"
KEY_EXTRACT_QUEUE_MESSAGE_ID = "extract_queue_message_id"
KEY_LAST_COMPRESS_QUEUE_TEXT = "last_compress_queue_text"
KEY_LAST_EXTRACT_QUEUE_TEXT = "last_extract_queue_text"
KEY_PASSWORD_PROMPT_MESSAGE_ID = "password_prompt_message_id"
KEY_START_MESSAGE_ID = "start_message_id"
KEY_ACTION_PROMPT_MESSAGE_ID = "action_prompt_message_id"
KEY_EXTRACT_TOTAL_RECEIVED = "extract_total_received"
KEY_EXTRACT_ARCHIVES_DONE = "extract_archives_done"
KEY_RESULT_ACTIONS = "result_actions"
KEY_PENDING_ACTION = "pending_action"
KEY_PENDING_ACTION_ID = "pending_action_id"
KEY_PENDING_FILE_ID = "pending_file_id"
KEY_PENDING_FILE_NAME = "pending_file_name"
KEY_PENDING_SOURCE_MESSAGE_ID = "pending_source_message_id"
KEY_PENDING_LOCAL_TARGETS = "pending_local_targets"
KEY_CANCEL_REQUESTED = "cancel_requested"

BTN_EXTRAER = "Extraer"
BTN_COMPRIMIR = "Comprimir"
BTN_LIMPIAR = "Limpiar"
BTN_CANCELAR = "Cancelar"
BTN_FINALIZAR_COMPRESION = "Finalizar compresion"
BTN_FINALIZAR_EXTRACCION = "Finalizar extraccion"
BTN_MENU_PRINCIPAL = "Menu principal"
BTN_ENCRYPTAR = "Encriptar"
BTN_RENOMBRAR = "Renombrar"
BTN_FAVORITO = "Anadir a favoritos"
BTN_QUITAR_FAVORITO = "Quitar de favoritos"
BTN_LISTO = "Listo"

CB_EXTRAER = "cb_extraer"
CB_COMPRIMIR = "cb_comprimir"
CB_LIMPIAR = "cb_limpiar"
CB_CANCELAR = "cb_cancelar"
CB_FINALIZAR_COMPRESION = "cb_finalizar_compresion"
CB_FINALIZAR_EXTRACCION = "cb_finalizar_extraccion"
CB_MENU_PRINCIPAL = "cb_menu_principal"
CB_ENCRYPTAR = "cb_encryptar"
CB_RENOMBRAR = "cb_renombrar"
CB_FAVORITO = "cb_favorito"
CB_FAVORITO_REMOVE = "cb_favorito_remove"
CB_LISTO = "cb_listo"
CB_CANCEL_PROGRESS = "cb_cancel_progress"

ARCHIVE_EXTENSIONS = {
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".tgz",
    ".bz2",
    ".xz",
    ".tar.gz",
    ".tar.bz2",
    ".tar.xz",
}

AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac", ".opus"}
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
