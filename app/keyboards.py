from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .constants import (
    BTN_CANCELAR,
    BTN_COMPRIMIR,
    BTN_EXTRAER,
    BTN_FINALIZAR_COMPRESION,
    BTN_FINALIZAR_EXTRACCION,
    BTN_LIMPIAR,
    BTN_MENU_PRINCIPAL,
    BTN_ENCRYPTAR,
    BTN_RENOMBRAR,
    BTN_FAVORITO,
    BTN_QUITAR_FAVORITO,
    BTN_LISTO,
    BTN_COMPRESS_NO_PASSWORD,
    BTN_COMPRESS_YES_PASSWORD,
    CB_CANCELAR,
    CB_COMPRIMIR,
    CB_EXTRAER,
    CB_FINALIZAR_COMPRESION,
    CB_FINALIZAR_EXTRACCION,
    CB_LIMPIAR,
    CB_MENU_PRINCIPAL,
    CB_ENCRYPTAR,
    CB_RENOMBRAR,
    CB_FAVORITO,
    CB_FAVORITO_REMOVE,
    CB_LISTO,
    CB_CANCEL_PROGRESS,
    CB_COMPRESS_NO_PASSWORD,
    CB_COMPRESS_YES_PASSWORD,
)


def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(BTN_EXTRAER, callback_data=CB_EXTRAER),
            InlineKeyboardButton(BTN_COMPRIMIR, callback_data=CB_COMPRIMIR),
            InlineKeyboardButton(BTN_LIMPIAR, callback_data=CB_LIMPIAR),
        ]]
    )


def compress_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(BTN_FINALIZAR_COMPRESION, callback_data=CB_FINALIZAR_COMPRESION),
            InlineKeyboardButton(BTN_CANCELAR, callback_data=CB_CANCELAR),
        ]]
    )


def extract_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(BTN_FINALIZAR_EXTRACCION, callback_data=CB_FINALIZAR_EXTRACCION),
            InlineKeyboardButton(BTN_CANCELAR, callback_data=CB_CANCELAR),
        ]]
    )


def waiting_upload_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(BTN_CANCELAR, callback_data=CB_CANCELAR),
            InlineKeyboardButton(BTN_MENU_PRINCIPAL, callback_data=CB_MENU_PRINCIPAL),
        ]]
    )


def compressed_result_keyboard(action_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(BTN_FAVORITO, callback_data=CB_FAVORITO)]]
    )


def compress_password_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(BTN_COMPRESS_NO_PASSWORD, callback_data=CB_COMPRESS_NO_PASSWORD),
                InlineKeyboardButton(BTN_COMPRESS_YES_PASSWORD, callback_data=CB_COMPRESS_YES_PASSWORD),
            ],
            [InlineKeyboardButton(BTN_CANCELAR, callback_data=CB_CANCELAR)],
        ]
    )


def favorite_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BTN_FAVORITO, callback_data=CB_FAVORITO)]])


def favorite_channel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(BTN_QUITAR_FAVORITO, callback_data=CB_FAVORITO_REMOVE)]])


def progress_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(BTN_CANCELAR, callback_data=CB_CANCEL_PROGRESS)]]
    )
