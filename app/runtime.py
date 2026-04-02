import asyncio


USER_OPERATION_LOCKS: dict[int, asyncio.Lock] = {}


def get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in USER_OPERATION_LOCKS:
        USER_OPERATION_LOCKS[user_id] = asyncio.Lock()
    return USER_OPERATION_LOCKS[user_id]
