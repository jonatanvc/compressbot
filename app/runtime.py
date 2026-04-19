import asyncio


USER_OPERATION_LOCKS: dict[int, asyncio.Lock] = {}
USER_QUEUE_UI_LOCKS: dict[int, asyncio.Lock] = {}
GLOBAL_HEAVY_JOB_SEMAPHORES: dict[int, asyncio.Semaphore] = {}


def get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in USER_OPERATION_LOCKS:
        USER_OPERATION_LOCKS[user_id] = asyncio.Lock()
    return USER_OPERATION_LOCKS[user_id]


def get_user_queue_ui_lock(user_id: int) -> asyncio.Lock:
    if user_id not in USER_QUEUE_UI_LOCKS:
        USER_QUEUE_UI_LOCKS[user_id] = asyncio.Lock()
    return USER_QUEUE_UI_LOCKS[user_id]


def get_global_heavy_job_semaphore(max_parallel_jobs: int) -> asyncio.Semaphore:
    slots = max(1, int(max_parallel_jobs))
    if slots not in GLOBAL_HEAVY_JOB_SEMAPHORES:
        GLOBAL_HEAVY_JOB_SEMAPHORES[slots] = asyncio.Semaphore(slots)
    return GLOBAL_HEAVY_JOB_SEMAPHORES[slots]


USER_ACTIVE_TASKS: dict[int, asyncio.Task] = {}


def register_active_task(user_id: int, task: asyncio.Task) -> None:
    USER_ACTIVE_TASKS[user_id] = task


def unregister_active_task(user_id: int) -> None:
    USER_ACTIVE_TASKS.pop(user_id, None)


def cancel_active_task(user_id: int) -> bool:
    task = USER_ACTIVE_TASKS.get(user_id)
    if task and not task.done():
        task.cancel()
        return True
    return False
