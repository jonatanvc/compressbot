import asyncio


USER_OPERATION_LOCKS: dict[int, asyncio.Lock] = {}
GLOBAL_HEAVY_JOB_SEMAPHORES: dict[int, asyncio.Semaphore] = {}


def get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in USER_OPERATION_LOCKS:
        USER_OPERATION_LOCKS[user_id] = asyncio.Lock()
    return USER_OPERATION_LOCKS[user_id]


def get_global_heavy_job_semaphore(max_parallel_jobs: int) -> asyncio.Semaphore:
    slots = max(1, int(max_parallel_jobs))
    if slots not in GLOBAL_HEAVY_JOB_SEMAPHORES:
        GLOBAL_HEAVY_JOB_SEMAPHORES[slots] = asyncio.Semaphore(slots)
    return GLOBAL_HEAVY_JOB_SEMAPHORES[slots]
