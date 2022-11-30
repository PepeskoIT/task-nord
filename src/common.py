from asyncio import Semaphore
from typing import Callable


def sempahore(limit: int) -> Callable:
    """Async decorator that limits pararell executions of decorated function.

    Args:
        limit (int): maximum allowed concurrent executions

    Returns:
        Callable: inner decorator function
    """
    sem = Semaphore(limit)

    def inner(f):
        async def wrapper(*args, **kwargs):
            async with sem:
                return await f(*args, **kwargs)
        return wrapper
    return inner
