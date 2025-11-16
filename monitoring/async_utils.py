import asyncio
from typing import Iterable, Awaitable, Optional, Callable, List


async def run_tasks_with_cleanup(
    tasks: Iterable[asyncio.Task],
    cleanup: Optional[Callable[[], Awaitable[None]]] = None,
) -> None:
    task_list: List[asyncio.Task] = list(tasks)
    try:
        if task_list:
            await asyncio.gather(*task_list)
    except asyncio.CancelledError:
        pass
    finally:
        for t in task_list:
            if not t.done():
                t.cancel()
        if task_list:
            await asyncio.gather(*task_list, return_exceptions=True)
        if cleanup is not None:
            await cleanup()

