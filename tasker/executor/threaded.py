import ctypes
import threading
import concurrent.futures

from . import _executor
from .. import worker


class ThreadedExecutor(
    _executor.Executor,
):
    def execute_tasks(
        self,
        tasks,
    ):
        future_to_task = {}
        self.current_timers = {}

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.worker_config['executor']['concurrency'],
        ) as executor:
            for task in tasks:
                future = executor.submit(self.execute_task, task)
                future_to_task[future] = task

        for future in concurrent.futures.as_completed(future_to_task):
            pass

    def pre_work(
        self,
        task,
    ):
        self.update_current_task(
            task=task,
        )

        interval = self.worker_config['timeouts']['soft_timeout']
        if interval == 0:
            interval = None

        self.current_timers[threading.get_ident()] = threading.Timer(
            interval=interval,
            function=ctypes.pythonapi.PyThreadState_SetAsyncExc,
            args=(
                ctypes.c_long(threading.get_ident()),
                ctypes.py_object(worker.WorkerSoftTimedout),
            )
        )

        self.current_timers[threading.get_ident()].start()

    def post_work(
        self,
    ):
        self.current_timers[threading.get_ident()].cancel()
