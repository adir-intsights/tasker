import time
import threading

from .. import logger


class Heartbeater(
    threading.Thread,
):
    def __init__(
        self,
        monitor_client,
        interval,
    ):
        super().__init__()

        self.monitor_client = monitor_client
        self.interval = interval

        self._stop_event = threading.Event()
        self._stop_event.set()

        self.logger = logger.logger.Logger(
            logger_name='heartbeater',
        )

        self.daemon = True

    def run(
        self,
    ):
        sleep_duration = 0

        while self._stop_event.is_set():
            if sleep_duration < self.interval:
                time.sleep(0.1)
                sleep_duration += 0.1

                continue

            try:
                self.monitor_client.increment_heartbeat()

                sleep_duration = 0
            except Exception as exception:
                self.logger.error(
                    msg=exception,
                )

                time.sleep(1.0)

    def stop(
        self,
    ):
        self._stop_event.clear()

    def __del__(
        self,
    ):
        self.stop()


class DummyHeartbeater:
    def __init__(
        self,
        *args,
        **kwargs
    ):
        pass

    def start(
        self,
    ):
        pass

    def stop(
        self,
    ):
        pass
