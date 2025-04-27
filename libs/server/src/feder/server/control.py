from threading import Lock


class ThreadControl:
    def __init__(self):
        self._mutex = Lock()
        self._state = Lock()

    def pause(self):
        with self._mutex:
            if not self.is_running:
                return
            self._state.acquire()

    def resume(self):
        with self._mutex:
            if self.is_running:
                return
            self._state.release()

    def check(self):
        with self._state:
            pass

    @property
    def is_running(self):
        return not self._state.locked()
