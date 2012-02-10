"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

class SerialManager(object):
    """Manage in-process work."""

    def __init__(self, tasks):
        self._tasks = tasks

    def manage(self):
        """Complete tasks."""

        while self._tasks:
            task = self._tasks.pop()

            yield (task, task())

    def clean(self):
        pass

