"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import os
import socket
import condor

class Message(object):
    """Message from a worker."""

    def __init__(self, sender):
        self.sender = sender
        self.host = socket.gethostname()
        self.pid = os.getpid()

    def make_summary(self, text):
        return "worker {0} (pid {1} on {2}) {3}".format(self.sender, self.pid, self.host, text)

class ApplyMessage(Message):
    """A worker wants a unit of work."""

    def get_summary(self):
        return self.make_summary("requested a job")

class ErrorMessage(Message):
    """An error occurred in a task."""

    def __init__(self, sender, key, description):
        Message.__init__(self, sender)

        self.key = key
        self.description = description

    def get_summary(self):
        brief = self.description.splitlines()[-1]

        return self.make_summary("encountered an error ({0})".format(brief))

class InterruptedMessage(Message):
    """A worker was interrupted."""

    def __init__(self, sender, key):
        Message.__init__(self, sender)

        self.key = key

    def get_summary(self):
        return self.make_summary("was interrupted")

class DoneMessage(Message):
    """A task was completed."""

    def __init__(self, sender, key, result):
        Message.__init__(self, sender)

        self.key = key
        self.result = result

    def get_summary(self):
        return self.make_summary("finished job {0}".format(self.key))

class TaskMessage(Message):
    """A task, with arguments stored appropriately."""

    def __init__(self, sender, task, cache = None):
        Message.__init__(self, sender)

        if cache is None:
            cache = {}

        self._call = task.call
        self._arg_ids = map(id, task.args)
        self._kwarg_ids = dict((k, id(v)) for (k, v) in task.kwargs.items())
        self._key = task.key
        self._cache = cache

        for arg in task.args:
            self._cache[id(arg)] = arg

        for kwarg in task.kwargs.values():
            self._cache[id(kwarg)] = kwarg

    def get_task(self):
        args = map(self._cache.__getitem__, self._arg_ids)
        kwargs = dict((k, self._cache[v]) for (k, v) in self._kwarg_ids.items())

        return condor.managers.Task(self._call, args, kwargs, self._key)

