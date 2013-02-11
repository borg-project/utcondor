"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import sys
import time
import random
import collections
import condor

logger = condor.log.get_logger(__name__, default_level = "INFO")

from .distributed import DistributedManager
from .parallel import ParallelManager
from .serial import SerialManager

class Task(object):
    """One unit of distributable work."""

    def __init__(self, call, args = [], kwargs = {}, key = None):
        self.call = call
        self.args = args
        self.kwargs = kwargs

        if key is None:
            self.key = id(self)
        else:
            self.key = key

    def __hash__(self):
        return hash(self.key)

    def __call__(self):
        return self.call(*self.args, **self.kwargs)

    @staticmethod
    def from_request(request):
        """Build a task, if necessary."""

        if isinstance(request, Task):
            return request
        elif isinstance(request, collections.Mapping):
            return Task(**request)
        else:
            return Task(*request)

class TaskState(object):
    """Current state of progress on a task."""

    def __init__(self, task):
        self.task = task
        self.done = False
        self.working = set()

    def score(self):
        """Score the urgency of this task."""

        if self.done:
            return (sys.maxint, sys.maxint, random.random())
        if len(self.working) == 0:
            return (0, 0, random.random())
        else:
            return (
                len(self.working),
                max(wstate.timestamp for wstate in self.working),
                random.random(),
                )

class WorkerState(object):
    """Current state of a known worker process."""

    def __init__(self, condor_id):
        self.condor_id = condor_id
        self.assigned = None
        self.timestamp = None

    def set_done(self):
        self.assigned.working.remove(self)

        was_done = self.assigned.done

        self.assigned.done = True
        self.assigned = None

        return was_done

    def set_assigned(self, tstate):
        """Change worker state in response to assignment."""

        self.disassociate()

        self.assigned = tstate
        self.timestamp = time.time()

        self.assigned.working.add(self)

    def set_interruption(self):
        """Change worker state in response to interruption."""

        self.disassociate()

    def set_error(self):
        """Change worker state in response to error."""

        self.disassociate()

    def disassociate(self):
        """Disassociate from the current job."""

        if self.assigned is not None:
            self.assigned.working.remove(self)

            self.assigned = None

class ManagerCore(object):
    """Maintain the task queue and worker assignments."""

    def __init__(self, tasks):
        """Initialize."""

        self.tstates = dict((t.key, TaskState(t)) for t in tasks)
        self.wstates = {}

    def handle(self, message):
        """Manage workers and tasks."""

        logger.info(
            "[%s/%i] %s",
            str(self.done_count()).rjust(len(str(len(self.tstates))), "0"),
            len(self.tstates),
            message.get_summary(),
            )

        sender = self.wstates.get(message.sender)

        if sender is None:
            sender = WorkerState(message.sender)

            self.wstates[sender.condor_id] = sender

        if isinstance(message, condor.messages.ApplyMessage):
            # task request
            sender.disassociate()
            sender.set_assigned(self.next_task())

            return (sender.assigned.task, None)
        elif isinstance(message, condor.messages.DoneMessage):
            # task result
            finished = sender.assigned
            was_done = sender.set_done()

            assert finished.task.key == message.key

            selected = self.next_task()

            if selected is None:
                selected_task = None
            else:
                selected_task = selected.task

                sender.set_assigned(selected)

            if was_done:
                return (selected_task, None)
            else:
                return (selected_task, (finished.task, message.result))
        elif isinstance(message, condor.messages.InterruptedMessage):
            # worker interruption
            sender.set_interruption()

            return (None, None)
        elif isinstance(message, condor.messages.ErrorMessage):
            # worker exception
            sender.set_error()

            return (None, None)
        else:
            raise TypeError("unrecognized message type")

    def next_task(self):
        """Select the next task on which to work."""

        tstate = min(self.tstates.itervalues(), key = TaskState.score)

        if tstate.done:
            return None
        else:
            return tstate

    def done_count(self):
        """Return the number of completed tasks."""

        return sum(1 for t in self.tstates.itervalues() if t.done)

    def unfinished_count(self):
        """Return the number of unfinished tasks."""

        return sum(1 for t in self.tstates.itervalues() if not t.done)

