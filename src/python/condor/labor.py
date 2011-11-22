"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

# XXX still completely insecure

from __future__ import absolute_import

import os
import os.path
import sys
import time
import socket
import signal
import random
import traceback
import collections
import multiprocessing
import cPickle as pickle
import condor

logger = condor.get_logger(__name__)

_current_task = None

def get_task():
    """Get the currently-executing task, if any."""

    return _current_task

try:
    import snappy
except ImportError:
    import zlib

    logger.debug("using zlib for compression")

    compress = lambda data: zlib.compress(data, 1)
    decompress = zlib.decompress
    compression_extension = "gz"
else:
    logger.debug("using snappy for compression")

    compress = snappy.compress
    decompress = snappy.decompress
    compression_extension = "snappy"

def send_pyobj_compressed(zmq_socket, message):
    pickled = pickle.dumps(message, protocol = -1)
    compressed = compress(pickled)

    zmq_socket.send(compressed)

def recv_pyobj_compressed(zmq_socket):
    compressed = zmq_socket.recv()
    decompressed = decompress(compressed)
    unpickled = pickle.loads(decompressed)

    return unpickled

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

        return Task(self._call, args, kwargs, self._key)

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

        if isinstance(message, ApplyMessage):
            # task request
            sender.disassociate()
            sender.set_assigned(self.next_task())

            return (sender.assigned.task, None)
        elif isinstance(message, DoneMessage):
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
        elif isinstance(message, InterruptedMessage):
            # worker interruption
            sender.set_interruption()

            return (None, None)
        elif isinstance(message, ErrorMessage):
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

class DistributedManager(object):
    """Manage remotely-distributed work."""

    def __init__(self, tasks, workers):
        """Initialize."""

        self._core = ManagerCore(tasks)

        # set up zeromq
        import zmq

        self._context = zmq.Context()
        self._rep_socket = self._context.socket(zmq.REP)

        rep_port = self._rep_socket.bind_to_random_port("tcp://*")

        logger.debug("communicating on port %i", rep_port)

        # launch condor jobs
        logger.info("distributing %i tasks to %i workers", len(tasks), workers)

        (root_path, self._cluster) = \
            condor.submit_condor_workers(
                workers,
                "tcp://{0}:{1}".format(socket.getfqdn(), rep_port),
                )

        self._cache = condor.cache.DiskCache(os.path.join(root_path, "cache"))

    def manage(self):
        """Manage workers and tasks."""

        import zmq

        try:
            poller = zmq.Poller()

            poller.register(self._rep_socket, zmq.POLLIN)

            while self._core.unfinished_count() > 0:
                events = dict(poller.poll())

                assert events.get(self._rep_socket) == zmq.POLLIN

                message = recv_pyobj_compressed(self._rep_socket)

                (next_task, completed) = self._core.handle(message)

                if next_task is None:
                    reply_message = None
                else:
                    reply_message = TaskMessage(None, next_task, self._cache.fork())

                send_pyobj_compressed(self._rep_socket, reply_message)

                if completed is not None:
                    yield completed
        except KeyboardInterrupt:
            # work around bizarre pyzmq SIGINT behavior
            raise

    def clean(self):
        """Clean up manager state."""

        condor.condor_rm(self._cluster)

        logger.info("removed condor jobs")

        self._rep_socket.close()
        self._context.term()

        logger.info("terminated zeromq context")

        self._cache.delete()

        logger.info("cleaned up argument cache")

class LocalWorkerProcess(multiprocessing.Process):
    """Work in a subprocess."""

    def __init__(self, stm_queue):
        """Initialize."""

        multiprocessing.Process.__init__(self)

        self.stm_queue = stm_queue
        self.mts_queue = multiprocessing.Queue()

    def run(self):
        """Work."""

        class DeathRequestedError(Exception):
            pass

        try:
            def handle_sigusr1(number, frame):
                raise DeathRequestedError()

            signal.signal(signal.SIGUSR1, handle_sigusr1)

            logger.info("subprocess running")

            task = None

            while True:
                # get an assignment
                if task is None:
                    self.stm_queue.put(ApplyMessage(os.getpid()))

                    task = self.mts_queue.get()

                    if task is None:
                        logger.info("received null assignment; terminating")

                        return None

                # complete the assignment
                try:
                    logger.info("starting work on task %s", task.key)

                    result = task()
                except KeyboardInterrupt, error:
                    logger.warning("interruption during task %s", task.key)

                    self.stm_queue.put(InterruptedMessage(os.getpid(), task.key))
                    self.mts_queue.get()

                    break
                except DeathRequestedError:
                    logger.warning("death requested; terminating")

                    break
                except BaseException, error:
                    description = traceback.format_exc(error)

                    logger.warning("error during task %s:\n%s", task.key, description)

                    self.stm_queue.put(ErrorMessage(os.getpid(), task.key, description))
                    self.mts_queue.get()

                    break
                else:
                    logger.info("finished task %s", task.key)

                    self.stm_queue.put(DoneMessage(os.getpid(), task.key, result))

                    task = self.mts_queue.get()
        except DeathRequestedError:
            pass

class ParallelManager(object):
    """Manage locally-distributed work."""

    def __init__(self, tasks, workers):
        """Initialize."""

        self._core = ManagerCore(tasks)

        logger.info("distributing %i tasks to %i workers", len(tasks), workers)

        self._stm_queue = multiprocessing.Queue()
        self._processes = [LocalWorkerProcess(self._stm_queue) for _ in xrange(workers)]

        for process in self._processes:
            process.start()

    def manage(self):
        """Manage workers and tasks."""

        process_index = dict((process.pid, process) for process in self._processes)

        while self._core.unfinished_count() > 0:
            message = self._stm_queue.get()

            (response, completed) = self._core.handle(message)

            process_index[message.sender].mts_queue.put(response)

            if completed is not None:
                yield completed

    def clean(self):
        for process in self._processes:
            os.kill(process.pid, signal.SIGUSR1)

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

def do(requests, workers, local = False):
    """Do work remotely or locally; yield result pairs."""

    tasks = sorted(map(Task.from_request, requests), key = lambda _: random.random())

    if workers > 0:
        if local:
            manager = ParallelManager(tasks, workers)
        else:
            manager = DistributedManager(tasks, workers)
    else:
        manager = SerialManager(tasks)

    try:
        for item in manager.manage():
            yield item
    finally:
        manager.clean()

def do_for(requests, workers, handler = lambda _, x: x, local = False):
    """Do work remotely or locally; apply handler to result pairs."""

    for (task, result) in do(requests, workers, local = local):
        handler(task, result)

do_or_distribute = do_for

