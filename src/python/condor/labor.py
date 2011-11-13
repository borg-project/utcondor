"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

from __future__ import absolute_import

import os
import sys
import time
import zlib
import socket
import signal
import random
import traceback
import collections
import multiprocessing
import cPickle as pickle
import numpy
import cargo

logger = cargo.get_logger(__name__, level = "INFO")

_current_task = None

def get_task():
    """Get the currently-executing task, if any."""

    return _current_task

def send_pyobj_gz(zmq_socket, message):
    pickled = pickle.dumps(message)
    compressed = zlib.compress(pickled, 1)

    zmq_socket.send(compressed)

def recv_pyobj_gz(zmq_socket):
    compressed = zmq_socket.recv()
    decompressed = zlib.decompress(compressed)
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

class Task(object):
    """One unit of distributable work."""

    def __init__(self, call, args = [], kwargs = {}):
        self.call = call
        self.args = args
        self.kwargs = kwargs
        self.key = id(self)

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
            return Task(**mapping)
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

    def __init__(self, task_list):
        """Initialize."""

        self.tstates = dict((t.key, TaskState(t)) for t in task_list)
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

class RemoteManager(object):
    """Manage remotely-distributed work."""

    def __init__(self, task_list, handler, rep_socket):
        """Initialize."""

        self.handler = handler
        self.rep_socket = rep_socket
        self.core = ManagerCore(task_list)

    def manage(self):
        """Manage workers and tasks."""

        import zmq

        poller = zmq.Poller()

        poller.register(self.rep_socket, zmq.POLLIN)

        while self.core.unfinished_count() > 0:
            events = dict(poller.poll())

            assert events.get(self.rep_socket) == zmq.POLLIN

            message = recv_pyobj_gz(self.rep_socket)

            (response, completed) = self.core.handle(message)

            send_pyobj_gz(self.rep_socket, response)

            if completed is not None:
                self.handler(*completed)

    @staticmethod
    def distribute(tasks, workers = 8, handler = lambda _, x: x):
        """Distribute computation to remote workers."""

        import zmq

        logger.info("distributing %i tasks to %i workers", len(tasks), workers)

        # prepare zeromq
        context = zmq.Context()
        rep_socket = context.socket(zmq.REP)
        rep_port = rep_socket.bind_to_random_port("tcp://*")

        logger.debug("listening on port %i", rep_port)

        # launch condor jobs
        cluster = cargo.submit_condor_workers(workers, "tcp://%s:%i" % (socket.getfqdn(), rep_port))

        try:
            try:
                return RemoteManager(tasks, handler, rep_socket).manage()
            except KeyboardInterrupt:
                # work around bizarre pyzmq SIGINT behavior
                raise
        finally:
            # clean up condor jobs
            cargo.condor_rm(cluster)

            logger.info("removed condor jobs")

            # clean up zeromq
            rep_socket.close()
            context.term()

            logger.info("terminated zeromq context")

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
                    seed = abs(hash(task.key))

                    logger.info("setting PRNG seed to %s", seed)

                    numpy.random.seed(seed)
                    random.seed(numpy.random.randint(2**32))

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

class LocalManager(object):
    """Manage locally-distributed work."""

    def __init__(self, stm_queue, task_list, processes, handler):
        """Initialize."""

        self.stm_queue = stm_queue
        self.core = ManagerCore(task_list)
        self.processes = processes
        self.handler = handler

    def manage(self):
        """Manage workers and tasks."""

        process_index = dict((process.pid, process) for process in self.processes)

        while self.core.unfinished_count() > 0:
            message = self.stm_queue.get()

            (response, completed) = self.core.handle(message)

            process_index[message.sender].mts_queue.put(response)

            if completed is not None:
                self.handler(*completed)

    @staticmethod
    def distribute(tasks, workers = 8, handler = lambda _, x: x):
        """Distribute computation to remote workers."""

        logger.info("distributing %i tasks to %i workers", len(tasks), workers)

        stm_queue = multiprocessing.Queue()
        processes = [LocalWorkerProcess(stm_queue) for _ in xrange(workers)]

        for process in processes:
            process.start()

        try:
            return LocalManager(stm_queue, tasks, processes, handler).manage()
        finally:
            for process in processes:
                os.kill(process.pid, signal.SIGUSR1)

            logger.info("cleaned up child processes")

def do_or_distribute(requests, workers, handler = lambda _, x: x, local = False):
    """Distribute or compute locally."""

    tasks = map(Task.from_request, requests)

    if workers > 0:
        if local:
            return LocalManager.distribute(tasks, workers, handler)
        else:
            return RemoteManager.distribute(tasks, workers, handler)
    else:
        while tasks:
            task = tasks.pop()
            result = task()

            handler(task, result)

