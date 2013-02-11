"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import os
import signal
import multiprocessing
import condor

logger = condor.log.get_logger(__name__)

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
                    self.stm_queue.put(condor.messages.ApplyMessage(os.getpid()))

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

                    self.stm_queue.put(condor.messages.InterruptedMessage(os.getpid(), task.key))
                    self.mts_queue.get()

                    break
                except DeathRequestedError:
                    logger.warning("death requested; terminating")

                    break
                except BaseException, error:
                    description = traceback.format_exc(error)

                    logger.warning("error during task %s:\n%s", task.key, description)

                    self.stm_queue.put(condor.messages.ErrorMessage(os.getpid(), task.key, description))
                    self.mts_queue.get()

                    break
                else:
                    logger.info("finished task %s", task.key)

                    self.stm_queue.put(condor.messages.DoneMessage(os.getpid(), task.key, result))

                    task = self.mts_queue.get()
        except DeathRequestedError:
            pass

class ParallelManager(object):
    """Manage locally-distributed work."""

    def __init__(self, tasks, workers):
        """Initialize."""

        self._core = condor.managers.ManagerCore(tasks)

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

