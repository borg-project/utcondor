"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import os.path
import socket
import condor

logger = condor.log.get_logger(__name__, default_level = "INFO")

class DistributedManager(object):
    """Manage remotely-distributed work."""

    def __init__(self, tasks, workers):
        """Initialize."""

        self._core = condor.managers.ManagerCore(tasks)

        # set up zeromq
        import zmq

        self._context = zmq.Context()
        self._rep_socket = self._context.socket(zmq.REP)

        rep_port = self._rep_socket.bind_to_random_port("tcp://*")

        logger.debug("communicating on port %i", rep_port)

        # launch condor jobs
        logger.info("distributing %i tasks to %i workers", len(tasks), workers)

        (root_path, self._cluster) = \
            condor.raw.submit_condor_workers(
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

                message = condor.recv_pyobj_compressed(self._rep_socket)

                (next_task, completed) = self._core.handle(message)

                if next_task is None:
                    reply_message = None
                else:
                    reply_message = condor.messages.TaskMessage(None, next_task, self._cache.fork())

                condor.send_pyobj_compressed(self._rep_socket, reply_message)

                if completed is not None:
                    yield completed
        except KeyboardInterrupt:
            # work around bizarre pyzmq SIGINT behavior
            raise

    def clean(self):
        """Clean up manager state."""

        #condor.raw.condor_rm(self._cluster)

        #logger.info("removed condor jobs")

        condor.raw.condor_vacate_job(self._cluster)

        #logger.info("held condor jobs")

        self._rep_socket.close()
        self._context.term()

        logger.info("terminated zeromq context")

        self._cache.delete()

        logger.info("cleaned up argument cache")

