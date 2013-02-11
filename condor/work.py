"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import plac
import condor.work

if __name__ == "__main__":
    plac.call(condor.work.main)

import sys
import imp
import traceback
import zmq
import condor

logger = condor.log.get_logger(__name__, default_level = "NOTSET")

def work_once(condor_id, req_socket, task_message):
    """Request and/or complete a single unit of work."""

    # get an assignment
    poller = zmq.Poller()

    poller.register(req_socket, zmq.POLLIN)

    if task_message is None:
        condor.send_pyobj_compressed(
            req_socket,
            condor.messages.ApplyMessage(condor_id),
            )

        polled = poller.poll(timeout = 60 * 1000)

        if dict(polled).get(req_socket, 0) & zmq.POLLIN:
            task_message = condor.recv_pyobj_compressed(req_socket)
        else:
            task_message = None

        if task_message is None:
            logger.info("received null assignment or timed out; terminating")

            return None

    task = task_message.get_task()

    # complete the assignment
    try:
        logger.info("starting work on task %s", task.key)

        result = task()
    except KeyboardInterrupt, error:
        logger.warning("interruption during task %s", task.key)

        condor.send_pyobj_compressed(
            req_socket,
            condor.messages.InterruptedMessage(condor_id, task.key),
            )

        req_socket.recv()
    except BaseException, error:
        description = traceback.format_exc(error)

        logger.warning("error during task %s:\n%s", task.key, description)

        condor.send_pyobj_compressed(
            req_socket,
            condor.messages.ErrorMessage(condor_id, task.key, description),
            )

        req_socket.recv()
    else:
        logger.info("finished task %s", task.key)

        condor.send_pyobj_compressed(
            req_socket,
            condor.messages.DoneMessage(condor_id, task.key, result),
            )

        return condor.recv_pyobj_compressed(req_socket)

    return None

def work_loop(condor_id, req_socket):
    """Repeatedly request and complete units of work."""

    task_message = None

    while True:
        try:
            task_message = work_once(condor_id, req_socket, task_message)
        except Exception:
            raise

        if task_message is None:
            break

@plac.annotations(
    req_address = ("zeromq address of master"),
    condor_id = ("condor process specifier"),
    main_path = ("path to module that replaces __main__"),
    )
def main(req_address, condor_id, main_path = None):
    """Do arbitrary distributed work."""

    condor.log.enable_default_logging()

    # replace the __main__ module, if necessary
    if main_path is not None:
        sys.modules["__old_main__"] = sys.modules["__main__"]
        sys.modules["__main__"] = imp.load_source("__new_main__", main_path)

    # connect to the work server
    logger.info("connecting to %s", req_address)

    context = zmq.Context()

    req_socket = context.socket(zmq.REQ)

    req_socket.setsockopt(zmq.LINGER, 60 * 1000)
    req_socket.connect(req_address) 

    # enter the work loop
    try:
        work_loop(condor_id, req_socket)
    finally:
        logger.debug("flushing sockets and terminating zeromq context")

        req_socket.close()
        context.term()

        logger.debug("zeromq cleanup complete")

