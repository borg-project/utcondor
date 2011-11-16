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

logger = condor.get_logger(__name__, level = "NOTSET")

def work_once(condor_id, req_socket, task):
    """Request and/or complete a single unit of work."""

    # get an assignment
    if task is None:
        condor.send_pyobj_gz(
            req_socket,
            condor.labor.ApplyMessage(condor_id),
            )

        task = condor.recv_pyobj_gz(req_socket)

        if task is None:
            logger.info("received null assignment; terminating")

            return None

    # complete the assignment
    try:
        condor.labor._current_task = task

        logger.info("starting work on task %s", task.key)

        result = task()
    except KeyboardInterrupt, error:
        logger.warning("interruption during task %s", task.key)

        condor.send_pyobj_gz(
            req_socket,
            condor.labor.InterruptedMessage(condor_id, task.key),
            )

        req_socket.recv()
    except BaseException, error:
        description = traceback.format_exc(error)

        logger.warning("error during task %s:\n%s", task.key, description)

        condor.send_pyobj_gz(
            req_socket,
            condor.labor.ErrorMessage(condor_id, task.key, description),
            )

        req_socket.recv()
    else:
        logger.info("finished task %s", task.key)

        condor.send_pyobj_gz(
            req_socket,
            condor.labor.DoneMessage(condor_id, task.key, result),
            )

        return condor.recv_pyobj_gz(req_socket)

    condor.labor._current_task = None

    return None

def work_loop(condor_id, req_socket):
    """Repeatedly request and complete units of work."""

    task = None

    while True:
        try:
            task = work_once(condor_id, req_socket, task)
        except Exception:
            raise

        if task is None:
            break

@plac.annotations(
    req_address = ("zeromq address of master"),
    condor_id = ("condor process specifier"),
    main_path = ("path to module that replaces __main__"),
    )
def main(req_address, condor_id, main_path = None):
    """Do arbitrary distributed work."""

    condor.enable_default_logging()

    # replace the __main__ module, if necessary
    if main_path is not None:
        sys.modules["__old_main__"] = sys.modules["__main__"]
        sys.modules["__main__"] = imp.load_source("__new_main__", main_path)

    # connect to the work server
    logger.info("connecting to %s", req_address)

    context = zmq.Context()

    req_socket = context.socket(zmq.REQ)

    req_socket.connect(req_address) 

    # enter the work loop
    try:
        work_loop(condor_id, req_socket)
    finally:
        logger.debug("flushing sockets and terminating zeromq context")

        req_socket.close()
        context.term()

        logger.debug("zeromq cleanup complete")

