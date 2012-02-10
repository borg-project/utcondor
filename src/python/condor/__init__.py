"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import random
import cPickle as pickle
import condor

from . import log

logger = condor.log.get_logger(__name__, level = "INFO")

from . import defaults
from . import raw
from . import cache
from . import managers
from . import messages

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
    compressed = condor.compress(pickled)

    zmq_socket.send(compressed)

def recv_pyobj_compressed(zmq_socket):
    compressed = zmq_socket.recv()
    decompressed = condor.decompress(compressed)
    unpickled = pickle.loads(decompressed)

    return unpickled

def do(requests, workers, local = False):
    """Do work remotely or locally; yield result pairs."""

    tasks = sorted(map(condor.managers.Task.from_request, requests), key = lambda _: random.random())

    if workers == "auto":
        workers = min(256, len(tasks))
    else:
        workers = int(workers)

    if workers > 0:
        if local:
            manager = condor.managers.ParallelManager(tasks, workers)
        else:
            manager = condor.managers.DistributedManager(tasks, workers)
    else:
        manager = condor.managers.SerialManager(tasks)

    try:
        for item in manager.manage():
            yield item
    finally:
        manager.clean()

def do_for(requests, workers, handler = lambda _, x: x, local = False):
    """Do work remotely or locally; apply handler to result pairs."""

    for (task, result) in do(requests, workers, local = local):
        handler(task, result)

