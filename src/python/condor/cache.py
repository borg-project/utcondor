import os
import os.path
import shutil
import cPickle as pickle
import condor

class DiskCache(object):
    """Use the filesystem as the key-value store that it is."""

    def __init__(self, root, threshold = 1024):
        self._root = os.path.abspath(root)
        self._threshold = threshold
        self._small = {}

        if not os.path.exists(self._root):
            os.mkdir(self._root, 0700)

    def __getitem__(self, key):
        uncompressed = self._small.get(key)

        if uncompressed is None:
            with open(self._pickle_path(key)) as pickle_file:
                uncompressed = condor.decompress(pickle_file.read())

        return pickle.loads(uncompressed)

    def __setitem__(self, key, value):
        assert key == id(value)

        if key not in self._small:
            path = self._pickle_path(key)

            if not os.path.exists(path):
                pickled = pickle.dumps(value)

                if len(pickled) > self._threshold:
                    with open(path, "wb") as pickle_file:
                        pickle_file.write(condor.compress(pickled))
                else:
                    self._small[key] = pickled

        return value

    def _pickle_path(self, key):
        assert isinstance(key, int)

        extension = condor.compression_extension

        return os.path.join(self._root, "{0}.pickle.{1}".format(key, extension))

    def fork(self):
        return DiskCache(self._root, threshold = self._threshold)

    def delete(self):
        shutil.rmtree(self._root)

