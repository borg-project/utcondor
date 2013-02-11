utcondor
========

The utcondor library provides Python tools for distributed computing on the
[Condor](http://www.cs.wisc.edu/condor/) platform at
[UTCS](http://www.cs.utexas.edu/). These tools probably won't be useful to you
unless you're a computer science graduate student at the University of Texas at
Austin.

The goal of utcondor is a reliable implementation of a simple distributed
computing model. It requires little boilerplate, and switches easily between
local and remote execution.

Overview
--------

The utcondor library supports distributed computing tasks that can be cast as a
one-level parallel map: a function executed over multiple inputs on multiple
machines. This model is primitive, but easy to apply and often good enough. For
a trivial example, to square a range of numbers in distributed fashion:

```python
import condor

def f(x):
    return x**2

def main():
    calls = [(f, [x]) for x in range(16)]

    for (call, result) in condor.do(calls, 4):
        print call.args, result

if __name__ == "__main__":
    main()
```

Any arguments passed to the remotely-executed callable must be pickleable.

Installation
------------

First, make sure that the condor binaries are accessible. Does "condor_q" run?
If not, you may want to add /lusr/opt/condor/bin to your PATH.

Then you can install the package. The latest release is pip-installable, so

```sh
$ pip install utcondor
```

should work.

You're running inside a [virtualenv](http://pypi.python.org/pypi/virtualenv),
right?

Caveat Emptor
-------------

Be careful. Pay attention to whether Condor jobs are being successfully cleaned
up. Use at your own risk.

Credits
-------

The primary author is Bryan Silverthorn <bcs@cargo-cult.org>.

License
-------

This software package is provided under the non-copyleft open-source "MIT"
license. The complete legal notice can be found in the included LICENSE file.

