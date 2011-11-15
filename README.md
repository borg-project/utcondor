utcondor
========

This package provides Python tools for distributed computing on the [Condor](http://www.cs.wisc.edu/condor/) platform at [UTCS](http://www.cs.utexas.edu/). These tools probably won't be useful to you unless you're a computer science graduate student at the University of Texas at Austin.

Overview
--------

Many simple distributed computing tasks can be modeled as a parallel map: some function is executed over some set of inputs, and each result is handled as it becomes available. This model is the one provided by utcondor. For example, to square a range of numbers in distributed fashion:

    import condor
    import this_module

    if __name__ == "__main__":
        this_module.main()

    def square(x):
        return x**2

    def main():
        jobs = [(square, [x]) for x in range(16)]

        def handle_result(task, xx):
            print task.args, xx

        condor.do_or_distribute(jobs, 4, handle_result)

Installation
------------

Install the two dependencies; `pyzmq-static` is probably the easiest way to install the Python bindings to [ØMQ](http://www.zeromq.org/):

    pip install plac
    pip install pyzmq-static

Then use waf:

    ./waf configure
    ./waf install

Caveat Emptor
-------------

License
-------

This software package is provided under the non-copyleft open-source "MIT"
license. The complete legal notice can be found in the included LICENSE file.

