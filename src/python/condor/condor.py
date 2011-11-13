"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import re
import os
import os.path
import sys
import datetime
import subprocess
import cargo
import cStringIO as StringIO

logger = cargo.get_logger(__name__, level = "INFO")

class CondorSubmission(object):
    """Stream output to a Condor submission file."""

    def __init__(self):
        self._out = StringIO.StringIO()

    def blank(self, lines = 1):
        """Write a blank line or many."""

        for i in xrange(lines):
            self._out.write("\n")

        return self

    def pair(self, name, value):
        """Write a variable assignment line."""

        self._out.write("%s = %s\n" % (name, value))

        return self

    def pairs(self, **kwargs):
        """Write a block of variable assignment lines."""

        self.pairs_dict(kwargs)

        return self

    def pairs_dict(self, pairs):
        """Write a block of variable assignment lines."""

        max_len = max(len(k) for (k, _) in pairs.iteritems())

        for (name, value) in pairs.iteritems():
            self.pair(name.ljust(max_len), value)

        return self

    def environment(self, **kwargs):
        """Write an environment assignment line."""

        self._out.write("environment = \\\n")

        pairs = sorted(kwargs.items(), key = lambda (k, _): k)

        for (i, (key, value)) in enumerate(pairs):
            self._out.write("    %s=%s;" % (key, value))

            if i < len(pairs) - 1:
                self._out.write(" \\")

            self._out.write("\n")

        return self

    def header(self, header):
        """Write commented header text."""

        dashes = "-" * len(header)

        self.comment(dashes)
        self.comment(header.upper())
        self.comment(dashes)

        return self

    def comment(self, comment):
        """Write a line of commented text."""

        self._out.write("# %s\n" % comment)

        return self

    def queue(self, count):
        """Write a queue instruction."""

        self._out.write("Queue %i\n" % count)

        return self

    @property
    def contents(self):
        """The raw contents of the file."""

        return self._out.getvalue()

def condor_submit(submit_path):
    """Submit to condor; return the cluster number."""

    (stdout, stderr) = cargo.check_call_capturing(["/usr/bin/env", "condor_submit", submit_path])
    expression = r"(\d+) job\(s\) submitted to cluster (\d+)\."
    match = re.match(expression , stdout.splitlines()[-1])

    if match:
        (jobs, cluster) = map(int, match.groups())

        logger.info("submitted %i condor jobs to cluster %i", jobs, cluster)

        return cluster
    else:
        raise RuntimeError("failed to submit to condor:%s" % stdout)

def condor_rm(specifier):
    """Kill condor job(s)."""

    logger.info("killing condor jobs matched by %s", specifier)

    try:
        cargo.check_call_capturing(["condor_rm", str(specifier)])
    except subprocess.CalledProcessError:
        return False
    else:
        return True

def condor_hold(specifiers):
    """Hold condor job(s)."""

    logger.info("holding condor job(s) matched by %s", specifiers)

    try:
        cargo.check_call_capturing(["condor_hold"] + map(str, specifiers))
    except subprocess.CalledProcessError:
        return False
    else:
        return True

def condor_release(specifiers):
    """Release condor job(s)."""

    logger.info("releasing condor job(s) matched by %s", specifiers)

    try:
        cargo.check_call_capturing(["condor_release"] + map(str, specifiers))
    except subprocess.CalledProcessError:
        return False
    else:
        return True

def default_condor_home():
    return "workers/%s" % datetime.datetime.now().replace(microsecond = 0).isoformat()

def submit_condor_workers(
    workers,
    req_address,
    matching = cargo.defaults.condor_matching,
    description = "distributed Python worker process(es)",
    group = "GRAD",
    project = "AI_ROBOTICS",
    condor_home = default_condor_home(),
    ):
    # prepare the working directories
    working_paths = [os.path.join(condor_home, "%i" % i) for i in xrange(workers)]

    for working_path in working_paths:
        os.makedirs(working_path)

    # provide a convenience symlink
    link_path = "workers-latest"

    if os.path.lexists(link_path):
        os.unlink(link_path)

    os.symlink(condor_home, link_path)

    # write the submit file, starting with job matching
    submit = CondorSubmission()

    if matching:
        submit \
            .header("matching") \
            .blank() \
            .pair("requirements", matching) \
            .blank()

    # write the general condor section
    submit \
        .header("configuration") \
        .blank() \
        .pairs_dict({
            "+Group": "\"%s\"" % group,
            "+Project": "\"%s\"" % project,
            "+ProjectDescription": "\"%s\"" % description,
            }) \
        .blank() \
        .pairs(
            universe = "vanilla",
            notification = "Error",
            kill_sig = "SIGINT",
            Log = "condor.log",
            Error = "condor.err",
            Output = "condor.out",
            Input = "/dev/null",
            Executable = os.environ.get("SHELL"),
            ) \
        .blank() \
        .environment(
            CARGO_LOG_FILE_PREFIX = "log",
            PATH = os.environ.get("PATH", ""),
            PYTHONPATH = os.environ.get("PYTHONPATH", ""),
            LD_LIBRARY_PATH = os.environ.get("LD_LIBRARY_PATH", ""),
            ) \
        .blank()

    # write the jobs section
    submit \
        .header("jobs") \
        .blank()

    for working_path in working_paths:
        arg_format = '"-c \'%s ""$0"" $@\' -m cargo.tools.labor.work2 %s $(Cluster).$(Process)"'

        submit \
            .pairs(
                Initialdir = working_path,
                Arguments = arg_format % (sys.executable, req_address),
                ) \
            .queue(1) \
            .blank()

    # submit the job to condor
    submit_path = os.path.join(condor_home, "workers.condor")

    with open(submit_path, "w") as submit_file:
        submit_file.write(submit.contents)

    return cargo.condor_submit(submit_path)

