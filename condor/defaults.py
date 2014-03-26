"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

import os

# default to putting utcondor files in ~/utcondor.

home = os.path.abspath(os.path.expanduser(os.path.join('~', 'utcondor')))

# we add a Memory requirement to prevent Condor from adding an automatic
# Memory >= ImageSize constraint, since its ImageSize detection is poor.

condor_matching = "InMastodon && (Arch == \"X86_64\") && (OpSys == \"LINUX\") && (Memory > 1024)"

try:
    from condor_site_defaults import *
except ImportError:
    pass

# if $UTCONDOR_HOME is set, use the value of the environment variable in
# preference to whatever settings were defined above.

try:
    home = os.environ['UTCONDOR_HOME']
except KeyError:
    pass
