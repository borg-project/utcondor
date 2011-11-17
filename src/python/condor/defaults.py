"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

# we add a Memory requirement to prevent Condor from adding an automatic
# Memory >= ImageSize constraint, since its ImageSize detection is poor.

condor_matching = "InMastodon && (Arch == \"X86_64\") && (OpSys == \"LINUX\") && (Memory > 1024)"

try:
    from condor_site_defaults import *
except ImportError:
    pass

