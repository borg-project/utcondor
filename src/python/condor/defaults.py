"""@author: Bryan Silverthorn <bcs@cargo-cult.org>"""

condor_matching = "InMastodon && (Arch == \"X86_64\") && (OpSys == \"LINUX\") && (Memory > 1024)"

try:
    from condor_site_defaults import *
except ImportError:
    pass

