import setuptools

setuptools.setup(
    # actual setup
    packages = setuptools.find_packages(),
    install_requires = [
        "plac>=0.9.1",
        "pyzmq>=2.2.0.1",
        ],
    scripts = [ "scripts/utcondor-run", "scripts/utcondor-ipcluster" ],
    # description
    name = "utcondor",
    version = "0.4.2",
    author = "Bryan Silverthorn",
    author_email = "bcs@cargo-cult.org",
    description = "tools for distributed computing at UTCS",
    url = "https://github.com/bsilverthorn/utcondor",
    keywords = "distributed condor UT UTexas cluster",
    license = "MIT",
    classifiers = [
        "Development Status :: 4 - Beta",
        "Operating System :: Unix",
        "Programming Language :: Python :: 2.6",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities"
        ],
    )

