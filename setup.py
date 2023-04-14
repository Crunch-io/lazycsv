import os

from setuptools import Extension, find_packages, setup

LAZYCSV_DEBUG = int(os.environ.get("LAZYCSV_DEBUG", 0))
LAZYCSV_INDEX_DTYPE = os.environ.get("LAZYCSV_INDEX_DTYPE", "short")

# TODO: use macros to include numpy as optional dependency for fast np.array
# materialization
LAZYCSV_INCLUDE_NUMPY = int(os.environ.get("LAZYCSV_INCLUDE_NUMPY", 0))

include_dirs = (
    [__import__("numpy").get_include()]
    if LAZYCSV_INCLUDE_NUMPY
    else []
)

extensions = [
    Extension(
        "lazycsv.lazycsv",
        [os.path.join("src", "lazycsv", "lazycsv.c")],
        include_dirs=include_dirs,
        define_macros=[
            ("INDEX_DTYPE", LAZYCSV_INDEX_DTYPE),
            ("INCLUDE_NUMPY", LAZYCSV_INCLUDE_NUMPY),
            ("DEBUG", LAZYCSV_DEBUG)
        ],
    )
]

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="lazycsv",
    version="1.0.0",
    author="Michael Green, Chris Perkins",
    author_email="dev@crunch.io",
    description="an OOM csv parser",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(where="src"),
    extras_require={
        "test": ["pytest"],
        "benchmarks": ["datatable", "pandas", "pyarrow", "polars"],
    },
    classifiers=[
        "Development Status :: 5 - Production",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython"
        "Topic :: Utilities",
    ],
    package_dir={"": "src"},
    ext_modules=extensions,
)
