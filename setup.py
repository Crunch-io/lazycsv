import os

from setuptools import Extension, find_packages, setup

LAZYCSV_DEBUG = int("LAZYCSV_DEBUG" in os.environ)
LAZYCSV_INDEX_DTYPE = os.environ.get("LAZYCSV_INDEX_DTYPE", "uint16_t")

LAZYCSV_INCLUDE_NUMPY = int("LAZYCSV_INCLUDE_NUMPY" in os.environ)
LAZYCSV_INCLUDE_NUMPY_LEGACY = int("LAZYCSV_INCLUDE_NUMPY_LEGACY" in os.environ)

include_dirs = (
    [__import__("numpy").get_include()]
    if (LAZYCSV_INCLUDE_NUMPY | LAZYCSV_INCLUDE_NUMPY_LEGACY)
    else []
)

if not LAZYCSV_INDEX_DTYPE.startswith(("unsigned", "uint")):
    raise ValueError("specified LAZYCSV_INDEX_DTYPE must be an unsigned integer type")

extensions = [
    Extension(
        "lazycsv.lazycsv",
        [os.path.join("src", "lazycsv", "lazycsv.c")],
        include_dirs=include_dirs,
        define_macros=[
            ("INDEX_DTYPE", LAZYCSV_INDEX_DTYPE),
            ("INCLUDE_NUMPY", LAZYCSV_INCLUDE_NUMPY),
            ("INCLUDE_NUMPY_LEGACY", LAZYCSV_INCLUDE_NUMPY_LEGACY),
            ("DEBUG", LAZYCSV_DEBUG),
        ],
    )
]

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="lazycsv",
    version="1.1.4",
    author="Michael Green, Chris Perkins",
    author_email="dev@crunch.io",
    description="an fast, memory efficient csv parser",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(where="src"),
    extras_require={
        "test": ["pytest", "numpy"],
        "benchmark": ["datatable", "pandas", "pyarrow", "polars"],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Utilities",
    ],
    package_dir={"": "src"},
    ext_modules=extensions,
)
