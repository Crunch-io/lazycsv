import os

from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext

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

_MACRO_STAMP = (
    "INDEX_DTYPE={} INCLUDE_NUMPY={} INCLUDE_NUMPY_LEGACY={} DEBUG={}"
    .format(LAZYCSV_INDEX_DTYPE, LAZYCSV_INCLUDE_NUMPY,
            LAZYCSV_INCLUDE_NUMPY_LEGACY, LAZYCSV_DEBUG)
)


class build_ext_force_on_macro_change(build_ext):
    """Force a rebuild when compile-time macros change."""

    def build_extensions(self):
        stamp_path = os.path.join(self.build_temp, ".lazycsv_macros")
        os.makedirs(self.build_temp, exist_ok=True)
        prev = ""
        if os.path.exists(stamp_path):
            with open(stamp_path) as f:
                prev = f.read().strip()
        if prev != _MACRO_STAMP:
            self.force = True
            with open(stamp_path, "w") as f:
                f.write(_MACRO_STAMP)
        super().build_extensions()

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
    version="1.2.1",
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Utilities",
    ],
    package_dir={"": "src"},
    ext_modules=extensions,
    cmdclass={"build_ext": build_ext_force_on_macro_change},
)
