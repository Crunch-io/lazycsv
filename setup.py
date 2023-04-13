import os.path

from setuptools import Extension, find_packages, setup

extensions = [
    Extension(
        "lazycsv.lazycsv",
        [os.path.join("src", "lazycsv", "lazycsv.c")],
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
