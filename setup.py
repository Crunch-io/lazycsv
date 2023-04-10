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
    version="0.0.1",
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
    package_dir={"": "src"},
    ext_modules=extensions,
)
