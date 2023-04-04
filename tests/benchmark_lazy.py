import random
import os.path
import tempfile

from time import perf_counter


def get_size(file_path, unit="bytes"):
    file_size = os.path.getsize(file_path)
    exponents_map = {"bytes": 0, "kb": 1, "mb": 2, "gb": 3}
    if unit not in exponents_map:
        raise ValueError("Must select from ['bytes', 'kb', 'mb', 'gb']")
    else:
        size = file_size / 1024 ** exponents_map[unit]
        return round(size, 3)


def run_lazy(fpath):
    from lazycsv import lazycsv

    print("indexing lazy... ", end="\r")

    ti = perf_counter()
    lazy = lazycsv.LazyCSV(fpath)
    te = perf_counter()

    print(f"indexing lazy... time to index: {te-ti}")

    for c in range(lazy.cols):
        col = lazy.sequence(col=c).materialize(tuple)
        if c % 100 == 0:
            print(f"parsing cols... {c}/{lazy.cols}", end="\r")
        del col

    tf = perf_counter()

    print(f"parsing cols... time to parse: {tf-te}", end="\r")

    del lazy

    print(f"\ntotal time: {tf-ti}")


def run_datatable(fpath):
    import datatable as dt

    print("creating datatables frame...", end="\r")

    ti = perf_counter()
    frame = dt.fread(fpath)
    te = perf_counter()

    print(f"creating datatables frame... time to object: {te-ti}")

    for c in range(frame.ncols):
        col = frame[c].to_list()
        if c % 100 == 0:
            print(f"parsing cols... {c}/{frame.ncols}", end="\r")
        del col

    tf = perf_counter()

    print(f"parsing cols... time to parse: {tf-te}", end="\r")

    del frame

    print(f"\ntotal time: {tf-ti}")


def run_pandas(fpath):
    import pandas as pd

    print("creating pandas dataframe...", end="\r")

    ti = perf_counter()
    df = pd.read_csv(fpath)
    te = perf_counter()

    print(f"creating pandas dataframe... time to object: {te-ti}")

    for i, c in enumerate(df.columns):
        col = df[c]
        if i % 100 == 0:
            print(f"parsing col: {c}", end="\r")
        del col

    te = perf_counter()

    del df

    print(f"\ntotal time: {te-ti}")


def run_pyarrow(fpath):
    from pyarrow import csv as pa_csv

    print("creating pyarrow table...", end="\r")

    ti = perf_counter()
    table = pa_csv.read_csv(fpath)
    te = perf_counter()

    print(f"creating pyarrow table... time to object: {te-ti}")

    for c in range(table.num_columns):
        col = table[c].to_pylist()
        if c % 100 == 0:
            print(f"parsing cols... {c}/{table.num_columns}", end="\r")
        del col

    tf = perf_counter()

    print(f"parsing cols... time to parse: {tf-te}", end="\r")

    del table

    print(f"\ntotal time: {tf-ti}")

def main():
    cols = 200000
    rows = 3000000

    cols = 100000
    rows = 100000
    sparsity = 0.95

    benchmarks = {
        # "pandas": run_pandas,
        # "pyarrow": run_pyarrow,
        "lazycsv": run_lazy,
        "datatable": run_datatable,
    }

    filename = f"benchmark_{rows}r_{cols}c_{int(sparsity*100)}%.csv"

    HERE = os.path.abspath(os.path.dirname(__file__))
    filepath = os.path.join(HERE, f"fixtures/benchmarks/{filename}")

    if os.path.isfile(filepath):
        name = filepath
        tempf = None
    else:
        tempf = tempfile.NamedTemporaryFile()

        headers = ",".join(f"col_{i}" for i in range(cols)) + "\n"
        tempf.write(headers.encode("utf8"))

        i = 0
        for i in range(rows):
            row = ",".join(
                f"{i}x{j}" if random.random() > sparsity else ""
                for j in range(cols)
            )
            tempf.write((row+"\n").encode("utf8"))
            del row
            if i % 100 == 0:
                print(f"writing rows: {i}/{rows}", end="\r")
        print(f"writing rows: {i}/{rows}")

        tempf.flush()
        name = tempf.name

        if input("copy to benchmarks? [Y/n]: ") in {"Y", ""}:
            __import__("shutil").copyfile(tempf.name, filepath)

    path = os.path.abspath(name)

    print(f"filesize: {get_size(name, 'gb')}gb")
    print(f"{cols=}")
    print(f"{rows=}")
    print(f"{sparsity=}")

    for name, fn in benchmarks.items():
        print(f"\nbenchmarking {name}:")
        fn(path)

    if tempf:
        tempf.close()


if __name__ == "__main__":
    main()
