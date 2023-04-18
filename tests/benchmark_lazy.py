import random
import itertools
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


def run_sqlite(fpath):
    import sqlite3, csv
    tempdir = tempfile.TemporaryDirectory()
    print("creating database... ", end="\r")
    ti = perf_counter()
    conn = sqlite3.connect(os.path.join(tempdir.name, "data.db"))
    cur = conn.cursor()
    headers, sql, chunks = [], "", []
    with open(fpath, 'r') as f:
        reader = csv.reader(f)
        for r in reader:
            if not headers:
                headers = ', '.join(r)
                cur.execute(f"CREATE TABLE t ({headers});")
                conn.commit()
                sql = f"INSERT INTO t ({headers}) VALUES ({', '.join('?'*len(r))});"
                headers = r
            chunks.append(r)
            if len(chunks) > 10000:
                cur.executemany(sql, chunks)
                conn.commit()
                chunks.clear()
    if chunks:
        cur.executemany(sql, chunks)
        conn.commit()
    te = perf_counter()
    print(f"creating database... time to db: {te-ti}")
    for i, c in enumerate(headers):
        sql = f"SELECT ({c}) from t;"
        # col = tuple(cur.execute(sql))
        col = list(itertools.chain(*cur.execute(sql)))
        if i % 100 == 0:
            print(f"parsing cols... {i}/{len(headers)}", end="\r")
        del col
    tf = perf_counter()
    print(f"parsing cols... time to parse: {tf-te}")
    cur.close()
    conn.close()
    print(f"\ntotal time: {tf-ti}")


def run_lazy(fpath):
    from lazycsv import lazycsv
    print("indexing lazy... ", end="\r")
    ti = perf_counter()
    lazy = lazycsv.LazyCSV(fpath)
    te = perf_counter()
    print(f"indexing lazy... time to index: {te-ti}")
    for c in range(lazy.cols):
        col = list(lazy.sequence(col=c))
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


def run_polars_read(fpath):
    import polars as pl
    print("creating polars df...", end="\r")
    ti = perf_counter()
    table = pl.read_csv(fpath)
    te = perf_counter()
    print(f"creating polars df... time to object: {te-ti}")
    for c in range(table.shape[1]):
        col = table[:, c].to_list()
        if c % 100 == 0:
            print(f"parsing cols... {c}/{table.shape[1]}", end="\r")
        del col
    tf = perf_counter()
    print(f"parsing cols... time to parse: {tf-te}", end="\r")
    del table
    print(f"\ntotal time: {tf-ti}")


def run_polars_scan(fpath):
    import polars as pl
    print("creating polars df...", end="\r")
    ti = perf_counter()
    table = pl.scan_csv(fpath, rechunk=False)
    te = perf_counter()
    print(f"creating polars df... time to object: {te-ti}")
    cols = len(table.columns)
    for i, c in enumerate(table.columns):
        col = tuple(
            table
            .select(c)
            .collect()
            .to_dict(as_series=False)
            .values()
        )
        if i % 100 == 0:
            print(f"parsing cols... {i}/{cols}", end="\r")
        del col
    tf = perf_counter()
    print(f"parsing cols... time to parse: {tf-te}", end="\r")
    del table
    print(f"\ntotal time: {tf-ti}")


def main():
    cols = 1000
    rows = 10000
    sparsity = 0.95
    benchmarks = {
        "lazycsv": run_lazy,
        # "pandas": run_pandas,
        # "pyarrow": run_pyarrow,
        # "datatable": run_datatable,
        # "polars (read)": run_polars_read,
        # "polars (scan)": run_polars_scan,
        # "sqlite": run_sqlite
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
                f"{i}x{j}" if random.random() > sparsity else "" for j in range(cols)
            )
            tempf.write((row + "\n").encode("utf8"))
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

