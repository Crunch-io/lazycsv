# lazycsv - an OOM csv parser

##### Developers: Michael Green, Chris Perkins

lazycsv is a C implementation of a csv parser for python. The aim of this
parser is to provide for fast extraction of sequences of data from a CSV file
in a memory-efficient manner, with zero dependencies.

LazyCSV utilizes memory mapped files and iterators to parse the file without
persisting any significant amounts of data to physical memory. The design
allows a user to generate PyObject's from a csv file lazily.

The parser works as follows:

First, The user file is memory-mapped internally to the LazyCSV object. That
file is used to generate two indexes; the first an index of each of the line
endings in the CSV, and the second an index of the fields for each value in the
CSV, relative to the previous line ending. These index files are then also
memory-mapped, and finally the LazyCSV object is returned to the user.

When a user requests a sequence of data (i.e. a row or a column), an iterator
is created and returned. This iterator uses the value of the requested sequence
and its internal position state to index into the index files the values
representing the index of the requested field, and its length. Those two values
are then used to create a single PyBytes object. These PyBytes objects are then
yielded to the user per iteration.

This process is lazy, only yielding data from the user file as the iterator is
consumed. It does not cache results as they are generated - it is the
responsibility of the user to store in physical memory the data which must be
persisted. The only persisted overhead in physical memory is the LazyCSV object
itself, any created iterators, and optionally the headers of the CSV file.

```python
>>> import os.path
>>> from lazycsv import lazycsv
>>> HERE = os.path.abspath(os.path.dirname(__name__))
>>> FPATH = os.path.join(HERE, "tests/fixtures/file.csv")
>>> lazy = lazycsv.LazyCSV(FPATH)
>>> lazy
<lazycsv.LazyCSV object at 0x7f5b212ea3d0>
>>> (col := lazy.sequence(col=0))
<lazycsv_iterator object at 0x7f5b212ea420>
>>> next(col)
b'0'
>>> next(col)
b'1'
>>> next(col)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
StopIteration
```

Since data is yielded per-iteraton, lazycsv pairs well with many of the builtin
functional components of Python. This has the added benefit of keeping the
iteration in the C level, maximizing performance.

```python
>>> col = lazy.sequence(col=0)
>>> list(map(int, col))
[0, 1]
>>> row = lazy.sequence(row=1)
list(map(lambda x: x.decode('utf8'), row))
['1', 'a1', 'b1']
```

Headers are by default parsed from the csv file and packaged into a tuple under
a `.headers` attribute. This can be skipped by passing `skip_headers=True` to
the object constructor. Skipping the header parsing step results in the header
value being included in the iterator.

*Note: `lazycsv` makes no effort to deduplicate headers and it is the
responsibility of the user to make sure that columns are properly named.*

```python
>>> lazy.headers
(b'', b'ALPHA', b'BETA')
>>> (col := lazy.sequence(col=1))
<lazycsv_iterator object at 0x7fdce5cee830>
>>> list(col)
[b'a0', b'a1']
>>> lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
>>> (col := lazy.sequence(col=1))
<lazycsv_iterator object at 0x7fdce5cee830>
>>> list(col)
[b'ALPHA', b'a0', b'a1']
```

Fields which are double-quoted by default are yielded without quotes. This
behavior can be disabled by passing `unquoted=False` to the object constructor.

```python
>>> lazy = lazycsv.LazyCSV(
...     os.path.join(HERE, "tests/fixtures/file_crlf2.csv")
... )
>>> lazy.headers
(b'', b'This,that', b'Fizz,Buzz')
>>> lazy = lazycsv.LazyCSV(
...     os.path.join(HERE, "tests/fixtures/file_crlf2.csv"), unquote=False
... )
>>> lazy.headers
(b'', b'"This,that"', b'"Fizz,Buzz"')
```

### Benchmarks (CPU)

```
root@f2612b113d10:/code# python tests/benchmark_lazy.py
filesize: 0.134gb
cols=10000
rows=10000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 0.7147269690176472
parsing cols... time to parse: 0.995767368003726
total time: 1.7104943370213732

benchmarking polars:
creating polars df... time to object: 2.505466743001307
parsing cols... time to parse: 1.3182334439989063
total time: 3.823700187000213

benchmarking datatable:
100% |██████████████████████████████████████████████████| Reading data [done]
creating datatables frame... time to object: 0.4007758339994325
parsing cols... time to parse: 3.6466693760012276
total time: 4.04744521000066
```

```
root@f2612b113d10:/code# python tests/benchmark_lazy.py
filesize: 1.387gb
cols=10000
rows=100000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 7.119601220998447
parsing cols... time to parse: 11.85147767199669
total time: 18.971078892995138

benchmarking polars:
creating polars df... time to object: 23.82051394299924
parsing cols... time to parse: 12.816952474997379
total time: 36.63746641799662

benchmarking datatable:
100% |██████████████████████████████████████████████████| Reading data [done]
creating datatables frame... time to object: 2.3516600279999693
parsing cols... time to parse: 35.72737046200018
total time: 38.07903049000015
```

```
root@f2612b113d10:/code# python tests/benchmark_lazy.py
filesize: 14.333gb
cols=100000
rows=100000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 72.66899809299503
parsing cols... time to parse: 260.63739303901093
total time: 333.30639113200596

benchmarking polars:
Killed

benchmarking datatable:
 58% |█████████████████████████████▍                    | Reading data Killed
```

