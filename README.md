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
file is used to generate three indexes. The first is an index of values which
correspond to the position in the user file where a given CSV field starts.
This value is always between 0 and 255, so as to minimize the size of this
index file. For index values outside this range, An "anchor point" is created,
which is a pair of `size_t` values that mark both the value which is subtracted
from the index value such that the index value fits within 8 bits, and the
first column of the CSV where the anchor value applies. This anchor point is
periodically written to the second index file when required for a given index.
Finally, the third index writes the index of the first anchor point for each
row of the file.

When a user requests a sequence of data (i.e. a row or a column), an iterator
is created and returned. This iterator uses the value of the requested sequence
and its internal position state to index into the index files the values
representing the index of the requested field, and its length. Those two values
are then used to create a single PyBytes object. These PyBytes objects are then
yielded to the user per-iteration.

This process is lazy, only yielding data from the user file as the iterator is
consumed. It does not cache results as they are generated - it is the
responsibility of the user to store in physical memory the data which must be
persisted. The only persisted overhead in physical memory is the LazyCSV object
itself, any created iterators, a small cache of common length-0 and length-1
`PyObject*`'s for fast returns, and optionally the headers of the CSV file.

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
root@aa9d7c7ffb59:/code# python tests/benchmark_lazy.py
filesize: 0.134gb
cols=10000
rows=10000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 0.44364099399899715
parsing cols... time to parse: 3.0821814209994045
total time: 3.5258224149984017

benchmarking datatable:
100% |██████████████████████████████████████████████████| Reading data [done]
creating datatables frame... time to object: 0.40828132900060154
parsing cols... time to parse: 3.810204313998838
total time: 4.21848564299944

benchmarking polars (read):
creating polars df... time to object: 2.357821761001105
parsing cols... time to parse: 1.3874979300017003
total time: 3.7453196910028055
```

```
root@aa9d7c7ffb59:/code# python tests/benchmark_lazy.py
filesize: 1.387gb
cols=10000
rows=100000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 4.054390757999499
parsing cols... time to parse: 65.48354616199867
total time: 69.53793691999817

benchmarking datatable:
100% |██████████████████████████████████████████████████| Reading data [done]
creating datatables frame... time to object: 2.4456441220027045
parsing cols... time to parse: 37.424315700998704
total time: 39.86995982300141

benchmarking polars (read):
creating polars df... time to object: 22.383294907001982
parsing cols... time to parse: 14.16580996599805
total time: 36.54910487300003
```

```
filesize: 14.333gb
cols=100000
rows=100000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 45.171728016001
parsing cols... time to parse: 1347.6434365789974
total time: 1392.8151645949984

benchmarking datatable:
 58% |█████████████████████████████▍                    | Reading data Killed

benchmarking polars (read):
Killed
```
