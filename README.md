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
file is used to generate an index, which is a binary tempfile representing the
delta between the natural index and the index of a commas in the user file. The
index file is then also memory-mapped, and finally the LazyCSV object is
returned to the user.

When a user requests a sequence of data (i.e. a row or a column), an iterator
is created and returned. This iterator uses the value of the requested sequence
and its internal position state to index into the index file the values
representing the delta between the position state and the index of the
enclosing commas for a target byte array. That byte array is then used to
create a single PyBytes object. These PyBytes objects are then yielded to the
user per iteration.

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
indexing lazy... time to index: 0.9846110779999435
parsing cols... time to parse: 1.4814507720000165
total time: 2.46606184999996

benchmarking datatable:
100% |██████████████████████████████████████████████████| Reading data [done]
creating datatables frame... time to object: 0.41146984300007716
parsing cols... time to parse: 3.753832629000044
total time: 4.165302472000121
```

```
``root@f2612b113d10:/code# python tests/benchmark_lazy.py
filesize: 1.387gb
cols=10000
rows=100000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 8.608913677999908
parsing cols... time to parse: 21.72626765100017
total time: 30.33518132900008

benchmarking datatable:
100% |██████████████████████████████████████████████████| Reading data [done]
creating datatables frame... time to object: 2.3598619169999893
parsing cols... time to parse: 36.83905054699994
total time: 39.19891246399993
```

```
root@f2612b113d10:/code# python tests/benchmark_lazy.py
filesize: 14.333gb
cols=100000
rows=100000
sparsity=0.95

benchmarking lazycsv:
indexing lazy... time to index: 109.6241959240001
parsing cols... time to parse: 607.3056542300001
total time: 716.9298501540002

benchmarking datatable:
 58% |█████████████████████████████▍                    | Reading data Killed

root@629d8d31f3f0:/code# python tests/benchmark_lazy.py # memory_limit=10**8
filesize: 14.333gb
cols=100000
rows=100000
sparsity=0.95

benchmarking datatable:
100% |██████████████████████████████████████████████████| Reading data [done]
creating datatables frame... time to object: 1606.480218615994
parsing cols... time to parse: 6937.707541549986
total time: 8544.18776016598
```

