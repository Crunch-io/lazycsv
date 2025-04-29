import sys

from lazycsv import lazycsv

file = sys.argv[1]

lazy = lazycsv.LazyCSV(file)

data = [
    list(lazy[:, i])
    for i in range(lazy.cols)
]
