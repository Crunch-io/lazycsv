import os.path

from lazycsv import lazycsv

HERE = os.path.abspath(os.path.dirname(__file__))
FPATH = os.path.join(HERE, "fixtures/file.csv")

lazy = lazycsv.LazyCSV(FPATH)
