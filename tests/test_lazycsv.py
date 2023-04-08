import contextlib
import os
import os.path
import tempfile
import textwrap

from lazycsv import lazycsv

import numpy as np
import pytest

HERE = os.path.abspath(os.path.dirname(__file__))
FPATH = os.path.join(HERE, "fixtures/file.csv")


@pytest.fixture
def lazy():
    lazy = lazycsv.LazyCSV(FPATH)
    yield lazy

@pytest.fixture
def file_1000r_1000c():
    tempf = tempfile.NamedTemporaryFile()
    cols, rows = 1000, 1000
    headers = ",".join(f"col_{i}" for i in range(cols)) + "\n"
    tempf.write(headers.encode("utf8"))
    for _ in range(rows):
        row = ",".join(f"{j}" for j in range(cols)) + "\n"
        tempf.write(row.encode("utf8"))
    tempf.flush()
    yield tempf
    tempf.close()


@contextlib.contextmanager
def prepped_file(actual):
    tempf = tempfile.NamedTemporaryFile()
    tempf.write(actual)
    tempf.flush()
    yield tempf
    tempf.close()


def test_demo():
    actual = b"INDEX,A,B\n0,,2\n,,5"
    with prepped_file(actual) as tempf:
        lazy = lazycsv.LazyCSV(tempf.name)
        data = tuple(
            tuple(lazy.sequence(col=c))
            for c in range(lazy.cols)
        )
    assert data == ((b'0', b''), (b'', b''), (b'2', b'5'))


class TestLazyCSV:
    def test_headers(self, lazy):
        assert lazy.name == os.path.abspath(FPATH)
        assert lazy.headers == (b"", b"ALPHA", b"BETA")

    def test_more_headers(self):
        actual = b"INDEX,,AA,B,CC,D,EE\n0,1,2,3,4,5,6\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
        assert lazy.headers == (b"INDEX", b"", b"AA", b"B", b"CC", b"D", b"EE")

    def test_headers_empty_index(self):
        actual = b",AA,B,CC,D,EE\n0,1,2,3,4,\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
        assert lazy.headers == (b"", b"AA", b"B", b"CC", b"D", b"EE")

    def test_initial_parse(self, lazy):
        assert lazy.rows, lazy.cols == (2, 3)

    def test_initial_parse_skip_headers(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        assert lazy.rows, lazy.cols == (3, 3)
        assert lazy.headers == ()

    def test_get_column(self, lazy):
        actual = list(lazy.sequence(col=0))
        assert actual == [b"0", b"1"]
        actual = list(lazy.sequence(col=1))
        assert actual == [b"a0", b"a1"]
        actual = list(lazy.sequence(col=2))
        assert actual == [b"b0", b"b1"]

    def test_get_actual_col(self):
        actual = b"INDEX,ATTR\n0,a\n1,b\n2,c\n3,d\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            assert list(lazy.sequence(col=0)) == [b'0', b"1", b"2", b"3"]
            assert list(lazy.sequence(col=1)) == [b'a', b"b", b"c", b"d"]
            assert lazy.headers == (b'INDEX', b'ATTR')
            assert lazy.rows, lazy.cols == (4, 2)

    def test_get_actual_col_skip_headers(self):
        actual = b"INDEX,ATTR\n0,a\n1,b\n2,c\n3,d\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            assert list(lazy.sequence(col=0)) == [b"INDEX", b'0', b"1", b"2", b"3"]
            assert list(lazy.sequence(col=1)) == [b"ATTR", b'a', b"b", b"c", b"d"]
            assert lazy.headers == ()
            assert lazy.rows, lazy.cols == (4, 2)

    def test_headless_actual_col(self):
        actual = b"INDEX,ATTR\n0,a\n1,b\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))

        assert lazy.rows, lazy.cols == (3, 2)
        assert actual == [[b'INDEX', b'0', b"1"], [b'ATTR', b'a', b"b"]]
        assert lazy.headers == ()

    def test_get_row(self, lazy):
        row_0 = list(lazy.sequence(row=0))
        assert row_0 == [b"0", b"a0", b"b0"]
        row_1 = list(lazy.sequence(row=1))
        assert row_1 == [b"1", b"a1", b"b1"]

    def test_empty_csv(self):
        fpath = os.path.join(HERE, "fixtures/file_empty.csv")
        lazy = lazycsv.LazyCSV(fpath)
        actual = [list(lazy.sequence(col=i)) for i in range(lazy.cols)]
        assert actual == [[b"", b""], [b"", b""], [b"", b""]]

    def test_headless_empty_csv(self):
        actual = b",\n,\n,\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            col1 = list(lazy.sequence(col=1))
            col0 = list(lazy.sequence(col=0))

            actual = [col0, col1]
            assert actual == [[b'', b''], [b'', b'']]
            assert lazy.rows, lazy.cols == (2, 3)

    def test_empty_skipped_headers_csv(self):
        actual = b",\n,\n,\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        assert lazy.rows, lazy.cols == (3, 2)
        assert actual == [[b'', b'', b''], [b'', b'', b'']]
        assert lazy.headers == ()

class TestLazyCSVOptions:
    def test_get_skipped_header_column(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        actual = list(lazy.sequence(col=0))
        assert actual == [b"", b"0", b"1"]
        actual = list(lazy.sequence(col=1))
        assert actual == [b"ALPHA", b"a0", b"a1"]
        actual = list(lazy.sequence(col=2))
        assert actual == [b"BETA", b"b0", b"b1"]

    def test_get_skip_headers_row(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        row_0 = list(lazy.sequence(row=0))
        assert row_0 == [b"", b"ALPHA", b"BETA"]
        row_1 = list(lazy.sequence(row=1))
        assert row_1 == [b"0", b"a0", b"b0"]
        row_2 = list(lazy.sequence(row=2))
        assert row_2 == [b"1", b"a1", b"b1"]

    def test_skipped_headers_full_row(self):
        actual = b"this,that\n,\n,\n"
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(row=i)) for i in range(lazy.rows))
        assert lazy.headers == ()
        header, *rest = actual
        assert header == [b"this", b"that"]
        assert rest == [[b"", b""], [b"", b""]]

    def test_get_skip_headers_row_reversed(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        row_0 = list(lazy.sequence(row=0, reversed=True))
        assert row_0 == [b"BETA", b"ALPHA", b""]
        row_1 = list(lazy.sequence(row=1, reversed=True))
        assert row_1 == [b"b0", b"a0", b"0"]
        row_2 = list(lazy.sequence(row=2, reversed=True))
        assert row_2 == [b"b1", b"a1", b"1"]

    def test_get_reversed_column(self, lazy):
        actual = list(lazy.sequence(col=0, reversed=True))
        assert actual == [b"1", b"0"]
        actual = list(lazy.sequence(col=1, reversed=True))
        assert actual == [b"a1", b"a0"]
        actual = list(lazy.sequence(col=2, reversed=True))
        assert actual == [b"b1", b"b0"]

    def test_get_skipped_header_column_reversed(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        actual = [list(lazy.sequence(col=i, reversed=True)) for i in range(lazy.cols)]
        expected = [
            [b"1", b"0", b""],
            [b"a1", b"a0", b"ALPHA"],
            [b"b1", b"b0", b"BETA"],
        ]
        assert actual == expected

    def test_get_reversed_row(self, lazy):
        row_0 = list(lazy.sequence(row=0, reversed=True))
        assert row_0 == [b"b0", b"a0", b"0"]
        row_1 = list(lazy.sequence(row=1, reversed=True))
        assert row_1 == [b"b1", b"a1", b"1"]

    def test_newlines_in_quote(self):
        fpath = os.path.join(HERE, "fixtures/file_newline.csv")
        lazy = lazycsv.LazyCSV(fpath, unquote=False)
        assert lazy.headers == (b"", b'"This,that\n"', b'"Fizz,Buzz\r"')
        actual = [list(lazy.sequence(col=i)) for i in range(lazy.cols)]
        assert actual == [[b"0"], [b'"Goo,Bar\n"'], [b'"Bizz,Bazz"']]

    def test_buffer_size(self):
        lazy = lazycsv.LazyCSV(FPATH, buffer_size=1024)
        actual = list(lazy.sequence(col=0))
        assert actual == [b"0", b"1"]

    def test_negative_buffer_size(self):
        with pytest.raises(ValueError) as e:
            lazycsv.LazyCSV(FPATH, buffer_size=-1)
        assert e.type == ValueError

    def test_dirname(self):
        tempdir = tempfile.TemporaryDirectory()
        _ = lazycsv.LazyCSV(FPATH, index_dir=tempdir.name)
        assert len(os.listdir(tempdir.name)) == 2


class TestCRLF:
    def test_crlf(self):
        fpath = os.path.join(HERE, "fixtures/file_crlf.csv")
        lazy = lazycsv.LazyCSV(fpath)

        assert lazy.headers == (b"", b"A", b"B")
        actual = list(lazy.sequence(col=0))
        assert actual == [b"0"]
        actual = list(lazy.sequence(col=1))
        assert actual == [b"a0"]
        actual = list(lazy.sequence(col=2))
        assert actual == [b"b0"]

    def test_crlf2(self):
        fpath = os.path.join(HERE, "fixtures/file_crlf2.csv")
        lazy = lazycsv.LazyCSV(fpath, unquote=False)

        assert lazy.headers == (b"", b'"This,that"', b'"Fizz,Buzz"')
        actual = list(lazy.sequence(col=0))
        assert actual == [b"0"]
        actual = list(lazy.sequence(col=1))
        assert actual == [b'"Goo,Bar"']
        actual = list(lazy.sequence(col=2))
        assert actual == [b'"Bizz,Bazz"']


class TestBigFiles:
    def test_bigger_file(self, file_1000r_1000c):
        lazy = lazycsv.LazyCSV(file_1000r_1000c.name)
        actual = list(lazy.sequence(col=0))
        assert len(actual) == 1000

    def test_variable_buffer_size(self, file_1000r_1000c):
        lazy = lazycsv.LazyCSV(file_1000r_1000c.name, buffer_size=10**7)
        actual = list(lazy.sequence(col=0))
        assert len(actual) == 1000

class TestUnorderedFiles:
    def test_missing_col(self):
        data = "x,y,z\r\n1,2\r\n3,1,3\r\n".encode()
        with prepped_file(data) as tempf, pytest.warns(RuntimeWarning):
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        expected = [[b"x", b"1", b"3"], [b"y", b"2", b"1"], [b"z", b"", b"3"]]
        assert actual == expected

    def test_many_missing_cols(self):
        data = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z\n\n0,0,0,0,,,,,,,,,,,,,,,,,,,,,,1\n".encode()
        with prepped_file(data) as tempf, pytest.raises(RuntimeError) as err:
            _ = lazycsv.LazyCSV(tempf.name)
        assert err.type == RuntimeError

    def test_extra_col(self):
        data = "x,y\r\n1,2,3\r\n4,5\r\n".encode()
        with prepped_file(data) as tempf, pytest.warns(RuntimeWarning):
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        expected = [[b"x", b"1", b"4"], [b"y", b"2", b"5"]]
        assert actual == expected

    def test_many_extra_col(self):
        data = "x\r\n,,,,,,,,,,,,,,,,,,,,,,,,,,,\r\n4\r\n".encode()
        with prepped_file(data) as tempf, pytest.warns(RuntimeWarning):
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        expected = [[b"x", b"", b"4"]]
        assert actual == expected

class TestEdgecases:
    def test_many_files_separators(self):
        for sep in ("\n", "\r", "\r\n"):
            for i in range(250, 265):
                header = "A"*i
                data = f"{header}{sep}1{sep}2"
                with prepped_file(data.encode()) as tempf:
                    lazy = lazycsv.LazyCSV(tempf.name)
                    actual = list(lazy.sequence(col=0))
                    headers = lazy.headers
                assert actual == [b"1", b"2"]
                assert headers == (header.encode(), )

    def test_many_empty_files_separators(self):
        for sep in ("\n", "\r", "\r\n"):
            for i in range(250, 261):
                header = "A"*i
                data = f"{header}{sep}{sep}"
                with prepped_file(data.encode()) as tempf:
                    lazy = lazycsv.LazyCSV(tempf.name)
                    actual = list(lazy.sequence(col=0))
                    headers = lazy.headers
                assert actual == [b""]
                assert headers == (header.encode(), )

    def test_many_empty_files_separators_many_cols(self):
        for sep in ("\n", "\r", "\r\n"):
            for item in ("", "0"):
                for n in range(250, 261):
                    header = ",".join(item for _ in range(n))
                    data = ""
                    for _ in range(n):
                        data += header+sep
                    with prepped_file(data.encode()) as tempf:
                        lazy = lazycsv.LazyCSV(tempf.name)
                        actual = list(lazy.sequence(col=0))
                        headers = lazy.headers
                        assert len(actual) == len(headers) - 1 == n - 1
                        assert all(i == item.encode() for i in actual)

    def test_sparse_column(self):
        data = "HEADER\n\n1\n\n2\n\n\n3\n"
        with prepped_file(data.encode()) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            actual = list(lazy.sequence(col=0))
            headers = lazy.headers
            assert headers == (b"HEADER", )
            assert actual == [b"", b"1", b"", b"2", b"", b"", b"3"]

    def test_sparse_crlf_column(self):
        data = "HEADER\r\n\r\n1\r\n\r\n2\r\n\r\n\r\n3\r\n"
        with prepped_file(data.encode()) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            actual = list(lazy.sequence(col=0))
            headers = lazy.headers
            assert headers == (b"HEADER", )
            assert actual == [b"", b"1", b"", b"2", b"", b"", b"3"]

    def test_getitem_with_crlf_newline_at_eof(self):
        data = 'x,y,z,str,date,quarter,ca_subvar_1,ca_subvar_2,ca_subvar_3,bool1,bool2,bool3\r\n1,2000-01-01T00:00:00,"range(-999.0,0.0)",red,2014-11-01T00:00:00,2014-10-01T00:00:00,1,1,2,1,1,0\r\n2,2000-01-02T00:00:00,No Data,green,2014-11-01T00:00:00,2014-10-01T00:00:00,1,2,3,1,0,0\r\n3,1950-12-24T00:00:00,1.234,reg-green-blue-whatever,2014-12-15T00:00:00,2014-10-01T00:00:00,2,3,4,0,1,0\r\n'.encode()
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        expected = [
            [b"1", b"2", b"3"],
            [b"2000-01-01T00:00:00", b"2000-01-02T00:00:00", b"1950-12-24T00:00:00"],
            [b"range(-999.0,0.0)", b"No Data", b"1.234"],
            [b"red", b"green", b"reg-green-blue-whatever"],
            [b"2014-11-01T00:00:00", b"2014-11-01T00:00:00", b"2014-12-15T00:00:00"],
            [b"2014-10-01T00:00:00", b"2014-10-01T00:00:00", b"2014-10-01T00:00:00"],
            [b"1", b"1", b"2"],
            [b"1", b"2", b"3"],
            [b"2", b"3", b"4"],
            [b"1", b"1", b"0"],
            [b"1", b"0", b"1"],
            [b"0", b"0", b"0"],
        ]
        assert expected == actual

    def test_problematic_numeric(self):
        data = """\
        ColName
        -9
        0
        -179769313486000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
        1.234
        999
        3.14159
        -179769313486000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
        """
        data = textwrap.dedent(data)
        data = data.encode()
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        assert actual == [data.split()]

    def test_encoded_headers(self):
        data = '"Göteborg","Sverige",Umeå,Köln,東京,deltaΔdelta\nc1,c2,c3,c4,c5,c6\n'
        data = data.encode()
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            assert lazy.headers == (
                "Göteborg".encode(),
                "Sverige".encode(),
                "Umeå".encode(),
                "Köln".encode(),
                "東京".encode(),
                "delta\u0394delta".encode(),
            )
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        expected = [[b"c1"], [b"c2"], [b"c3"], [b"c4"], [b"c5"], [b"c6"]]
        assert actual == expected

    def test_crlf_no_newline(self):
        actual = "header1,header2,header3\r\n1,2,3\r\n4,5,6\r\n7,8,9".encode()
        with prepped_file(actual) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        expected = [[b"1", b"4", b"7"], [b"2", b"5", b"8"], [b"3", b"6", b"9"]]
        assert actual == expected

    def test_empty_headers(self):
        data = (
            '" ","","repeated"\n'
            '2557," Bagua "," Amazonas"\n'
            '2563," Bongara "," Amazonas"\n'
            '2535," Chachapoyas "," Amazonas"\n'
            '2576," Condorcanqui "," Amazonas"\n'
        )
        data = textwrap.dedent(data)
        data = data.encode()
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            assert lazy.headers == (b" ", b"", b"repeated")
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))
        expected = [
            [b"2557", b"2563", b"2535", b"2576"],
            [b" Bagua ", b" Bongara ", b" Chachapoyas ", b" Condorcanqui "],
            [b" Amazonas", b" Amazonas", b" Amazonas", b" Amazonas"],
        ]
        assert actual == expected

