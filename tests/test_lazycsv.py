import contextlib
import os.path
import tempfile
import textwrap

from lazycsv import lazycsv

import pytest

HERE = os.path.abspath(os.path.dirname(__file__))
FPATH = os.path.join(HERE, "fixtures/file.csv")


@pytest.fixture
def lazy():
    lazy = lazycsv.LazyCSV(FPATH)
    yield lazy


@contextlib.contextmanager
def prepped_file(data):
    tempf = tempfile.NamedTemporaryFile()
    tempf.write(data)
    tempf.flush()
    yield tempf
    tempf.close()


class TestLazyCSV:
    def test_headers(self, lazy):
        assert lazy.name == os.path.abspath(FPATH)
        assert lazy.headers == (b"", b"ALPHA", b"BETA")

    def test_more_headers(self):
        data = b"INDEX,,AA,B,CC,D,EE\n0,1,2,3,4,5,6\n"
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)

        assert lazy.headers == (b"INDEX", b"", b"AA", b"B", b"CC", b"D", b"EE")

    def test_headers_empty_index(self):
        data = b",AA,B,CC,D,EE\n0,1,2,3,4,\n"
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)

        assert lazy.headers == (b"", b"AA", b"B", b"CC", b"D", b"EE")

    def test_initial_parse(self, lazy):
        assert lazy.rows == 2
        assert lazy.cols == 3

    def test_initial_parse_skip_headers(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        assert lazy.rows == 3
        assert lazy.cols == 3
        assert lazy.headers == ()

    def test_get_column(self, lazy):
        data = list(lazy.sequence(col=0))
        assert data == [b"0", b"1"]
        data = list(lazy.sequence(col=1))
        assert data == [b"a0", b"a1"]
        data = list(lazy.sequence(col=2))
        assert data == [b"b0", b"b1"]

    def test_get_data_col(self):
        data = b"INDEX,ATTR\n0,a\n1,b\n2,c\n3,d\n"
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))

        assert lazy.rows == 4
        assert lazy.cols == 2
        assert actual == [[b'0', b"1", b"2", b"3"], [b'a', b"b", b"c", b"d"]]
        assert lazy.headers == (b'INDEX', b'ATTR')

    def test_headless_data_col(self):
        data = b"INDEX,ATTR\n0,a\n1,b\n"
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))

        assert lazy.rows == 3
        assert lazy.cols == 2
        assert actual == [[b'INDEX', b'0', b"1"], [b'ATTR', b'a', b"b"]]
        assert lazy.headers == ()

    def test_get_row(self):
        lazy = lazycsv.LazyCSV(FPATH)
        row_0 = list(lazy.sequence(row=0))
        assert row_0 == [b"0", b"a0", b"b0"]
        row_1 = list(lazy.sequence(row=1))
        assert row_1 == [b"1", b"a1", b"b1"]

    def test_empty_csv(self):
        fpath = os.path.join(HERE, "fixtures/file_empty.csv")
        lazy = lazycsv.LazyCSV(fpath)
        data = [list(lazy.sequence(col=i)) for i in range(lazy.cols)]
        assert data == [[b"", b""], [b"", b""], [b"", b""]]

    def test_headless_empty_csv(self):
        data = b",\n,\n,\n"
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))

        assert actual == [[b'', b''], [b'', b'']]
        assert lazy.rows == 2
        assert lazy.cols == 2

    def test_empty_skipped_headers_csv(self):
        data = b",\n,\n,\n"
        with prepped_file(data) as tempf:
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))

        assert lazy.rows == 3
        assert lazy.cols == 2
        assert actual == [[b'', b'', b''], [b'', b'', b'']]
        assert lazy.headers == ()

class TestLazyCSVOptions:
    def test_get_skipped_header_column(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        data = list(lazy.sequence(col=0))
        assert data == [b"", b"0", b"1"]
        data = list(lazy.sequence(col=1))
        assert data == [b"ALPHA", b"a0", b"a1"]
        data = list(lazy.sequence(col=2))
        assert data == [b"BETA", b"b0", b"b1"]

    def test_get_skip_headers_row(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        row_0 = list(lazy.sequence(row=0))
        assert row_0 == [b"", b"ALPHA", b"BETA"]
        row_1 = list(lazy.sequence(row=1))
        assert row_1 == [b"0", b"a0", b"b0"]
        row_2 = list(lazy.sequence(row=2))
        assert row_2 == [b"1", b"a1", b"b1"]

    def test_get_skip_headers_row_reversed(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        row_0 = list(lazy.sequence(row=0, reversed=True))
        assert row_0 == [b"BETA", b"ALPHA", b""]
        row_1 = list(lazy.sequence(row=1, reversed=True))
        assert row_1 == [b"b0", b"a0", b"0"]
        row_2 = list(lazy.sequence(row=2, reversed=True))
        assert row_2 == [b"b1", b"a1", b"1"]

    def test_get_reversed_column(self):
        lazy = lazycsv.LazyCSV(FPATH)
        data = list(lazy.sequence(col=0, reversed=True))
        assert data == [b"1", b"0"]
        data = list(lazy.sequence(col=1, reversed=True))
        assert data == [b"a1", b"a0"]
        data = list(lazy.sequence(col=2, reversed=True))
        assert data == [b"b1", b"b0"]

    def test_get_skipped_header_column_reversed(self):
        lazy = lazycsv.LazyCSV(FPATH, skip_headers=True)
        actual = [list(lazy.sequence(col=i, reversed=True)) for i in range(lazy.cols)]
        expected = [
            [b"1", b"0", b""],
            [b"a1", b"a0", b"ALPHA"],
            [b"b1", b"b0", b"BETA"],
        ]

        assert actual == expected

    def test_get_reversed_row(self):
        lazy = lazycsv.LazyCSV(FPATH)
        row_0 = list(lazy.sequence(row=0, reversed=True))
        assert row_0 == [b"b0", b"a0", b"0"]
        row_1 = list(lazy.sequence(row=1, reversed=True))
        assert row_1 == [b"b1", b"a1", b"1"]

    def test_newlines_in_quote(self):
        fpath = os.path.join(HERE, "fixtures/file_newline.csv")
        lazy = lazycsv.LazyCSV(fpath, unquote=False)
        assert lazy.headers == (b"", b'"This,that\n"', b'"Fizz,Buzz\r"')
        data = [list(lazy.sequence(col=i)) for i in range(lazy.cols)]
        assert data == [[b"0"], [b'"Goo,Bar\n"'], [b'"Bizz,Bazz"']]


class TestCRLF:
    def test_crlf(self):
        fpath = os.path.join(HERE, "fixtures/file_crlf.csv")
        lazy = lazycsv.LazyCSV(fpath)

        assert lazy.headers == (b"", b"A", b"B")
        data = list(lazy.sequence(col=0))
        assert data == [b"0"]
        data = list(lazy.sequence(col=1))
        assert data == [b"a0"]
        data = list(lazy.sequence(col=2))
        assert data == [b"b0"]

    def test_crlf2(self):
        fpath = os.path.join(HERE, "fixtures/file_crlf2.csv")
        lazy = lazycsv.LazyCSV(fpath, unquote=False)

        assert lazy.headers == (b"", b'"This,that"', b'"Fizz,Buzz"')
        data = list(lazy.sequence(col=0))
        assert data == [b"0"]
        data = list(lazy.sequence(col=1))
        assert data == [b'"Goo,Bar"']
        data = list(lazy.sequence(col=2))
        assert data == [b'"Bizz,Bazz"']


class TestBigFiles:
    def test_bigger_file(self):
        tempf = tempfile.NamedTemporaryFile()

        cols = 1000
        rows = 1000

        headers = ",".join(f"col_{i}" for i in range(cols)) + "\n"
        tempf.write(headers.encode("utf8"))

        for _ in range(rows):
            row = ",".join(f"{j}" for j in range(cols)) + "\n"
            tempf.write(row.encode("utf8"))

        tempf.flush()
        lazy = lazycsv.LazyCSV(tempf.name)
        data = list(lazy.sequence(col=0))
        assert len(data) == 1000


class TestUnorderedFiles:
    def test_unordered_file_missing_col(self):
        data = "x,y,z\r\n1,2\r\n3,1,3\r\n".encode()
        with prepped_file(data) as tempf, pytest.warns(RuntimeWarning):
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))

        expected = [[b"x", b"1", b"3"], [b"y", b"2", b"1"], [b"z", b"", b"3"]]

        assert actual == expected

    def test_unordered_file_extra_col(self):
        data = "x,y\r\n1,2,3\r\n4,5\r\n".encode()
        with prepped_file(data) as tempf, pytest.warns(RuntimeWarning):
            lazy = lazycsv.LazyCSV(tempf.name, skip_headers=True)
            actual = list(list(lazy.sequence(col=i)) for i in range(lazy.cols))

        expected = [[b"x", b"1", b"4"], [b"y", b"2", b"5"]]

        assert actual == expected


class TestEdgecases:
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
        data = "header1,header2,header3\r\n1,2,3\r\n4,5,6\r\n7,8,9".encode()

        with prepped_file(data) as tempf:
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
