from pathlib import Path
import subprocess
import tempfile
import textwrap
from typing import Tuple
import pyarrow
from .util import assert_table_equals


def do_convert(
    csv_path: Path,
    *,
    delimiter: str = ",",
    max_rows: int = 9999,
    max_columns: int = 9999,
    max_bytes_per_value: int = 9999,
    include_stdout: bool = False
) -> Tuple[pyarrow.Table, bytes]:
    with tempfile.NamedTemporaryFile(suffix=".arrow") as arrow_file:
        args = [
            "/usr/bin/csv-to-arrow",
            "--delimiter",
            delimiter,
            "--max-rows",
            str(max_rows),
            "--max-columns",
            str(max_columns),
            "--max-bytes-per-value",
            str(max_bytes_per_value),
            str(csv_path),
            arrow_file.name,
        ]
        print(repr(args))
        try:
            result = subprocess.run(
                args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
            )
        except subprocess.CalledProcessError as err:
            # Rewrite error so it's easy to read in test-result stack trace
            raise RuntimeError(
                "Process failed with code %d: %s"
                % (
                    err.returncode,
                    (
                        err.stdout.decode("utf-8", errors="replace")
                        + err.stderr.decode("utf-8", errors="replace")
                    ),
                )
            ) from None

        assert result.stderr == b""
        result_reader = pyarrow.ipc.open_file(arrow_file.name)
        table = result_reader.read_all()
        if include_stdout:
            return table, result.stdout
        else:
            return table


def do_convert_dedented_utf8_csv(csv_text: str, **kwargs):
    with tempfile.NamedTemporaryFile(suffix=".csv") as csv_file:
        csv_path = Path(csv_file.name)
        csv_path.write_text(textwrap.dedent(csv_text))
        return do_convert(csv_path, **kwargs)


def test_simple_csv():
    result = do_convert_dedented_utf8_csv(
        """\
        1,foo,bar
        2,bar,baz
        """
    )
    expected = pyarrow.table(
        {"0": ["1", "2"], "1": ["foo", "bar"], "2": ["bar", "baz"]}
    )
    assert_table_equals(result, expected)


def test_no_newline_at_end_of_file():
    result = do_convert_dedented_utf8_csv(
        """\
        1,foo,bar
        2,bar,baz"""
    )
    expected = pyarrow.table(
        {"0": ["1", "2"], "1": ["foo", "bar"], "2": ["bar", "baz"]}
    )
    assert_table_equals(result, expected)


def test_backfill_null():
    result = do_convert_dedented_utf8_csv(
        """\
        1
        2
        3,x
        4,x,y,z
        """
    )
    expected = pyarrow.table(
        {
            "0": ["1", "2", "3", "4"],
            "1": [None, None, "x", "x"],
            "2": [None, None, None, "y"],
            "3": [None, None, None, "z"],
        }
    )
    assert_table_equals(result, expected)


def test_forward_fill_null():
    result = do_convert_dedented_utf8_csv(
        """\
        1,x,y,z
        2,x
        """
    )
    expected = pyarrow.table(
        {"0": ["1", "2"], "1": ["x", "x"], "2": ["y", None], "3": ["z", None]}
    )
    assert_table_equals(result, expected)


def test_middle_fill_null():
    result = do_convert_dedented_utf8_csv(
        """\
        1,x,y,z
        2
        3,x
        4,a,b,c
        """
    )
    expected = pyarrow.table(
        {
            "0": ["1", "2", "3", "4"],
            "1": ["x", None, "x", "a"],
            "2": ["y", None, None, "b"],
            "3": ["z", None, None, "c"],
        }
    )
    assert_table_equals(result, expected)


def test_quotes():
    result = do_convert_dedented_utf8_csv(
        """\
        "A","B"
        "foo
        bar","baz"
        """
    )
    expected = pyarrow.table({"0": ["A", "foo\nbar"], "1": ["B", "baz"]})
    assert_table_equals(result, expected)


def test_double_quotes():
    result = do_convert_dedented_utf8_csv(
        """\
        "x""y","z"
        """
    )
    expected = pyarrow.table({"0": ['x"y'], "1": ["z"]})
    assert_table_equals(result, expected)


def test_semicolon_delimiter():
    result = do_convert_dedented_utf8_csv(
        """\
        A,B;C
        """,
        delimiter=";",
    )
    expected = pyarrow.table({"0": ["A,B"], "1": ["C"]})
    assert_table_equals(result, expected)


def test_allow_quotes_mid_value():
    # supports some files that have no quote chars. (TSVs tend to have no
    # quote chars.)
    result, stdout = do_convert_dedented_utf8_csv(
        """\
        a,b"not quoted"
        c""do not unescape,d
        """,
        include_stdout=True,
    )
    expected = pyarrow.table(
        {"0": ["a", 'c""do not unescape'], "1": ['b"not quoted"', "d"]}
    )
    assert_table_equals(result, expected)
    assert stdout == b"", "should not warn"


def test_empty_values():
    # If a cell exists but has no value, it's empty string.
    # If a row is empty, it does not exist
    result = do_convert_dedented_utf8_csv(
        """\
        "",,
        ,,,,
        ,

        """
    )
    expected = pyarrow.table(
        {
            "0": ["", "", ""],
            "1": ["", "", ""],
            "2": ["", "", None],
            "3": [None, "", None],
            "4": [None, "", None],
        }
    )
    assert_table_equals(result, expected)


def test_repair_text_after_quotes():
    # quote-and-then-text usually means there's a parsing problem. Users should
    # be notified.
    result, stdout = do_convert_dedented_utf8_csv(
        """\
        a,"quoted"cru"ft
        ""x,d
        """,
        include_stdout=True,
    )
    expected = pyarrow.table({"0": ["a", "x"], "1": ['quotedcru"ft', "d"]})
    assert_table_equals(result, expected)
    assert (
        stdout == b"repaired 2 values (misplaced quotation marks; see row 0 column 1)\n"
    )


def test_repair_file_missing_end_quote():
    result, stdout = do_convert_dedented_utf8_csv(
        """\
        a,"b
        """,
        include_stdout=True,
    )
    expected = pyarrow.table({"0": ["a"], "1": ["b\n"]})
    assert_table_equals(result, expected)
    assert stdout == b"repaired last value (missing quotation mark)\n"


def test_skip_columns():
    result, stdout = do_convert_dedented_utf8_csv(
        """\
        A,B,C,D,E
        1,2,3,4,5
        """,
        max_columns=2,
        include_stdout=True,
    )
    expected = pyarrow.table({"0": ["A", "1"], "1": ["B", "2"]})
    assert_table_equals(result, expected)
    assert stdout == b"skipped 3 columns (after column limit of 2)\n"


def test_skip_rows():
    result, stdout = do_convert_dedented_utf8_csv(
        """\
        A,B
        1,2
        3,4
        5,6
        7,8
        9,0
        """,
        max_rows=4,
        include_stdout=True,
    )
    expected = pyarrow.table({"0": ["A", "1", "3", "5"], "1": ["B", "2", "4", "6"]})
    assert_table_equals(result, expected)
    assert stdout == b"skipped 2 rows (after row limit of 4)\n"


def test_truncate_values():
    result, stdout = do_convert_dedented_utf8_csv(
        """\
        333,4444
        55555,6666666
        7777777,8
        9,10
        """,
        max_bytes_per_value=4,
        include_stdout=True,
    )
    expected = pyarrow.table(
        {"0": ["333", "5555", "7777", "9"], "1": ["4444", "6666", "8", "10"]}
    )
    assert_table_equals(result, expected)
    assert stdout == b"truncated 3 values (value byte limit is 4; see row 1 column 0)\n"
