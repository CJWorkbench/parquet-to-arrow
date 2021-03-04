import datetime
import subprocess
from pathlib import Path
from typing import Tuple

import pyarrow

from .util import empty_file, parquet_file


def do_diff(path1: Path, path2: Path) -> Tuple[int, str]:
    completed = subprocess.run(
        ["/usr/bin/parquet-diff", str(path1), str(path2)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )

    if completed.stderr:
        raise RuntimeError(
            "parquet-to-text-stream exited with %d: %s"
            % (completed.returncode, completed.stderr)
        )

    return completed.returncode, completed.stdout


def assert_same(path1: Path, path2: Path):
    assert do_diff(path1, path2) == (0, "")


def arrow_table_diff(table1: pyarrow.Table, table2: pyarrow.Table) -> str:
    """
    return do_diff(), after calling parquet_file() on args.
    """
    with parquet_file(table1) as parquet1:
        with parquet_file(table2) as parquet2:
            return do_diff(parquet1, parquet2)


def assert_arrow_table_identity(table: pyarrow.Table):
    """
    Assert the same table stored twice gives diff=0.
    """
    with parquet_file(table) as parquet1:
        with parquet_file(table) as parquet2:
            assert_same(parquet1, parquet2)


def test_different_num_row_groups_is_different():
    # 0 row-group file: create and then delete a writer
    with empty_file() as rg0:
        writer = pyarrow.parquet.ParquetWriter(
            str(rg0), schema=pyarrow.schema([("A", pyarrow.int32())])
        )
        writer.close()

        # Assume pyarrow.parquet.write_table() writes 1 row groups when nrows==0
        with parquet_file(
            pyarrow.table({"A": pyarrow.array([], pyarrow.int32())})
        ) as rg1:
            assert do_diff(rg0, rg1) == (1, "Number of row groups:\n-0\n+1\n")


def test_different_num_columns_is_different():
    with parquet_file(pyarrow.table({"A": [1]})) as col1:
        with parquet_file(pyarrow.table({"A": [1], "B": [2]})) as col2:
            assert do_diff(col1, col2) == (1, "Number of columns:\n-1\n+2\n")


def test_different_column_name_is_different():
    with parquet_file(pyarrow.table({"A": [1], "B": [2]})) as col1:
        with parquet_file(pyarrow.table({"A": [1], "B'": [2]})) as col2:
            assert do_diff(col1, col2) == (1, "Column 1 name:\n-B\n+B'\n")


def test_different_column_physical_type_is_different():
    with parquet_file(
        pyarrow.table({"A": pyarrow.array([1], pyarrow.int32())})
    ) as col1:
        with parquet_file(
            pyarrow.table({"A": pyarrow.array([1], pyarrow.int64())})
        ) as col2:
            assert do_diff(col1, col2) == (
                1,
                "Column 0 (A) physical type:\n-INT32\n+INT64\n",
            )


def test_different_column_logical_type_is_different():
    with parquet_file(pyarrow.table({"A": pyarrow.array([1], pyarrow.int8())})) as col1:
        with parquet_file(
            pyarrow.table({"A": pyarrow.array([1], pyarrow.int16())})
        ) as col2:
            assert do_diff(col1, col2) == (
                1,
                "Column 0 (A) logical type:\n-Int(bitWidth=8, isSigned=true)\n+Int(bitWidth=16, isSigned=true)\n",
            )


def test_different_row_group_lengths_differ():
    # Two tables of 2 chunks each, but with different sizes
    with parquet_file(pyarrow.table({"A": [1, 2, 3, 4]}), chunk_size=2) as table1:
        with parquet_file(pyarrow.table({"A": [1, 2, 3, 4]}), chunk_size=3) as table2:
            assert do_diff(table1, table2) == (
                1,
                "RowGroup 0 number of rows:\n-2\n+3\n",
            )


def test_same_row_group_lengths_ok():
    with parquet_file(pyarrow.table({"A": [1, 2, 3, 4]}), chunk_size=3) as path:
        assert_same(path, path)


def test_column_type_int32_different():
    table1 = pyarrow.table(
        {"A": pyarrow.array([1, 2, -1, None, 3, None, 1], pyarrow.int32())}
    )
    table2 = pyarrow.table(
        {"A": pyarrow.array([1, 2, -1, None, 3, None, -2], pyarrow.int32())}
    )
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 6:\n-1\n+-2\n",
    )


def test_column_type_int32_same():
    table = pyarrow.table(
        {"A": pyarrow.array([1, 2, -1, None, 3, None, 1], pyarrow.int32())}
    )
    assert_arrow_table_identity(table)


def test_column_type_int64_different():
    table1 = pyarrow.table(
        {"A": pyarrow.array([1, 2, -1, None, 3, None, 1], pyarrow.int64())}
    )
    table2 = pyarrow.table(
        {"A": pyarrow.array([1, 2, -1, None, 3, None, -2], pyarrow.int64())}
    )
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 6:\n-1\n+-2\n",
    )


def test_column_type_int64_same():
    table = pyarrow.table(
        {"A": pyarrow.array([1, 2, -1, None, 3, None, 1], pyarrow.int64())}
    )
    assert_arrow_table_identity(table)


def test_column_type_float32_different():
    table1 = pyarrow.table(
        {"A": pyarrow.array([1.1, -2.1, None, 3.4], pyarrow.float32())}
    )
    table2 = pyarrow.table(
        {"A": pyarrow.array([1.1, -2.1, None, 3.400001], pyarrow.float32())}
    )
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 3:\n-3.4\n+3.400001\n",
    )


def test_column_type_float32_same():
    table = pyarrow.table(
        {"A": pyarrow.array([1.1, -2.1, None, 3.40001], pyarrow.float32())}
    )
    assert_arrow_table_identity(table)


def test_column_type_float64_different():
    table1 = pyarrow.table(
        {"A": pyarrow.array([1.1, -2.1, None, 3.4], pyarrow.float64())}
    )
    table2 = pyarrow.table(
        {"A": pyarrow.array([1.1, -2.1, None, 3.4000001], pyarrow.float64())}
    )
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 3:\n-3.4\n+3.4000001\n",
    )


def test_column_type_float64_same():
    table = pyarrow.table(
        {"A": pyarrow.array([1.1, -2.1, None, 3.4], pyarrow.float64())}
    )
    assert_arrow_table_identity(table)


def test_column_str_different():
    table1 = pyarrow.table({"A": pyarrow.array(["a", None, "bc", "d"])})
    table2 = pyarrow.table({"A": pyarrow.array(["a", None, "b", "cd"])})
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 2:\n-bc\n+b\n",
    )


def test_column_str_same():
    table = pyarrow.table({"A": pyarrow.array(["a", None, "bc", "d"])})
    assert_arrow_table_identity(table)


def test_column_dictionary_equal_to_str():
    table = pyarrow.table({"A": pyarrow.array(["a", None, "b", "a", None, "a"])})
    with parquet_file(table, use_dictionary=[]) as plain:
        with parquet_file(table, use_dictionary=[b"A"]) as encoded:
            assert do_diff(plain, encoded) == (0, "")


def test_column_dictionary_different_values():
    # Two files with same indices, different values
    table1 = pyarrow.table({"A": pyarrow.array(["a", None, "b", "a"])})
    table2 = pyarrow.table({"A": pyarrow.array(["a", None, "c", "a"])})
    with parquet_file(table1, use_dictionary=[b"A"]) as path1:
        with parquet_file(table2, use_dictionary=[b"A"]) as path2:
            assert do_diff(path1, path2) == (
                1,
                "RowGroup 0, Column 0, Row 2:\n-b\n+c\n",
            )


def test_column_dictionary_different_indices():
    # Two files with same indices, different values
    table1 = pyarrow.table({"A": pyarrow.array(["a", None, "b", "a"])})
    table2 = pyarrow.table({"A": pyarrow.array(["a", None, "b", "b"])})
    with parquet_file(table1, use_dictionary=[b"A"]) as path1:
        with parquet_file(table2, use_dictionary=[b"A"]) as path2:
            assert do_diff(path1, path2) == (
                1,
                "RowGroup 0, Column 0, Row 3:\n-a\n+b\n",
            )


def test_column_str_byte_compare_only():
    # Example from http://unicode.org/reports/tr15/#Norm_Forms
    # Both strings are the same; parquet-diff should treat them as different
    # (because it's easier/faster/simpler....)
    table1 = pyarrow.table({"A": ["\u2126"]})
    table2 = pyarrow.table({"A": ["\u03A9"]})
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 0:\n-\u2126\n+\u03A9\n",
    )


def test_date_same():
    table = pyarrow.table(
        {"A": [datetime.date(2019, 10, 21), datetime.date(2021, 3, 4)]}
    )
    assert_arrow_table_identity(table)


def test_date_different():
    table1 = pyarrow.table(
        {"A": [datetime.date(2019, 10, 21), datetime.date(2021, 3, 4)]}
    )
    table2 = pyarrow.table(
        {"A": [datetime.date(2019, 10, 22), datetime.date(2021, 3, 4)]}
    )
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 0:\n-18190\n+18191\n",
    )


def test_timestamp_same():
    table = pyarrow.table(
        {
            "A": [
                datetime.datetime(2019, 10, 21, 1, 2, 3),
                datetime.datetime(2019, 10, 21, 2, 3, 4),
            ]
        }
    )
    assert_arrow_table_identity(table)


def test_timestamp_different():
    table1 = pyarrow.table(
        {
            "A": [
                datetime.datetime(2019, 10, 21, 1, 2, 3),
                datetime.datetime(2019, 10, 21, 2, 3, 4),
            ]
        }
    )
    table2 = pyarrow.table(
        {
            "A": [
                datetime.datetime(2019, 10, 21, 1, 2, 3),
                datetime.datetime(2019, 10, 21, 2, 4, 4),
            ]
        }
    )
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 0, Row 1:\n-1571623384000000\n+1571623444000000\n",
    )


def test_timestamp_different_only_because_unit_different():
    table1 = pyarrow.table(
        {
            "A": pyarrow.array(
                [
                    datetime.datetime(2019, 10, 21, 1, 2, 3),
                    datetime.datetime(2019, 10, 21, 2, 3, 4),
                ],
                pyarrow.timestamp(unit="us"),
            )
        }
    )
    table2 = pyarrow.table(
        {
            "A": pyarrow.array(
                [
                    datetime.datetime(2019, 10, 21, 1, 2, 3),
                    datetime.datetime(2019, 10, 21, 2, 3, 4),
                ],
                pyarrow.timestamp(unit="ms"),
            )
        }
    )
    assert arrow_table_diff(table1, table2) == (
        1,
        (
            "Column 0 (A) logical type:\n"
            "-Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false)\n"
            "+Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false)\n"
        ),
    )


def test_parquet_1_converted_type_and_parquet_2_logical_type_same():
    table = pyarrow.table(
        {
            "A": pyarrow.array(
                [
                    datetime.datetime(2019, 10, 21, 1, 2, 3),
                    datetime.datetime(2019, 10, 21, 2, 3, 4),
                ],
                pyarrow.timestamp(unit="us"),
            )
        }
    )
    with parquet_file(table, version="1.0") as v1:
        with parquet_file(table, version="2.0") as v2:
            assert_same(v1, v2)


def test_diff_in_non_first_column():
    table1 = pyarrow.table({"A": [1, 2, 3], "B": [2, 3, 4]})
    table2 = pyarrow.table({"A": [1, 2, 3], "B": [2, 1, 4]})
    assert arrow_table_diff(table1, table2) == (
        1,
        "RowGroup 0, Column 1, Row 1:\n-3\n+1\n",
    )
