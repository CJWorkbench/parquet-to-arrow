from datetime import datetime
from pathlib import Path
import subprocess
import sys
import tempfile
import numpy as np
import pyarrow
from .util import parquet_file, assert_table_equals


def do_convert(parquet_path: Path, column_range: str, row_range: str) -> pyarrow.Table:
    with tempfile.NamedTemporaryFile() as arrow_file:
        try:
            subprocess.run(
                [
                    "/usr/bin/parquet-to-arrow-slice",
                    str(parquet_path),
                    column_range,
                    row_range,
                    arrow_file.name,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
        except subprocess.CalledProcessError as err:
            # Rewrite error so it's easy to read in test-result stack trace
            raise RuntimeError(
                "Process failed with code %d: %s"
                % (err.returncode, err.output.decode("utf-8", errors="replace"))
            ) from None

        result_reader = pyarrow.ipc.open_file(arrow_file.name)
        return result_reader.read_all()


def _test_read_write_table(
    table: pyarrow.Table, column_range: str = "0-50", row_range: str = "0-200"
) -> None:
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path, column_range, row_range)
        assert_table_equals(result, table)


def test_read_write_int64():
    # You may ignore any RuntimeWarning about numpy.ufunc size change:
    # https://github.com/numpy/numpy/issues/11788
    _test_read_write_table(pyarrow.table({"A": [1, 2 ** 62, 3]}))


def test_read_write_float64():
    _test_read_write_table(pyarrow.table({"A": [1.0, 2.2, 3.0, np.nan]}))


def test_read_write_float64_all_null():
    _test_read_write_table(
        pyarrow.table({"A": pyarrow.array([None], type=pyarrow.float64())})
    )


def test_read_write_text():
    _test_read_write_table(pyarrow.table({"A": ["x", None, "y"]}))


def test_read_write_text_all_null():
    _test_read_write_table(
        pyarrow.table({"A": pyarrow.array([None], type=pyarrow.string())})
    )


def test_read_write_text_categorical():
    table = pyarrow.table(
        {"A": pyarrow.array(["x", None, "y", "x"]).dictionary_encode()}
    )
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path, "0-10", "0-200")
        expected = pyarrow.table({"A": ["x", None, "y", "x"]})
        assert_table_equals(result, expected)


def test_read_write_datetime():
    _test_read_write_table(pyarrow.table({"A": [datetime.now(), None, datetime.now()]}))


def test_na_only_categorical_has_object_dtype():
    # Start with a Categorical with no values. (In Workbench, all
    # Categoricals are text.)
    table = pyarrow.table(
        {"A": pyarrow.array([None], type=pyarrow.string()).dictionary_encode()}
    )
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path, "0-10", "0-200")
        expected = pyarrow.table({"A": pyarrow.array([None], type=pyarrow.string())})
        assert_table_equals(result, expected)


def test_empty_categorical_has_object_dtype():
    table = pyarrow.table(
        {
            "A": pyarrow.DictionaryArray.from_arrays(
                pyarrow.array([], type=pyarrow.int32()),
                pyarrow.array([], type=pyarrow.string()),
            )
        }
    )
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path, "0-10", "0-200")
        expected = pyarrow.table({"A": pyarrow.array([], type=pyarrow.string())})
        assert_table_equals(result, expected)


def test_read_zero_row_groups():
    # When no row groups are in the file, there actually isn't anything in
    # the file that suggests a dictionary. We should read that empty column
    # back as strings.

    # In this example, `pyarrow.string()` is equivalent to
    # `pyarrow.dictionary(pyarrow.int32(), pyarrow.string())`
    table = pyarrow.Table.from_batches(
        [], schema=pyarrow.schema([("A", pyarrow.string()), ("B", pyarrow.int32())])
    )
    _test_read_write_table(table)


def test_skip_rows_at_start():
    table = pyarrow.table({"A": list(range(203))})
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path, "0-10", "200-203")
        assert_table_equals(result, pyarrow.table({"A": [200, 201, 202]}))


def test_skip_more_rows_than_page_size_at_start():
    table = pyarrow.table({"A": list(range(10008))})
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path, "0-10", "10006-10008")
        assert_table_equals(result, pyarrow.table({"A": [10006, 10007]}))


def test_select_column():
    table = pyarrow.table({"A": [1], "B": [2], "C": [3], "D": [4], "E": [5]})
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path, "2-4", "0-100")
        expected = pyarrow.table({"C": [3], "D": [4]})
        assert_table_equals(result, expected)
