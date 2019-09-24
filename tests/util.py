from contextlib import contextmanager, suppress
import pathlib
import os
import tempfile
from typing import ContextManager
import unittest
from numpy.testing import assert_equal  # [None, "x"] == [None, "x"]
import pyarrow
import pyarrow.parquet


def assert_table_equals(actual: pyarrow.Table, expected: pyarrow.Table) -> None:
    assertEqual = unittest.TestCase().assertEqual
    assertEqual(actual.num_rows, expected.num_rows)
    assertEqual(actual.num_columns, expected.num_columns)

    for column_number, actual_column, expected_column in zip(
        range(actual.num_columns), actual.columns, expected.columns
    ):
        assertEqual(
            actual_column.name,
            expected_column.name,
            f"column {column_number} has wrong name",
        )
        assertEqual(
            actual_column.type,
            expected_column.type,
            f"column {actual_column.name} has wrong type",
        )
        actual_data = actual_column.data.to_pandas()  # numpy.ndarray
        expected_data = expected_column.data.to_pandas()  # numpy.ndarray
        assert_equal(
            actual_data, expected_data, f"column {actual_column.name} has wrong data"
        )


@contextmanager
def parquet_file(
    table: pyarrow.Table, use_dictionary=False
) -> ContextManager[pathlib.Path]:
    """
    Yield a filename with `table` written to a Parquet file.
    """
    fd, filename = tempfile.mkstemp()
    try:
        os.close(fd)
        pyarrow.parquet.write_table(
            table,
            filename,
            version="2.0",  # allow int64 ns timestamps
            compression="SNAPPY",
            use_dictionary=use_dictionary,
        )
        yield pathlib.Path(filename)
    finally:
        with suppress(FileNotFoundError):
            os.unlink(filename)
