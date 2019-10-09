from contextlib import contextmanager, suppress
import pathlib
import os
import tempfile
from typing import ContextManager
import unittest
from pandas.testing import assert_series_equal
import pyarrow
import pyarrow.parquet


def assert_table_equals(actual: pyarrow.Table, expected: pyarrow.Table) -> None:
    assertEqual = unittest.TestCase().assertEqual
    assertEqual(actual.num_rows, expected.num_rows)
    assertEqual(actual.num_columns, expected.num_columns)

    for (
        column_number,
        actual_name,
        actual_column,
        expected_name,
        expected_column,
    ) in zip(
        range(actual.num_columns),
        actual.column_names,
        actual.columns,
        expected.column_names,
        expected.columns,
    ):
        assertEqual(
            actual_name, expected_name, f"column {column_number} has wrong name"
        )
        assertEqual(
            actual_column.type,
            expected_column.type,
            f"column {actual_name} has wrong type",
        )
        actual_data = actual_column.to_pandas()
        expected_data = expected_column.to_pandas()
        assert_series_equal(
            actual_data, expected_data, f"column {actual_name} has wrong data"
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
