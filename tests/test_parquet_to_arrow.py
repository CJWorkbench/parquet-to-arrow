from datetime import datetime
from pathlib import Path
import subprocess
import tempfile
import numpy as np
import fastparquet
import pandas as pd
import pyarrow
import pytest
from .util import parquet_file, assert_table_equals


def do_convert(parquet_path: Path) -> pyarrow.Table:
    with tempfile.NamedTemporaryFile() as arrow_file:
        try:
            subprocess.run(
                ["/usr/bin/parquet-to-arrow", str(parquet_path), arrow_file.name],
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


def _test_read_write_table(table: pyarrow.Table) -> None:
    with parquet_file(table) as parquet_path:
        result = do_convert(parquet_path)
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


@pytest.mark.filterwarnings("ignore:RangeIndex._:DeprecationWarning")
def test_read_fastparquet_text_categorical():
    dataframe = pd.DataFrame(
        {"A": pd.Series(["x", None, "y", "x", "x"], dtype="category")}
    )
    with tempfile.NamedTemporaryFile() as tf:
        fastparquet.write(tf.name, dataframe, object_encoding="utf8")
        result = do_convert(Path(tf.name))
        expected = pyarrow.table(
            {"A": pyarrow.array(["x", None, "y", "x", "x"]).dictionary_encode()}
        )
        assert_table_equals(result, expected)


def test_read_write_datetime():
    _test_read_write_table(pyarrow.table({"A": [datetime.now(), None, datetime.now()]}))


def test_na_only_categorical_has_categorical_type():
    # Start with a Categorical with no values. (In Workbench, all
    # Categoricals are text.)
    table = pyarrow.table(
        {"A": pyarrow.array([None], type=pyarrow.string()).dictionary_encode()}
    )
    with parquet_file(table, use_dictionary=[b"A"]) as parquet_path:
        result = do_convert(parquet_path)
        assert_table_equals(result, table)


def test_empty_categorical_has_categorical_type():
    table = pyarrow.table(
        {
            "A": pyarrow.DictionaryArray.from_arrays(
                pyarrow.array([], type=pyarrow.int32()),
                pyarrow.array([], type=pyarrow.string()),
            )
        }
    )
    with parquet_file(table, use_dictionary=[b"A"]) as parquet_path:
        result = do_convert(parquet_path)
        assert_table_equals(result, table)


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


def test_invalid_parquet():
    with tempfile.NamedTemporaryFile() as tf:
        tf.write(b"XXX NOT PARQUET")
        tf.flush()
        with tempfile.NamedTemporaryFile() as arrow_file:
            child = subprocess.run(
                ["/usr/bin/parquet-to-arrow", tf.name, arrow_file.name],
                capture_output=True,
            )

    assert child.returncode == 1
    assert child.stdout == b""
    assert child.stderr == (
        b"Invalid: Parquet magic bytes not found in footer. Either the file is corrupted"
        b" or this is not a parquet file.\n"
    )


@pytest.mark.filterwarnings("ignore:RangeIndex._:DeprecationWarning")
def test_empty_categorical_with_zero_row_groups_has_object_type():
    # In Parquet, "dictionary encoding" is a property of each chunk, not of the
    # column. So if there are zero chunks, we can't know whether we want
    # dictionary encoding.
    #
    # Assume no dictionary -- that's simpler.
    dataframe = pd.DataFrame({"A": pd.Series([], dtype=str).astype("category")})
    with tempfile.NamedTemporaryFile() as parquet_file:
        fastparquet.write(
            parquet_file.name, dataframe, row_group_offsets=[], write_index=False
        )

        result = do_convert(Path(parquet_file.name))
        expected = pyarrow.table({"A": pyarrow.array([], type=pyarrow.string())})
        assert_table_equals(result, expected)
