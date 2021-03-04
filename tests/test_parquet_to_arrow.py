import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

import pyarrow

from .util import assert_table_equals, empty_file, parquet_file


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


#     with tempfile.NamedTemporaryFile() as tf:
#         fastparquet.write(tf.name, dataframe, object_encoding="utf8")
#         result = do_convert(Path(tf.name))
#         expected = pyarrow.table(
#             {"A": pyarrow.array(["x", None, "y", "x", "x"]).dictionary_encode()}
#         )
#         assert_table_equals(result, expected)
@pytest.mark.filterwarnings("ignore:RangeIndex._:DeprecationWarning")
def test_read_fastparquet_text_categorical():
    # To write this file, install fastparquet and run:
    #
    # import fastparquet
    # import pandas as pd
    # fastparquet.write(
    #     'x.parquet',
    #     pd.DataFrame({"A": pd.Series(["x", None, "y", "x", "x"], dtype="category")})
    # )
    path = (
        Path(__file__).parent / "files" / "column-A-dictionary-from-fastparquet.parquet"
    )
    result = do_convert(path)
    assert_table_equals(
        result,
        pyarrow.table(
            {"A": pyarrow.array(["x", None, "y", "x", "x"]).dictionary_encode()}
        ),
    )


def test_read_write_timestamp():
    _test_read_write_table(pyarrow.table({"A": [datetime.now(), None, datetime.now()]}))


def test_read_write_date():
    _test_read_write_table(
        pyarrow.table({"A": pyarrow.array([18689, None, -123], pyarrow.date32())})
    )


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
    #
    # Note that pyarrow.parquet can't actually write a file without including
    # Arrow-specific metadata. Other Parquet writers, like fastparquet, can.
    #
    # To write this file, install fastparquet and run:
    #
    # import fastparquet
    # import pandas as pd
    # from fastparquet import parquet_thrift
    # fmd = parquet_thrift.FileMetaData(
    #     num_rows=0,
    #     schema=[
    #         parquet_thrift.SchemaElement(name='schema', num_children=1),
    #         parquet_thrift.SchemaElement(
    #             name='A',
    #             type_length=None,
    #             converted_type=parquet_thrift.ConvertedType.UTF8,
    #             type=parquet_thrift.Type.BYTE_ARRAY,
    #             repetition_type=parquet_thrift.FieldRepetitionType.REQUIRED
    #         ),
    #     ],
    #     version=1,
    #     row_groups=[],
    #     key_value_metadata=[]
    # )
    # fastparquet.writer.write_simple(
    #     'x.parquet',
    #     pd.DataFrame({"A": pd.Series([], dtype=str).astype("category")}),
    #     fmd=fmd,
    #     compression=None,
    #     open_with=fastparquet.writer.default_open,
    #     has_nulls=False,
    #     row_group_offsets=[]
    # )
    path = (
        Path(__file__).parent / "files" / "column-A-string-with-no-row-groups.parquet"
    )
    result = do_convert(path)
    assert_table_equals(
        result,
        pyarrow.Table.from_batches(
            [], schema=pyarrow.schema([("A", pyarrow.string())])
        ),
    )


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
