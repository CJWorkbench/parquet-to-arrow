from datetime import datetime
import json
from pathlib import Path
import subprocess
import numpy as np
import pandas as pd
import pyarrow
from .util import parquet_file


def canonical_json(data) -> str:
    return json.dumps(
        data,
        ensure_ascii=False,
        check_circular=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def do_convert(parquet_path: Path, format: str) -> str:
    try:
        completed = subprocess.run(
            ["/usr/bin/parquet-to-text-stream", str(parquet_path), format],
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as err:
        # Rewrite error so it's easy to read in test-result stack trace
        raise RuntimeError(
            "Process failed with code %d: %s"
            % (err.returncode, err.stdout + err.stderr)
        ) from None

    if len(completed.stderr):
        raise RuntimeError("Stderr should be empty, but was: %s" % completed.stderr)
    return completed.stdout


def _test_convert_via_arrow(table: pyarrow.Table, expect_csv, expect_json) -> None:
    """
    Convert `table` from Arrow to parquet; then stream the Parquet file.

    This is an intuitive interface for testing happy-path Parquet files.
    """
    with parquet_file(table) as parquet_path:
        csv = do_convert(parquet_path, "csv")
        if isinstance(expect_csv, str):
            expect_csv = expect_csv.encode("utf-8")
        assert csv == expect_csv

        json_text = do_convert(parquet_path, "json")
        if isinstance(expect_json, list):
            expect_json = canonical_json(expect_json)
        if isinstance(expect_json, str):
            expect_json = expect_json.encode("utf-8")
        assert json_text == expect_json


def test_convert_int32_int64():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "i64": pyarrow.array(
                    [1, 4611686018427387904, -2, None], type=pyarrow.int64()
                ),
                "i32": pyarrow.array([1, 1073741824, -2, None], type=pyarrow.int32()),
            }
        ),
        "i64,i32\n1,1\n4611686018427387904,1073741824\n-2,-2\n,",
        [
            {"i64": 1, "i32": 1},
            {"i64": 4611686018427387904, "i32": 1073741824},
            {"i64": -2, "i32": -2},
            {"i64": None, "i32": None},
        ],
    )


def test_convert_int8_int16():
    # parquet only stores int32/int64 values natively: int8 and int16 are
    # written as int32 and their "Logical Type" is stored in column metadata.
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "i8": pyarrow.array([1, -32, 120, None], type=pyarrow.int8()),
                "i16": pyarrow.array([1, -320, 31022, None], type=pyarrow.int16()),
            }
        ),
        "i8,i16\n1,1\n-32,-320\n120,31022\n,",
        [
            {"i8": 1, "i16": 1},
            {"i8": -32, "i16": -320},
            {"i8": 120, "i16": 31022},
            {"i8": None, "i16": None},
        ],
    )


def test_convert_uint64():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "u64": pyarrow.array(
                    [1, 9223372039002259456, None], type=pyarrow.uint64()
                )
            }
        ),
        "u64\n1\n9223372039002259456\n",
        [{"u64": 1}, {"u64": 9223372039002259456}, {"u64": None}],
    )


def test_convert_uint8_uint16_uint32():
    # parquet only stores int32/int64 values natively. These are upcast to
    # be encoded.
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "u8": pyarrow.array([1, 138, None], type=pyarrow.uint8()),
                "u16": pyarrow.array([1, 38383, None], type=pyarrow.uint16()),
                "u32": pyarrow.array([1, 4294967291, None], type=pyarrow.uint32()),
            }
        ),
        "u8,u16,u32\n1,1,1\n138,38383,4294967291\n,,",
        [
            dict(u8=1, u16=1, u32=1),
            dict(u8=138, u16=38383, u32=4294967291),
            dict(u8=None, u16=None, u32=None),
        ],
    )


def test_convert_f32_f64():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "f32": pyarrow.array(
                    np.array(
                        [
                            0.12314,
                            # too precise -- float32 does not store this
                            9999999999999999999.0,
                            float("inf"),
                            float("-inf"),
                            float("nan"),
                            None,
                        ],
                        dtype=np.float32,
                    ),
                    type=pyarrow.float32(),
                ),
                "f64": pyarrow.array(
                    [
                        0.12314,
                        # too many digits
                        9999999999999999999999999999999999999999999999999999.0,
                        float("inf"),
                        float("-inf"),
                        float("nan"),
                        None,
                    ],
                    type=pyarrow.float64(),
                ),
            }
        ),
        "f32,f64\n0.12314,0.12314\n10000000000000000000,1e+52\n,\n,\n,\n,",
        r'[{"f32":0.12314,"f64":0.12314},{"f32":10000000000000000000,"f64":1e+52},{"f32":null,"f64":null},{"f32":null,"f64":null},{"f32":null,"f64":null},{"f32":null,"f64":null}]',
    )


def test_convert_text():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "A": ["x", None, "y", "a,b", "c\nd", 'a"\\b"c', "0\x001\x022\r3\n4\t5"],
                "B": ["", "a", "b", "c", "d", "e", "f"],
            }
        ),
        """A,B\nx,\n,a\ny,b\n"a,b",c\n"c\nd",d\n"a""\\b""c",e\n"0\x001\x022\r3\n4\t5",f""",
        [
            dict(A="x", B=""),
            dict(A=None, B="a"),
            dict(A="y", B="b"),
            dict(A="a,b", B="c"),
            dict(A="c\nd", B="d"),
            dict(A='a"\\b"c', B="e"),
            dict(A="0\x001\x022\r3\n4\t5", B="f"),
        ],
    )


def test_convert_text_dictionaries():
    table = pyarrow.table(
        {"A": pyarrow.array(["x", "x", "y", "x", None, "y"]).dictionary_encode()}
    )
    with parquet_file(table, use_dictionary=[b"A"]) as parquet_path:
        assert do_convert(parquet_path, "csv") == b"A\nx\nx\ny\nx\n\ny"
        assert do_convert(parquet_path, "json") == canonical_json(
            [{"A": "x"}, {"A": "x"}, {"A": "y"}, {"A": "x"}, {"A": None}, {"A": "y"}]
        ).encode("utf-8")


def test_convert_text_over_batch_size():
    # Seen on production:
    # "Failure concatenating column chunks: NotImplemented: Concat with
    # dictionary unification NYI"
    BATCH_SIZE = 100  # copy parquet-to-text-stream.cc

    table = pyarrow.table(
        {"A": pyarrow.array(["x"] * BATCH_SIZE + ["y", "x", None, "y"])}
    )
    with parquet_file(table, use_dictionary=[b"A"]) as parquet_path:
        assert do_convert(parquet_path, "csv") == b"A" + (b"\nx" * 100) + b"\ny\nx\n\ny"
        assert do_convert(parquet_path, "json") == canonical_json(
            ([{"A": "x"}] * 100) + [{"A": "y"}, {"A": "x"}, {"A": None}, {"A": "y"}]
        ).encode("utf-8")


def test_convert_text_dictionaries_over_batch_size():
    # Seen on production:
    # "Failure concatenating column chunks: NotImplemented: Concat with
    # dictionary unification NYI"
    BATCH_SIZE = 100  # copy parquet-to-text-stream.cc

    table = pyarrow.table(
        {
            "A": pyarrow.array(
                ["x"] * BATCH_SIZE + ["y", "x", None, "y"]
            ).dictionary_encode()
        }
    )
    with parquet_file(table, use_dictionary=[b"A"]) as parquet_path:
        assert do_convert(parquet_path, "csv") == b"A" + (b"\nx" * 100) + b"\ny\nx\n\ny"
        assert do_convert(parquet_path, "json") == canonical_json(
            ([{"A": "x"}] * 100) + [{"A": "y"}, {"A": "x"}, {"A": None}, {"A": "y"}]
        ).encode("utf-8")


def test_convert_na_only_categorical():
    table = pyarrow.table(
        {"A": pyarrow.array([None], type=pyarrow.string()).dictionary_encode()}
    )
    with parquet_file(table, use_dictionary=[b"A"]) as parquet_path:
        assert do_convert(parquet_path, "csv") == b"A\n"
        assert do_convert(parquet_path, "json") == b'[{"A":null}]'


def test_convert_zero_row_groups():
    table = pyarrow.Table.from_batches(
        [], schema=pyarrow.schema([("A", pyarrow.string()), ("B", pyarrow.int32())])
    )
    with parquet_file(table) as parquet_path:
        assert do_convert(parquet_path, "csv") == b"A,B"
        assert do_convert(parquet_path, "json") == b"[]"


def test_convert_zero_rows():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "A": pyarrow.array([], type=pyarrow.string()),
                "B": pyarrow.array([], type=pyarrow.int32()),
            }
        ),
        "A,B",
        "[]",
    )


# def test_convert_datetime_s():
#     # Parquet has no "s" option like Arrow's.


def test_convert_datetime_ms():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "ms": pyarrow.array(
                    [
                        datetime(2019, 3, 4),
                        datetime(2019, 3, 4, 5),
                        datetime(2019, 3, 4, 5, 6),
                        datetime(2019, 3, 4, 5, 6, 7),
                        datetime(2019, 3, 4, 0, 0, 0, 8000),
                        None,
                        None,
                    ],
                    type=pyarrow.timestamp(unit="ms"),
                )
            }
        ),
        "ms\n2019-03-04\n2019-03-04T05:00:00Z\n2019-03-04T05:06:00Z\n2019-03-04T05:06:07Z\n2019-03-04T00:00:00.008Z\n\n",
        [
            {"ms": "2019-03-04"},
            {"ms": "2019-03-04T05:00:00Z"},
            {"ms": "2019-03-04T05:06:00Z"},
            {"ms": "2019-03-04T05:06:07Z"},
            {"ms": "2019-03-04T00:00:00.008Z"},
            {"ms": None},
            {"ms": None},
        ],
    )


def test_convert_datetime_us():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "us": pyarrow.array(
                    [
                        datetime(2019, 3, 4),
                        datetime(2019, 3, 4, 5),
                        datetime(2019, 3, 4, 5, 6),
                        datetime(2019, 3, 4, 5, 6, 7),
                        datetime(2019, 3, 4, 5, 6, 7, 8000),
                        datetime(2019, 3, 4, 5, 6, 7, 8),  # microsecond
                        None,
                    ],
                    type=pyarrow.timestamp(unit="us"),
                )
            }
        ),
        "us\n2019-03-04\n2019-03-04T05:00:00Z\n2019-03-04T05:06:00Z\n2019-03-04T05:06:07Z\n2019-03-04T05:06:07.008Z\n2019-03-04T05:06:07.000008Z\n",
        [
            {"us": "2019-03-04"},
            {"us": "2019-03-04T05:00:00Z"},
            {"us": "2019-03-04T05:06:00Z"},
            {"us": "2019-03-04T05:06:07Z"},
            {"us": "2019-03-04T05:06:07.008Z"},
            {"us": "2019-03-04T05:06:07.000008Z"},
            {"us": None},
        ],
    )


def test_convert_datetime_ns():
    _test_convert_via_arrow(
        pyarrow.table(
            {
                "ns": pyarrow.array(
                    [
                        pd.Timestamp(2019, 3, 4).value,
                        pd.Timestamp(2019, 3, 4, 5).value,
                        pd.Timestamp(2019, 3, 4, 5, 6).value,
                        pd.Timestamp(2019, 3, 4, 5, 6, 7).value,
                        pd.Timestamp(2019, 3, 4, 5, 6, 7, 8000).value,
                        pd.Timestamp(2019, 3, 4, 5, 6, 7, 8).value,
                        pd.Timestamp(2019, 3, 4, 5, 6, 7, 0, 8).value,
                    ],
                    type=pyarrow.timestamp(unit="ns"),
                )
            }
        ),
        "ns\n2019-03-04\n2019-03-04T05:00:00Z\n2019-03-04T05:06:00Z\n2019-03-04T05:06:07Z\n2019-03-04T05:06:07.008Z\n2019-03-04T05:06:07.000008Z\n2019-03-04T05:06:07.000000008Z",
        [
            {"ns": "2019-03-04"},
            {"ns": "2019-03-04T05:00:00Z"},
            {"ns": "2019-03-04T05:06:00Z"},
            {"ns": "2019-03-04T05:06:07Z"},
            {"ns": "2019-03-04T05:06:07.008Z"},
            {"ns": "2019-03-04T05:06:07.000008Z"},
            {"ns": "2019-03-04T05:06:07.000000008Z"},
        ],
    )
