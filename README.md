parquet-to-arrow
================

Statically-linked Linux binaries for dealing with Parquet files.

Usage
=====

Use this in your Docker images. For instance:

```
# (look to Git tags to find latest VERSION)
FROM workbenchdata/parquet-to-arrow:VERSION AS parquet-to-arrow

FROM debian:buster AS my-normal-build
# ... normal Dockerfile stuff...
# ... normal Dockerfile stuff...
COPY --from=parquet-to-arrow /usr/bin/parquet-to-arrow-slice /usr/bin/parquet-to-arrow-slice
# ... normal Dockerfile stuff...
```

... and now your programs running in that Docker container have access
to these binaries.

Binaries
========

parquet-to-arrow
----------------

*Purpose*: convert a Parquet file to Arrow format.

*Usage*: `parquet-to-arrow input.parquet output.arrow`

*Features*:

* _Preserve dictionary encoding_: Parquet dictionaries become Arrow
  dictionaries. See https://arrow.apache.org/blog/2019/09/05/faster-strings-cpp-parquet/

You may choose to invoke `parquet-to-arrow` even from Python, where `pyarrow`
has the same features. That way, if the kernel out-of-memory killer kills
`parquet-to-arrow`, your Python code can handle the error. (TODO limit RAM by
converting and writing one column at a time.)

csv-to-arrow
------------

*Purpose*: convert a CSV file to Arrow format, permissively and RAM-safely.

*Usage*:

```
csv-to-arrow input.csv output.arrow \
    --delimiter=, \
    --max-columns=1000 \
    --max-rows=1000000 \
    --max-bytes-per-value=32768
```

`csv-to-arrow` holds the entire Arrot table in RAM. To restrict RAM usage,
truncate the input file beforehand.

Also, `csv-to-arrow` assumes valid UTF-8. To prevent errors, validate the input
file beforehand.

If you plan to load the output into Pandas, be aware that a Python `str` costs
about 50 bytes of overhead. As a heuristic, loading the output Arrow file as a
`pandas.DataFrame` costs:

* The size of all the bytes of text -- roughly the size of the Arrow file
* 8 bytes per cell for a pointer -- `8 * table.num_columns * table.num_rows`
* 50 bytes per string --
  `50 * (table.num_columns * table.num_rows - sum([col.null_count for col in table.columns]))`

Memory savings can be found if strings are duplicated in a column:
`column.dictionary_encode()` will save 50 bytes per duplicate; and 
[Pandas Categoricals](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Categorical.html)
may reduce a column's 8-byte pointers to 4 bytes per value.

*Features*:

* _Graceful limits_: truncate the result if it's too long or wide.
* _Truncate unwanted data_: ignore excess rows and columns; and truncate values
  that surpass the desired limit.
* _Skip charset checks_: assume UTF-8 input.
* _Variable row lengths_: allow any new row to add new columns (back-filling
  `null` for prior rows).
* _No types_: every value is a string.
* _No column names_: columns are named `"0"`, `"1"`, etc.
* _Skip empty rows_: newline-only rows are ignored.
* _Universal newlines_: `\r`, `\n` and any sequence of them start a new record.
* _Permit any characters_: control characters and `\0` are not a problem. Also,
  `"` is allowed in an unquoted value (except, of course, as the first
  character).
* _Recover on unclosed quote_: close a quote if we end on an unclosed quote.
* _Recover on invalid value_: data after a close-quote is appended to a value.
* _Warn on stdout_: stdout can produce lines of text matching these patterns:

```
skipped 102312 rows (after row limit of 1000000)
skipped 1 columns (after column limit of 1000)
truncated 123 values (value byte limit is 32768; see row 2 column 1)
repaired 321 values (misplaced quotation marks; see row 3 column 5)
repaired last value (missing quotation mark)
```

(Note `skipped 1 columns` is plural. The intent is for callers to parse using
regular expressions, so the `s` is not optional. Also, messages formats won't
change without a major-version bump.


parquet-to-arrow-slice
----------------------

*Purpose*: convert a small fraction of a Parquet file to Arrow format.

*Usage*: `parquet-to-arrow-slice input.parquet 0-10 0-200 output.arrow`
(where `0-10` is a selection of columns and `0-200` is a selection of
rows).

*Features*:

* _Manageable RAM usage_: in large datasets, never hold an entire column
  in memory.
* _Auto-convert UTF-8 dictionaries_ to string columns: this tool is tuned to
  small numbers of rows; dictionaries are unneeded hassle.

*TODO*:

* When Parquet 0.15 comes out, use its enhanced dictionary support to seek
  without converting dictionary to string. That should speed slicing a file
  with an oversized dictionary and hundreds of thousands of rows.

parquet-to-text-stream
----------------------

*Purpose*: stream a Parquet file for public consumption in common format.

*Usage*: `parquet-to-text-stream input.parquet csv > out.csv`
(where the format is one of `csv` or ... uh ... that's it for now!)

*Features*:

* _Manageable RAM usage_: in large datasets, hold a small number of rows (and
  all column dictionaries) in memory.
* _Quick time to first byte_: streaming clients see results quickly.
* _CSV Output_ (choose `csv` format): null/inf/-inf/NaN are all output as empty
  string; all but the most wonky floats are formatted as decimal; timestamps
  are ISO8601-formatted with the fewest characters possible (e.g., "2019-09-24"
  instead of "2019-09-24T00:00:00.000000000Z")
* _JSON Output_ (choose `json` format): null/inf/-inf/NaN are all output as
  `null`; floats are formatted according to
  [ECMAScript Standard](https://www.ecma-international.org/ecma-262/6.0/#sec-tostring-applied-to-the-number-type);
  timestamps are ISO8601-formatted Strings with the fewest characters possible
  (e.g., "2019-09-24" instead of "2019-09-24T00:00:00.000000000Z")

parquet-diff
------------

*Purpose*: exit with status code 0 only if two Parquet files are equal.

*Usage*: `parquet-diff file1.parquet file2.parquet`

*Features*:

* _Manageable RAM usage_: only hold two columns' data in memory at a time.
* _Strict about row groups_: two files with different row-group counts or
  lengths are different.
* _Strict about physical types_: int32 and int64 are different, even if their
  values are equivalent.
* _Strict about logical types_: int8 and int16 are different, even if they're
  both equal int32 values in the file.
* _Strict about timestamp precision_: columns with different `isAdjustedToUTC`
  or `precision` are different, even if their values are equivalent.
* _Strict about Unicode normalization_: two strings are different if their
  UTF-8 byte sequences are different -- regardless of whether they are
  canonically equivalent according to Unicode.
* _Loose about column encoding_: dictionary-encoded strings and plain strings
  are equal if their string values are equal.
* _Loose about versions_: Parquet v1.0 and v2.0 files may compare as equal.
* _Loose about null_: the array `[1, null, 2]` is equal to another array
  `[1, null, 2]`, because `null == null`.

Developing
==========

`docker build .` will compile and unit-test everything.

To make a faster development loop when you need it most, use `docker` to
jump in with an intermediate image. You can see all `IMAGE_ID`s in the
`docker build .` output.

* `docker run -it --rm --volume "$(pwd):/app" IMAGE_ID bash` -- start a
  Bash shell, and overwrite the image's source code with our own. The mounted
  volume means you can `make` or `pytest` immediately after you edit source
  code. (That's not normal.)


Deploying
=========

1. Write to `CHANGELOG.md`
2. `git commit`
3. `git tag VERSION` (use semver -- e.g., `v1.2.3`)
4. `git push --tags && git push`
5. Wait; Docker Hub will publish the new image
