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
