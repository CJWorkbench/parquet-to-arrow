parquet-to-arrow
================

Statically-linked Linux binaries for dealing with Parquet files.

Usage
=====

Use this in your Docker images. For instance:

```
# (look to Git tags to find latest VERSION)
FROM workbenchdata:parquet-to-arrow:VERSION AS parquet-to-arrow

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

parquet-to-arrow-slice
----------------------

*Purpose*: convert a small fraction of a Parquet file to Arrow format.

*Usage*: `parquet-to-arrow-slice input.parquet 0-10 0-200 output.arrow`
(where `0-10` is a selection of columns and `0-200` is a selection of
rows).

*Features*:

* _Manageable RAM usage_: in large datasets, never hold an entire column
  in memory.
* _Auto-convert UTF-8 dictionaries_: this tool is tuned to small numbers
  of rows; dictionaries are unneeded hassle.


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
