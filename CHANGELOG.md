v2.0.1 - 2020-03-03
-------------------

* `parquet-to-text-stream`: fix ISO8601-formatting of dates before 1970.

v2.0.0 - 2020-02-19
-------------------

* `parquet-to-arrow-slice`: delete this binary, because nobody is using it.
  (This is why there's a major-version bump.)
* Upgrade to Arrow 0.16.0.

v1.1.0 - 2019-12-18
-------------------

* `parquet-to-text-stream`: add `--column-range` and `--row-range` flags

v1.0.0 - 2019-11-25
-------------------

* Move `csv-to-arrow` to a separate project. Focus on Parquet here.

v0.3.0 - 2019-11-22
-------------------

* `csv-to-arrow`: not Parquet-related, but so handy and fast....

v0.2.0 - 2019-10-21
-------------------

* `parquet-diff`: return 0 if two Parquet files are equivalent.

v0.1.3 - 2019-10-15
-------------------

* `parquet-to-text-stream`, `parquet-to-slice`: patch Arrow bug
   https://issues.apache.org/jira/browse/ARROW-6895 so programs correctly
   translate dictionary-encoded columns of over 100 rows.

v0.1.2 - 2019-10-09
-------------------

* `parquet-to-arrow`: patch Arrow bug
   https://issues.apache.org/jira/browse/ARROW-6861 so Parquet files written
   by Arrow 0.14.1 can be converted, preserving dictionaries.

v0.1.1 - 2019-10-09
-------------------

* `parquet-to-arrow`: convert the whole file.

v0.1.0 - 2019-10-08
-------------------

* Upgrade to Arrow 0.15. Arrow files written by `parquet-to-arrow-slice` can
  only be read by Arrow 0.15.

v0.0.4 - 2019-09-25
-------------------

* `parquet-to-text-stream`: use unlocked stdio, for speed

v0.0.3 - 2019-09-25
-------------------

* `parquet-to-text-stream`: add `json` format

v0.0.2 - 2019-09-24
-------------------

* Added `parquet-to-text-stream` supporting `csv` format

v0.0.1 - 2019-09-23
-------------------

* Initial release, with `parquet-to-arrow-slice`
