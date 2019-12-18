FROM debian:buster AS cpp-builddeps

# DEBUG SYMBOLS: to build with debug symbols (which help gdb), do these
# changes (but don't commit them):
#
# * in Dockerfile, ensure libstdc++6-8-dbg is installed (this, we commit)
# * in Dockerfile, set -DCMAKE_BUILD_TYPE=Debug when building Arrow
# * in Dockerfile, set -DCMAKE_BUILD_TYPE=Debug when building parquet-to-arrow

RUN true \
      && apt-get update \
      && apt-get install -y \
          autoconf \
          bison \
          build-essential \
          cmake \
          curl \
          flex \
          g++ \
          gnupg \
          libboost-dev \
          libboost-filesystem-dev \
          libboost-regex-dev \
          libboost-system-dev \
          libstdc++6-8-dbg \
          pkg-config \
          python \
          tar \
      && true

COPY arrow-patches/ /arrow-patches/

RUN true \
      && mkdir -p /src \
      && cd /src \
      && curl -Oapache-arrow-0.15.0.tar.gz --location http://archive.apache.org/dist/arrow/arrow-0.15.0/apache-arrow-0.15.0.tar.gz \
      && tar zxf apache-arrow-0.15.0.tar.gz \
      && cd apache-arrow-0.15.0 \
      && for patch in $(find /arrow-patches/*.diff | sort); do patch --verbose -p1 <$patch; done \
      && cd cpp \
      && cmake -DARROW_PARQUET=ON -DARROW_COMPUTE=ON -DARROW_OPTIONAL_INSTALL=ON -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF -DCMAKE_BUILD_TYPE=Release . \
      && make -j4 arrow \
      && make -j4 parquet \
      && make install

ENV PKG_CONFIG_PATH "/src/apache-arrow-0.15.0/cpp/gflags_ep-prefix/src/gflags_ep/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/zlib_ep/src/zlib_ep-install/share/pkgconfig:/src/apache-arrow-0.15.0/cpp/zstd_ep-install/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/brotli_ep/src/brotli_ep-install/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/zstd_ep-prefix/src/zstd_ep-build/lib:/src/apache-arrow-0.15.0/cpp/gflags_ep-prefix/src/gflags_ep/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/zlib_ep/src/zlib_ep-install/share/pkgconfig:/src/apache-arrow-0.15.0/cpp/zstd_ep-install/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/brotli_ep-prefix/src/brotli_ep-install/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/pkgconfig:/src/apache-arrow-0.15.0/cpp/zstd_ep-prefix/src/zstd_ep-build/lib"
ENV CMAKE_PREFIX_PATH "/src/apache-arrow-0.15.0/cpp/thrift_ep/src/thrift_ep-install:/src/apache-arrow-0.15.0/cpp/lz4_ep-prefix/src/lz4_ep:/src/apache-arrow-0.15.0/cpp/brotli_ep/src/brotli_ep-install:/src/apache-arrow-0.15.0/cpp/double-conversion_ep/src/double-conversion_ep"
ENV Snappy_DIR "/src/apache-arrow-0.15.0/cpp/snappy_ep/src/snappy_ep-install/lib/cmake/Snappy"


FROM python:3.7.4-buster AS python-dev

RUN pip install pyarrow==0.15.0 pytest pandas==0.25.1 fastparquet==0.3.2

RUN mkdir /app
WORKDIR /app


FROM cpp-builddeps AS cpp-build

RUN mkdir -p /app/src
RUN touch /app/src/parquet-diff.cc /app/src/parquet-to-arrow-slice.cc /app/src/parquet-to-text-stream.cc /app/src/parquet-to-arrow.cc /app/src/common.cc /app/src/range.cc
WORKDIR /app
COPY CMakeLists.txt /app
RUN cmake -DCMAKE_BUILD_TYPE=Release .
#RUN cmake .

COPY src/ /app/src/
RUN make -j4


FROM python-dev AS test

COPY --from=cpp-build /app/parquet-diff /usr/bin/parquet-diff
COPY --from=cpp-build /app/parquet-to-arrow /usr/bin/parquet-to-arrow
COPY --from=cpp-build /app/parquet-to-arrow-slice /usr/bin/parquet-to-arrow-slice
COPY --from=cpp-build /app/parquet-to-text-stream /usr/bin/parquet-to-text-stream
COPY tests/ /app/tests/
WORKDIR /app
RUN pytest -vv


FROM scratch AS dist
COPY --from=cpp-build /app/parquet-diff /usr/bin/parquet-diff
COPY --from=cpp-build /app/parquet-to-arrow /usr/bin/parquet-to-arrow
COPY --from=cpp-build /app/parquet-to-arrow-slice /usr/bin/parquet-to-arrow-slice
COPY --from=cpp-build /app/parquet-to-text-stream /usr/bin/parquet-to-text-stream
