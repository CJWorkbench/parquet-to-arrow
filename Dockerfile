FROM debian:buster AS cpp-builddeps

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
          pkg-config \
          python \
          tar \
      && true

RUN true \
      && mkdir -p /src \
      && cd /src \
      && curl -Oapache-arrow-0.14.1.tar.gz --location http://apache.mirror.gtcomm.net/arrow/arrow-0.14.1/apache-arrow-0.14.1.tar.gz \
      && tar zxf apache-arrow-0.14.1.tar.gz \
      && cd apache-arrow-0.14.1/cpp \
      && cmake -DARROW_PARQUET=ON -DARROW_COMPUTE=ON -DARROW_OPTIONAL_INSTALL=ON -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF . \
      && make -j4 arrow \
      && make -j4 parquet \
      && make install

ENV PKG_CONFIG_PATH "/src/apache-arrow-0.14.1/cpp/gflags_ep-prefix/src/gflags_ep/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/zlib_ep/src/zlib_ep-install/share/pkgconfig:/src/apache-arrow-0.14.1/cpp/zstd_ep-install/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/brotli_ep/src/brotli_ep-install/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/zstd_ep-prefix/src/zstd_ep-build/lib:/src/apache-arrow-0.14.1/cpp/gflags_ep-prefix/src/gflags_ep/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/zlib_ep/src/zlib_ep-install/share/pkgconfig:/src/apache-arrow-0.14.1/cpp/zstd_ep-install/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/brotli_ep-prefix/src/brotli_ep-install/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/pkgconfig:/src/apache-arrow-0.14.1/cpp/zstd_ep-prefix/src/zstd_ep-build/lib"
ENV CMAKE_PREFIX_PATH "/src/apache-arrow-0.14.1/cpp/thrift_ep/src/thrift_ep-install:/src/apache-arrow-0.14.1/cpp/lz4_ep-prefix/src/lz4_ep:/src/apache-arrow-0.14.1/cpp/brotli_ep/src/brotli_ep-install:/src/apache-arrow-0.14.1/cpp/double-conversion_ep/src/double-conversion_ep"
ENV Snappy_DIR "/src/apache-arrow-0.14.1/cpp/snappy_ep/src/snappy_ep-install/lib/cmake/Snappy"


FROM python:3.7.4-buster AS python-dev

RUN pip install pyarrow==0.14.1 pytest pandas==0.25.1

RUN mkdir /app
WORKDIR /app


FROM cpp-builddeps AS cpp-build

RUN mkdir -p /app/src
RUN touch /app/src/parquet-to-arrow-slice.cc
WORKDIR /app
COPY CMakeLists.txt /app
#RUN cmake -DCMAKE_BUILD_TYPE=Debug .
RUN cmake .

COPY src/ /app/src/
RUN make


FROM python-dev AS test

COPY . /app
COPY --from=cpp-build /app/parquet-to-arrow-slice /usr/bin/parquet-to-arrow-slice
WORKDIR /app
RUN pytest


FROM scratch AS dist
COPY --from=cpp-build /app/parquet-to-arrow-slice /usr/bin/parquet-to-arrow-slice
