FROM debian:buster AS cpp-builddeps

# DEBUG SYMBOLS: to build with debug symbols (which help gdb), do these
# changes (but don't commit them):
#
# * in Dockerfile, ensure libstdc++6-8-dbg is installed (this, we commit)
# * in Dockerfile, set -DCMAKE_BUILD_TYPE=Debug when building Arrow
# * in Dockerfile, set -DCMAKE_BUILD_TYPE=Debug when building parquet-to-arrow
# * in CMakeLists.txt, find the library "libthriftd.a" instead of "libthrift.a"

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
          libdouble-conversion-dev \
          libstdc++6-8-dbg \
          pkg-config \
          python \
          tar \
      && true

RUN true \
      && mkdir -p /src \
      && cd /src \
      && curl -Oapache-arrow-0.16.0.tar.gz --location http://apache.mirror.gtcomm.net/arrow/arrow-0.16.0/apache-arrow-0.16.0.tar.gz \
      && tar zxf apache-arrow-0.16.0.tar.gz \
      && cd apache-arrow-0.16.0/cpp \
      && cmake -DARROW_PARQUET=ON -DARROW_COMPUTE=ON -DARROW_WITH_SNAPPY=ON -DARROW_OPTIONAL_INSTALL=ON -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF -DCMAKE_BUILD_TYPE=Release . \
      && make -j4 arrow \
      && make -j4 parquet \
      && make install

ENV PKG_CONFIG_PATH "/src/apache-arrow-0.16.0/cpp/jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/pkgconfig"
ENV CMAKE_PREFIX_PATH "/src/apache-arrow-0.16.0/cpp/thrift_ep/src/thrift_ep-install"
ENV Snappy_DIR "/src/apache-arrow-0.16.0/cpp/snappy_ep/src/snappy_ep-install/lib/cmake/Snappy"


FROM python:3.8.1-buster AS python-dev

RUN pip install pyarrow==0.16.0 pytest pandas==0.25.1 fastparquet==0.3.2

RUN mkdir /app
WORKDIR /app


FROM cpp-builddeps AS cpp-build

RUN mkdir -p /app/src
RUN touch /app/src/parquet-to-text-stream.cc /app/src/parquet-to-arrow.cc /app/src/common.cc
WORKDIR /app
COPY CMakeLists.txt /app
RUN cmake -DCMAKE_BUILD_TYPE=Release .
#RUN cmake .

COPY src/ /app/src/
RUN VERBOSE=true make


FROM python-dev AS test

COPY --from=cpp-build /app/parquet-to-arrow /usr/bin/parquet-to-arrow
COPY --from=cpp-build /app/parquet-to-text-stream /usr/bin/parquet-to-text-stream
COPY tests/ /app/tests/
WORKDIR /app
RUN pytest -vv


FROM scratch AS dist
COPY --from=cpp-build /app/parquet-to-arrow /usr/bin/parquet-to-arrow
COPY --from=cpp-build /app/parquet-to-text-stream /usr/bin/parquet-to-text-stream
