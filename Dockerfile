FROM debian:bullseye AS cpp-builddeps

# DEBUG SYMBOLS: to build with debug symbols (which help gdb), run
# `docker build --build-arg CMAKE_BUILD_TYPE=Debug ...`
#
# We install libstdc++6-10-dbg, gdb and time regardless. They don't affect final
# image size in Release mode.
ARG CMAKE_BUILD_TYPE=Release

# We build Arrow ourselves instead of using precompiled binaries. Two reasons:
#
# 1. File size. These statically-linked executables get copied and run a lot.
#    Smaller files mean faster deploys (and marginally-faster start time).
# 2. Dev experience. With --build-arg CMAKE_BUILD_TYPE=Debug, we can help this
#    package's maintainer get a useful stack trace sooner.

RUN true \
      && apt-get update \
      && apt-get install -y --no-install-recommends \
          build-essential \
          ca-certificates \
          cmake \
          curl \
          gdb \
          libc-dbg \
          libdouble-conversion-dev \
          libgflags-dev \
          libstdc++6-10-dbg \
          pkg-config \
          tar \
          time

RUN true \
      && mkdir -p /src \
      && cd /src \
      && curl -L "http://www.apache.org/dyn/closer.lua?filename=arrow/arrow-4.0.1/apache-arrow-4.0.1.tar.gz&action=download" | tar xz

COPY arrow-patches/ /arrow-patches/

RUN true \
      && cd /src/apache-arrow-4.0.1 \
      && for patch in $(find /arrow-patches/*.diff | sort); do patch --verbose -p1 <$patch; done \
      && cd cpp \
      && cmake \
          -DARROW_PARQUET=ON \
          -DARROW_COMPUTE=OFF \
          -DARROW_WITH_SNAPPY=ON \
          -DARROW_WITH_UTF8PROC=OFF \
          -DARROW_WITH_RE2=OFF \
          -DARROW_SNAPPY_USE_SHARED=OFF \
          -DARROW_OPTIONAL_INSTALL=ON \
          -DARROW_BUILD_STATIC=ON \
          -DARROW_BUILD_SHARED=OFF \
          -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE . \
      && make -j4 arrow arrow_bundled_dependencies parquet \
      && make install


FROM python:3.9.6-buster AS python-dev

RUN pip install pyarrow==4.0.1 pytest pandas==1.3.0 fastparquet

RUN mkdir /app
WORKDIR /app


FROM cpp-builddeps AS cpp-build

RUN mkdir -p /app/src
RUN touch /app/src/parquet-diff.cc /app/src/parquet-to-text-stream.cc /app/src/parquet-to-arrow.cc /app/src/common.cc /app/src/range.cc
WORKDIR /app
COPY CMakeLists.txt /app
# Redeclare CMAKE_BUILD_TYPE: its scope is its build stage
ARG CMAKE_BUILD_TYPE=Release
RUN cmake -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE .

COPY src/ /app/src/
RUN VERBOSE=true make -j4 install
# RUN VERBOSE=true make -j4 install/strip
# Display size. In v2.1, it's ~7MB per executable.
RUN ls -lh /usr/bin/parquet-*


FROM python-dev AS test

COPY --from=cpp-build /usr/bin/parquet-* /usr/bin/
COPY tests/ /app/tests/
WORKDIR /app
RUN pytest -s -vv


FROM scratch AS dist
COPY --from=cpp-build /usr/bin/parquet-* /usr/bin/
