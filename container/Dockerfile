FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    UV_HTTP_TIMEOUT=120 \
    PATH="/venv/bin:/root/.local/bin:$PATH"

# Debian bookworm (python:3.12-slim base). If a package is missing on a
# future base bump, use `apt-cache search <lib>` inside the image to
# find the current name — Debian's 64-bit time_t transition renamed
# many runtime libs with a "t64" suffix (e.g. libhdf5-103 -> libhdf5-
# 103-1t64), and future stable releases may keep churning.
#
# HDF5 is deliberately *not* installed: h5py's manylinux wheels bundle
# their own HDF5 (~4 MB) so we don't need the system runtime. If a user
# needs system-level HDF5 (rare — direct h5cc / mpi builds), they can
# apt-install it in a per-project variant of this image.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tini git \
    libgomp1 libgfortran5 libstdc++6 libgcc-s1 \
    libblas3 liblapack3 libopenblas0-pthread \
    libgl1 libglib2.0-0 libsm6 libxext6 libxrender1 \
    libjpeg62-turbo libpng16-16 libfreetype6 libtiff6 libwebp7 zlib1g \
    libpq5 \
    libgeos-c1v5 libproj25 \
    librdkafka1 \
    libssl3 libkrb5-3 \
    && rm -rf /var/lib/apt/lists/*

# uv is downloaded at build time but *not executed* here — Rosetta and
# qemu-user-static both segfault on uv's Rust binary when the build host
# arch doesn't match the image arch (e.g. building linux/amd64 on Apple
# Silicon). We only invoke uv at container runtime, where the Fargate
# host runs the native architecture and uv works normally.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Boot venv used by entrypoint.sh before the per-job venv exists. Built
# with the base image's own pip (not uv) so the build step is portable
# across cross-arch Docker builds. Installs ds-platform-utils from the
# local repo copy so ``remote_step.*`` / ``metaflow_extensions.remote_step.*``
# are importable for parsing spec.json, downloading the payload, and
# building the per-job venv.
RUN python3 -m venv /venv-runner
COPY pyproject.toml /ds-platform-utils/pyproject.toml
COPY README.md /ds-platform-utils/README.md
COPY src /ds-platform-utils/src
RUN /venv-runner/bin/pip install --no-cache-dir --disable-pip-version-check \
      boto3 \
      /ds-platform-utils

COPY container/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
