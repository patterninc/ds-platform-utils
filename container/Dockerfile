FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    UV_HTTP_TIMEOUT=120 \
    PATH="/venv/bin:/root/.local/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tini git \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# uv — used by both the boot venv here and the per-job venv the entrypoint builds.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Boot venv used by entrypoint.sh before the per-job venv exists. Installs
# ds-platform-utils from the local repo copy (no network / auth needed at
# build time) so `remote_step.*` and `metaflow_extensions.remote_step.*`
# are importable for parsing spec.json, downloading the payload, and
# building the per-job venv.
RUN /root/.local/bin/uv venv /venv-runner --python 3.10
COPY pyproject.toml /ds-platform-utils/pyproject.toml
COPY README.md /ds-platform-utils/README.md
COPY src /ds-platform-utils/src
RUN /root/.local/bin/uv pip install --python /venv-runner/bin/python \
      boto3 \
      /ds-platform-utils

COPY container/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
