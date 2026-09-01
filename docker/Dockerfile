# Slim runtime for @remote_step's SageMaker backend.
#
# Why this exists: the only stock SageMaker image we found with Python 3.11 is a ~5GB PyTorch DLC,
# and pulling it dominated job startup (~169s total for a 0.8s body). Python minor version is not
# negotiable -- the step body is cloudpickled bytecode and the decorator refuses a mismatch -- so
# the fix is a small image that happens to have 3.11 rather than a large one that does.
#
# This image is a cache, not the dependency list. A decorated step inherits its flow's
# @pypi_base packages and installs them at job start, so what a body can import is decided by the
# flow -- one list, and the same imports available wherever the step runs. Baking the common ones
# in only makes that install a few seconds instead of a few minutes.
#
# Keep this in step with IMAGE_PACKAGES in helpers/compute_backends.py, which is how the backend
# knows what it can skip. Versions are pinned to what flows declare so the skip is a genuine
# no-op rather than a downgrade on every job.
FROM --platform=linux/amd64 python:3.11-slim

# Which ref of this library the image carries. Must match what the submitting project installs --
# the step body is handed to execute_remote_step from the installed package, so a mismatch is a
# version skew between the two halves of the same decorator.
ARG DS_PLATFORM_UTILS_REF=main

# cloudpickle must match the submitting side exactly: its pickle format moves between versions, and
# a mismatch fails with "Can't get attribute '_function_setstate'". The backend also pins it at
# submit time; this just means the pin is usually already satisfied and pip does nothing.
# pandas and pyarrow are here because the decorator encodes DataFrames as parquet across the wire.
# ds-platform-utils is baked in rather than pip-installed at job start: it pulls Metaflow and
# friends, which is slow to resolve every run, and installing from git would need a git binary in
# the image. git is added and removed in the same layer so it does not ship.
#
# It also carries the decorator itself, so the container imports execute_remote_step from the
# installed package rather than from the payload. Build the image from the same ref a project
# installs, or the two halves can drift: DS_PLATFORM_UTILS_REF below is what to change.
#
# boto3 is here for the ephemeral-credential path: the container reads a short-lived Snowflake
# OAuth token from Secrets Manager, which the submitting step wrote and deletes afterwards.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && pip install --no-cache-dir \
        cloudpickle==3.1.2 \
        pandas==2.2.3 \
        pyarrow \
        numpy \
        scikit-learn \
        xgboost \
        boto3 \
        snowflake-connector-python==3.18.0 \
        "ds-platform-utils @ git+https://github.com/patterninc/ds-platform-utils.git@${DS_PLATFORM_UTILS_REF}" \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

# SageMaker passes the entrypoint explicitly (AlgorithmSpecification.ContainerEntrypoint for
# training, AppSpecification.ContainerEntrypoint for processing), so no CMD is needed here.
WORKDIR /opt/ml
