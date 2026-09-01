"""Reaching Snowflake *data* from a container running on AWS compute.

Snowflake is not a compute option here -- that was evaluated and dropped. It is still where the
data lives, so a step body that has been moved to SageMaker or ECS must be able to read and write
it exactly as it would from an EKS pod, through the same ``ds_platform_utils`` calls.

The obstacle is identity. An EKS step authenticates through the ``snowflake-default`` Outerbounds
integration; an AWS job container has no Snowflake identity at all. Rather than mint a service user
and a key pair, the submitting step asks that same integration for a short-lived OAuth token, puts
it in a Secrets Manager secret named for the job, and passes only the secret's *name* to the
container. The handle deletes the secret when the job ends, success or failure.

**The token exists in AWS for the life of the job.** It is narrow (minutes), IAM-scoped to the
execution role, audited by CloudTrail, and force-deleted afterwards -- but it is not nothing.
`Workload identity federation <https://docs.snowflake.com/en/user-guide/workload-identity-federation>`_
removes it entirely by letting the job's IAM role authenticate to Snowflake directly, and is the
production answer once a Snowflake admin can map a service user to that role.

Also worth weighing: this pulls Snowflake data *out* to AWS. For large reads, unloading to S3 and
reading that from the job is usually better than holding a connection open across the boundary.

Nothing here contacts Snowflake at import time, so flow modules stay importable without Snowflake
installed or authenticated.
"""

from __future__ import annotations

import os
import sys

DEFAULT_DATABASE = "PATTERN_DB"
DEFAULT_SCHEMA = "DATA_SCIENCE_STAGE"

# The repo is public, so no credentials are involved in installing it.
DS_PLATFORM_UTILS_REQUIREMENT = "git+https://github.com/patterninc/ds-platform-utils.git@main"

# Set by the backend to the name of a short-lived Secrets Manager secret holding the Snowflake
# OAuth connection parameters.
SNOWFLAKE_SECRET_ENV_VAR = "REMOTE_STEP_SNOWFLAKE_SECRET"


class _NoOpCard:
    """Absorbs ``current.card`` writes, which cannot reach the Metaflow task from a container."""

    def append(self, *_args, **_kwargs) -> None:
        """Swallow a card append."""

    def refresh(self, *_args, **_kwargs) -> None:
        """Swallow a card refresh."""

    def __getitem__(self, _key):
        """Support ``current.card[...]`` chains."""
        return self


class CurrentStandIn:
    """Stands in for Metaflow's ``current`` inside a container.

    Reads come from the snapshot taken on the Metaflow side, so ``is_production`` stays truthful --
    that is what selects the prod vs dev schema. ``card`` is a sink. Anything not captured returns
    None rather than raising, since a missing context field should not break a library call.
    """

    def __init__(self, snapshot) -> None:
        object.__setattr__(self, "_snapshot", snapshot)
        object.__setattr__(self, "card", _NoOpCard())

    def __getattr__(self, name: str):
        """Read a field from the snapshot, or None if it was not captured."""
        return getattr(object.__getattribute__(self, "_snapshot"), name, None)

    def __bool__(self) -> bool:
        """``if current:`` guards in ds_platform_utils must see a live context."""
        return True


class _NonClosingConnection:
    """A ``SnowflakeConnection`` that ignores ``close()``.

    ``ds_platform_utils`` closes the connection when it finishes with it, which is right when it
    created that connection itself. Here it is handed the container's *shared* connection, so an
    honest ``close()`` would end the session for everything that runs afterwards -- in practice,
    ``publish_pandas`` succeeding and then ``publish`` failing with "Connection is closed". The job
    owns the connection lifecycle, so closing is the one call we intercept.
    """

    def __init__(self, connection) -> None:
        object.__setattr__(self, "_connection", connection)

    def __getattr__(self, name: str):
        """Delegate everything else to the real connection."""
        return getattr(object.__getattribute__(self, "_connection"), name)

    def __enter__(self):
        """Support ``with connection:`` without handing over ownership."""
        return self

    def __exit__(self, *_exc_info) -> None:
        """Leave the connection open on block exit."""

    def close(self) -> None:
        """Ignore. The container's connection outlives any single library call."""


def container_connection():
    """Return a Snowflake connection built from the credentials staged for this job.

    :return: The connection, wrapped so a library cannot close it out from under us.
    :raises RuntimeError: If no secret was staged, which means the step was submitted without
        ``with_snowflake=True`` and the body was not expected to touch Snowflake.
    """
    secret_name = os.environ.get(SNOWFLAKE_SECRET_ENV_VAR)
    if not secret_name:
        raise RuntimeError(
            f"No Snowflake credentials in this container: {SNOWFLAKE_SECRET_ENV_VAR} is unset. "
            f"Submit the step with a backend built with with_snowflake=True."
        )
    return _NonClosingConnection(_connect_from_secret(secret_name))


def _connect_from_secret(secret_name: str):
    """Build a Snowflake connection from OAuth parameters held in Secrets Manager.

    The secret holds what ``get_oauth_connection_params`` produced on the submitting side: a
    short-lived token plus the account and user it was minted for. It is deleted once the job
    finishes, so this is the only place it is ever read.

    :param secret_name: Name of the secret.
    :return: A live ``SnowflakeConnection``.
    """
    import json

    import boto3
    import snowflake.connector

    secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)
    return snowflake.connector.connect(**json.loads(secret["SecretString"]))


def bootstrap_ds_platform_utils(current_snapshot) -> dict:
    """Patch imported ``ds_platform_utils`` modules so their Snowflake calls work in a container.

    The library reaches Snowflake through ``get_snowflake_connection`` and reports progress through
    ``current`` -- every function routes through those two names, so replacing them is enough to
    make the whole library work, rather than shimming functions one at a time.

    Patching a library's namespace does depend on it continuing to route that way, so the counts
    are returned for logging: a version that stops doing so shows up as a changed count instead of
    a silent behaviour change. Safe to call when the library is absent, or when no credentials were
    staged -- a body that never touches Snowflake should not fail because of this.

    :param current_snapshot: The picklable ``current`` snapshot taken on the Metaflow side.
    :return: Whether the library was importable, and how many modules were patched.
    """
    try:
        import ds_platform_utils  # noqa: F401
    except ImportError:
        return {"installed": False, "patched_current": 0, "patched_connection": 0}

    # Import the submodules first so their `from metaflow import current` bindings exist and can be
    # replaced; patching only the top-level package would miss them.
    for module_name in ("ds_platform_utils.metaflow.pandas", "ds_platform_utils.metaflow.snowflake_connection"):
        try:
            __import__(module_name)
        except ImportError:
            continue

    stand_in = CurrentStandIn(current_snapshot)
    try:
        connection = container_connection()
    except Exception as exc:
        # No staged credentials means this body was not meant to reach Snowflake. Patching
        # `current` alone would leave the library half-wired, so do nothing and say so.
        return {"installed": True, "patched_current": 0, "patched_connection": 0, "error": str(exc)}

    patched_current = 0
    patched_connection = 0

    for name, module in list(sys.modules.items()):
        if not name.startswith("ds_platform_utils") or module is None:
            continue
        if hasattr(module, "current"):
            module.current = stand_in
            patched_current += 1
        if hasattr(module, "get_snowflake_connection"):
            module.get_snowflake_connection = lambda *_args, **_kwargs: connection
            patched_connection += 1

    return {"installed": True, "patched_current": patched_current, "patched_connection": patched_connection}


IDENTITY_QUERY = """
SELECT
    CURRENT_ROLE()      AS role,
    CURRENT_USER()      AS snowflake_user,
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_DATABASE()  AS database,
    CURRENT_SCHEMA()    AS schema,
    CURRENT_ACCOUNT()   AS account
"""


def snowflake_identity(label: str = "") -> dict:
    """Report the Snowflake identity this code is executing as.

    Works on either side of the boundary: in a Metaflow step this resolves through the
    ``snowflake-default`` integration; inside a job container it resolves through the staged token,
    which was minted from that same integration.

    Comparing the two is the check that matters before anything writes from a container: the role
    decides grants and object ownership, so a container publishing as a different role than the
    flow expects is a problem worth catching early.

    :param label: Where this was called from, e.g. ``"metaflow step"``.
    :return: Role, user, warehouse, database, schema and account.
    """
    from ds_platform_utils.metaflow import query_pandas_from_snowflake

    row = query_pandas_from_snowflake(query=IDENTITY_QUERY).iloc[0].to_dict()
    return {"label": label, **{key: (None if value is None else str(value)) for key, value in row.items()}}
