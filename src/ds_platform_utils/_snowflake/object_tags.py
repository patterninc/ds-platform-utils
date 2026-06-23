"""Build and apply Snowflake object tags for table ownership / governance.

Implements the tag schema from the "Snowflake table ownership via object tags" RFC.
Tags are applied only to production tables, so both the tag *definitions* and the
*tables* live in ``PATTERN_DB.DATA_SCIENCE``.

The tag *definitions* must be created once by a Snowflake admin (see the RFC's
``CREATE TAG`` setup). Until they exist, :func:`apply_table_tags` warns and leaves the
(already successful) table write untouched -- tagging must never break a publish.
"""

import re
from typing import TYPE_CHECKING, Dict, Optional

from ds_platform_utils._snowflake.run_query import _execute_sql
from ds_platform_utils.metaflow._consts import PROD_SCHEMA
from ds_platform_utils.sql_utils import get_select_dev_query_tags

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection

DATABASE = "PATTERN_DB"

# A Snowflake unquoted identifier: starts with a letter/underscore, then letters/digits/underscores.
# Identifiers (table name, schema, tag names) are interpolated directly into the SET TAG SQL, so we
# reject anything else to avoid malformed SQL or statement injection. (Tag *values* are safely
# single-quoted + escaped via _quote and are not subject to this check.)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# RFC allowed-value lists for the constrained tags.
TABLE_STATUS_ALLOWED = {"active", "development", "testing", "deprecated", "archived", "retired"}
TABLE_SLA_ALLOWED = {
    "streaming",
    "realtime",
    "hourly",
    "daily",
    "weekly",
    "monthly",
    "quarterly",
    "ad_hoc",
    "on_demand",
}
DEFAULT_TABLE_STATUS = "active"

# Value used by get_select_dev_query_tags when a derived field can't be resolved.
UNKNOWN_VALUE = "unknown"


def _owner_from_domain(domain: str) -> str:
    """Map a domain to its owning team alias, e.g. ``advertising`` -> ``ds-advertising-team``."""
    return f"ds-{domain}-team"


# All seven RFC tag names.
TAG_OWNER = "TABLE_OWNER"
TAG_TEAM = "TABLE_TEAM"
TAG_DOMAIN = "TABLE_DOMAIN"
TAG_PROJECT = "TABLE_PROJECT"
TAG_STATUS = "TABLE_STATUS"
TAG_SLA = "TABLE_SLA"
TAG_CONTACT = "TABLE_CONTACT"

# Maps accepted override keys (case-insensitive, with or without the ``TABLE_`` prefix)
# to the canonical tag name.
_OVERRIDE_ALIASES = {
    "owner": TAG_OWNER,
    "team": TAG_TEAM,
    "domain": TAG_DOMAIN,
    "project": TAG_PROJECT,
    "status": TAG_STATUS,
    "sla": TAG_SLA,
    "contact": TAG_CONTACT,
}


def _normalize_overrides(tags_override: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Normalize caller override keys to canonical tag names.

    Accepts e.g. ``owner``, ``OWNER`` or ``TABLE_OWNER`` -> ``TABLE_OWNER``.

    :param tags_override: Raw override dict supplied by the caller.
    :return: Override dict keyed by canonical tag name.
    :raises ValueError: If an override key does not map to a known tag.
    """
    normalized: Dict[str, str] = {}
    for key, value in (tags_override or {}).items():
        canonical = _OVERRIDE_ALIASES.get(key.strip().lower().removeprefix("table_"))
        if canonical is None:
            raise ValueError(
                f"Unknown tag override key {key!r}. Allowed keys: {sorted(_OVERRIDE_ALIASES)} "
                f"(optionally prefixed with 'TABLE_')."
            )
        normalized[canonical] = value
    return normalized


def build_table_tags(
    tags_override: Optional[Dict[str, str]] = None,
    current_obj: Optional[object] = None,
) -> Dict[str, str]:
    """Build the final ``{TAG_NAME: value}`` dict to apply to a published table.

    TEAM / DOMAIN / PROJECT are derived from the Metaflow run context (reusing
    :func:`get_select_dev_query_tags`); STATUS defaults to ``active``. OWNER is resolved
    by priority: (1) an explicit ``owner`` override, else (2) the owning-team alias derived
    from the (possibly overridden) domain -- ``ds-<domain>-team`` -- when the domain is
    known, else (3) ``unknown``. (We deliberately don't use ``current.username`` for OWNER:
    on deployed/argo runs it resolves to a service identity, not a person.) SLA and CONTACT
    are only included when supplied via ``tags_override``.

    :param tags_override: Optional overrides, keyed by ``owner``/``TABLE_OWNER``/etc.
    :param current_obj: Optional Metaflow ``current`` stand-in (for testing).
    :return: Mapping of canonical tag name to value, ready to apply.
    :raises ValueError: If STATUS or SLA is not in its allowed-value list, or an
        override key is unknown.
    """
    overrides = _normalize_overrides(tags_override)
    derived = get_select_dev_query_tags(current_obj=current_obj)

    tags: Dict[str, str] = {
        TAG_TEAM: derived["team"],
        TAG_DOMAIN: derived["domain"],
        TAG_PROJECT: derived["workload_id"],
        TAG_STATUS: DEFAULT_TABLE_STATUS,
    }
    # SLA / CONTACT are only set when explicitly provided.
    tags.update(overrides)

    # Resolve OWNER: explicit override wins; else derive a team alias from the (final) domain
    # when it's known; else fall back to "unknown".
    if TAG_OWNER not in overrides:
        domain = tags.get(TAG_DOMAIN)
        if domain and domain != UNKNOWN_VALUE:
            tags[TAG_OWNER] = _owner_from_domain(domain)
        else:
            tags[TAG_OWNER] = UNKNOWN_VALUE

    status = tags[TAG_STATUS]
    if status not in TABLE_STATUS_ALLOWED:
        raise ValueError(f"TABLE_STATUS must be one of {sorted(TABLE_STATUS_ALLOWED)}, got {status!r}.")

    sla = tags.get(TAG_SLA)
    if sla is not None and sla not in TABLE_SLA_ALLOWED:
        raise ValueError(f"TABLE_SLA must be one of {sorted(TABLE_SLA_ALLOWED)}, got {sla!r}.")

    # Drop any tags whose value is None/empty so we never emit ``= ''``.
    return {name: str(value) for name, value in tags.items() if value is not None and str(value) != ""}


def _quote(value: str) -> str:
    """Escape a tag value for a single-quoted SQL literal (double embedded quotes)."""
    return value.replace("'", "''")


def _validate_identifier(value: str, kind: str) -> None:
    """Reject anything that isn't a plain unquoted SQL identifier.

    Identifiers are interpolated unquoted into the ``SET TAG`` SQL, so a value containing
    e.g. ``;`` or whitespace could produce invalid SQL or statement injection.

    :param value: Identifier to check (table name, schema, or tag name).
    :param kind: Human-readable label used in the error message.
    :raises ValueError: If ``value`` is not a valid unquoted identifier.
    """
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {kind} {value!r}; expected an unquoted identifier (letters/numbers/underscore).")


def build_set_tag_sql(table_name: str, tags: Dict[str, str], schema: str = PROD_SCHEMA) -> str:
    """Build a single ``ALTER TABLE ... SET TAG`` statement.

    Only production tables are tagged, so the table and its tag *definitions* both live in
    ``schema`` (``DATA_SCIENCE``).

    :param table_name: Table to tag (upper-cased to match Snowflake's stored identifier).
    :param tags: Mapping of tag name to value (e.g. from :func:`build_table_tags`).
    :param schema: Schema holding both the table and the tag definitions.
    :return: The ``ALTER TABLE`` SQL string.
    :raises ValueError: If ``tags`` is empty, or any identifier (table/schema/tag name) is invalid.
    """
    if not tags:
        raise ValueError("No tags to apply.")
    table = table_name.upper()
    _validate_identifier(table, "table_name")
    _validate_identifier(schema, "schema")
    for name in tags:
        _validate_identifier(name, "tag name")
    assignments = ",\n        ".join(f"{DATABASE}.{schema}.{name} = '{_quote(value)}'" for name, value in tags.items())
    return f"ALTER TABLE {DATABASE}.{schema}.{table}\n    SET TAG\n        {assignments};"


def apply_table_tags(
    conn: "SnowflakeConnection",
    table_name: str,
    tags: Dict[str, str],
    schema: str = PROD_SCHEMA,
) -> None:
    """Apply object tags to a published table, warning (never raising) on failure.

    A failure here most commonly means the tag definitions have not yet been created in
    ``schema`` by an admin (see the RFC ``CREATE TAG`` setup), or the publishing role lacks
    ``APPLY`` on the tags. Because the table write has already succeeded by this point, we
    log a clear warning and return rather than breaking the publish.

    :param conn: Open Snowflake connection.
    :param table_name: Table to tag.
    :param tags: Mapping of tag name to value.
    :param schema: Schema holding both the table and its tag definitions (``DATA_SCIENCE``).
    """
    if not tags:
        return
    target = f"{DATABASE}.{schema}.{table_name.upper()}"
    try:
        # Built inside the try so identifier-validation errors warn-and-skip rather than break publish.
        sql = build_set_tag_sql(table_name=table_name, tags=tags, schema=schema)
        _execute_sql(conn, sql)
        conn.commit()
        print(f"Applied ownership tags to {target}: {sorted(tags)}")
    except Exception as exc:  # noqa: BLE001 -- tagging must never break a successful publish
        print(
            f"Warning: failed to apply ownership tags to {target} ({exc}). The table was published "
            f"successfully; tags were skipped. This usually means the tag definitions have not been "
            f"created yet by a Snowflake admin, or the publishing role lacks APPLY on the tags "
            f"(see the table-ownership RFC)."
        )
