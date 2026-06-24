import pytest

from ds_platform_utils._snowflake import object_tags
from ds_platform_utils._snowflake.object_tags import (
    apply_table_tags,
    build_set_tag_sql,
    build_table_tags,
)


class FakeCurrent:
    """Stand-in for ``metaflow.current`` used to drive tag derivation in tests."""

    tags = ["ds.domain:recommendations", "ds.project:two_tower_v2"]
    flow_name = "MyFlow"
    project_name = "recsys-proj"
    step_name = "end"
    run_id = "123"
    username = "john_doe"
    namespace = "user:john"
    is_production = True


def test_build_table_tags_derives_all_mappings():
    """Context-derived tags + default STATUS present; OWNER = team alias from domain; SLA/CONTACT omitted."""
    tags = build_table_tags(current_obj=FakeCurrent())

    # OWNER is derived from the domain (not current.username) when no override is given.
    assert tags["TABLE_OWNER"] == "ds-recommendations-team"
    assert tags["TABLE_TEAM"] == "data-science"
    assert tags["TABLE_DOMAIN"] == "recommendations"
    assert tags["TABLE_PROJECT"] == "two_tower_v2"
    assert tags["TABLE_STATUS"] == "active"
    assert "TABLE_SLA" not in tags
    assert "TABLE_CONTACT" not in tags


def test_build_table_tags_owner_falls_back_to_unknown_without_domain():
    """When the domain can't be resolved, OWNER falls back to 'unknown'."""

    class NoDomainCurrent(FakeCurrent):
        tags = []  # no ds.domain / ds.project -> domain resolves to "unknown"

    tags = build_table_tags(current_obj=NoDomainCurrent())

    assert tags["TABLE_DOMAIN"] == "unknown"
    assert tags["TABLE_OWNER"] == "unknown"


def test_build_table_tags_owner_follows_overridden_domain():
    """Overriding the domain (without overriding owner) re-derives the owner team alias."""
    tags = build_table_tags(tags_override={"domain": "advertising"}, current_obj=FakeCurrent())

    assert tags["TABLE_DOMAIN"] == "advertising"
    assert tags["TABLE_OWNER"] == "ds-advertising-team"


def test_build_table_tags_explicit_owner_beats_domain_derivation():
    """An explicit owner override wins over the domain-derived team alias."""
    tags = build_table_tags(tags_override={"owner": "jane"}, current_obj=FakeCurrent())

    assert tags["TABLE_OWNER"] == "jane"


def test_build_table_tags_ds_owner_flow_tag_beats_domain_derivation():
    """A `ds.owner` flow tag is used (priority 2) over the domain-derived team alias."""

    class DsOwnerCurrent(FakeCurrent):
        tags = ["ds.domain:recommendations", "ds.project:two_tower_v2", "ds.owner:john_doe"]

    tags = build_table_tags(current_obj=DsOwnerCurrent())

    assert tags["TABLE_OWNER"] == "john_doe"


def test_build_table_tags_explicit_owner_beats_ds_owner_flow_tag():
    """An explicit override (priority 1) wins over the `ds.owner` flow tag (priority 2)."""

    class DsOwnerCurrent(FakeCurrent):
        tags = ["ds.domain:recommendations", "ds.project:two_tower_v2", "ds.owner:john_doe"]

    tags = build_table_tags(tags_override={"owner": "jane"}, current_obj=DsOwnerCurrent())

    assert tags["TABLE_OWNER"] == "jane"


def test_build_table_tags_overrides_win():
    """Overrides (incl. alias + cased keys) replace derived values and add SLA/CONTACT."""
    tags = build_table_tags(
        tags_override={"owner": "jane", "SLA": "daily", "TABLE_CONTACT": "#ds-recsys"},
        current_obj=FakeCurrent(),
    )

    assert tags["TABLE_OWNER"] == "jane"
    assert tags["TABLE_SLA"] == "daily"
    assert tags["TABLE_CONTACT"] == "#ds-recsys"
    # Non-overridden derived values still present.
    assert tags["TABLE_DOMAIN"] == "recommendations"


@pytest.mark.parametrize("override", [{"status": "bogus"}, {"sla": "every_minute"}])
def test_build_table_tags_invalid_constrained_value_raises(override):
    """Invalid STATUS or SLA values raise ValueError (caller error)."""
    with pytest.raises(ValueError):
        build_table_tags(tags_override=override, current_obj=FakeCurrent())


def test_build_table_tags_unknown_key_raises():
    """An unrecognized override key raises ValueError."""
    with pytest.raises(ValueError, match="Unknown tag override key"):
        build_table_tags(tags_override={"foo": "bar"}, current_obj=FakeCurrent())


def test_build_set_tag_sql_format_and_escaping():
    """SQL targets the table schema for the object, upper-cases the table, escapes quotes."""
    sql = build_set_tag_sql(table_name="my_table", tags={"TABLE_OWNER": "o'brien"}, schema="DATA_SCIENCE")

    assert "ALTER TABLE PATTERN_DB.DATA_SCIENCE.MY_TABLE" in sql
    assert "PATTERN_DB.DATA_SCIENCE.TABLE_OWNER = 'o''brien'" in sql
    assert sql.strip().endswith(";")


def test_build_set_tag_sql_defaults_to_data_science():
    """Schema defaults to DATA_SCIENCE (only production tables are tagged)."""
    sql = build_set_tag_sql(table_name="my_table", tags={"TABLE_OWNER": "ds-advertising-team"})

    assert "ALTER TABLE PATTERN_DB.DATA_SCIENCE.MY_TABLE" in sql
    assert "PATTERN_DB.DATA_SCIENCE.TABLE_OWNER = 'ds-advertising-team'" in sql


def test_build_set_tag_sql_empty_raises():
    with pytest.raises(ValueError, match="No tags to apply"):
        build_set_tag_sql(table_name="t", tags={}, schema="DATA_SCIENCE")


def test_build_set_tag_sql_multiple_tags_joined():
    """Multiple tags are comma-joined under a single SET TAG / single trailing semicolon."""
    sql = build_set_tag_sql(
        table_name="my_table",
        tags={"TABLE_OWNER": "john_doe", "TABLE_TEAM": "data-science", "TABLE_STATUS": "active"},
        schema="DATA_SCIENCE",
    )

    assert sql.count("SET TAG") == 1
    assert "PATTERN_DB.DATA_SCIENCE.TABLE_OWNER = 'john_doe'," in sql
    assert "PATTERN_DB.DATA_SCIENCE.TABLE_TEAM = 'data-science'," in sql
    assert "PATTERN_DB.DATA_SCIENCE.TABLE_STATUS = 'active'" in sql
    # Exactly one statement terminator, on the last assignment only.
    assert sql.count(";") == 1
    assert sql.count("=") == 3


def test_build_table_tags_drops_empty_override_value():
    """An empty-string override is dropped rather than emitted as TAG = ''."""
    tags = build_table_tags(tags_override={"contact": ""}, current_obj=FakeCurrent())

    assert "TABLE_CONTACT" not in tags


@pytest.mark.parametrize("bad_table", ["bad; DROP TABLE x", "has space", "1leading_digit", "", "a-b"])
def test_build_set_tag_sql_rejects_invalid_table_name(bad_table):
    """Non-identifier table names are rejected before reaching SQL."""
    with pytest.raises(ValueError, match="Invalid table_name"):
        build_set_tag_sql(table_name=bad_table, tags={"TABLE_OWNER": "john_doe"}, schema="DATA_SCIENCE")


def test_build_set_tag_sql_rejects_invalid_tag_name():
    """Non-identifier tag names are rejected."""
    with pytest.raises(ValueError, match="Invalid tag name"):
        build_set_tag_sql(table_name="my_table", tags={"TABLE_OWNER; DROP": "x"}, schema="DATA_SCIENCE")


def test_build_set_tag_sql_rejects_invalid_schema():
    """Non-identifier schema is rejected."""
    with pytest.raises(ValueError, match="Invalid schema"):
        build_set_tag_sql(table_name="my_table", tags={"TABLE_OWNER": "x"}, schema="DATA_SCIENCE; DROP")


class FakeConn:
    def __init__(self):
        self.committed = False

    def commit(self):
        """Record that commit was called."""
        self.committed = True


def test_apply_table_tags_swallows_errors_and_warns(monkeypatch, capsys):
    """A failure applying tags must not raise and must not break the publish."""

    def _boom(*_args, **_kwargs):
        raise RuntimeError("tag 'TABLE_OWNER' does not exist")

    monkeypatch.setattr(object_tags, "_execute_sql", _boom)
    conn = FakeConn()

    apply_table_tags(conn=conn, table_name="my_table", tags={"TABLE_OWNER": "john_doe"}, schema="DATA_SCIENCE")

    assert conn.committed is False
    assert "Warning: failed to apply ownership tags" in capsys.readouterr().out


def test_apply_table_tags_invalid_identifier_warns_not_raises(monkeypatch, capsys):
    """An invalid identifier must warn-and-skip, not propagate out of apply_table_tags."""
    executed = False

    def _spy(*_args, **_kwargs):
        nonlocal executed
        executed = True

    monkeypatch.setattr(object_tags, "_execute_sql", _spy)
    conn = FakeConn()

    # A malformed table name would otherwise build invalid/injectable SQL.
    apply_table_tags(
        conn=conn, table_name="bad; DROP TABLE x", tags={"TABLE_OWNER": "john_doe"}, schema="DATA_SCIENCE"
    )

    assert executed is False  # never reached execution
    assert conn.committed is False
    assert "Warning: failed to apply ownership tags" in capsys.readouterr().out


def test_apply_table_tags_success_executes_and_commits(monkeypatch, capsys):
    """Happy path: the built SQL is executed against the conn and the change is committed."""
    captured = {}

    def _capture(conn, sql):
        captured["conn"] = conn
        captured["sql"] = sql

    monkeypatch.setattr(object_tags, "_execute_sql", _capture)
    conn = FakeConn()

    apply_table_tags(conn=conn, table_name="my_table", tags={"TABLE_OWNER": "john_doe"}, schema="DATA_SCIENCE")

    assert captured["conn"] is conn
    assert "ALTER TABLE PATTERN_DB.DATA_SCIENCE.MY_TABLE" in captured["sql"]
    assert "PATTERN_DB.DATA_SCIENCE.TABLE_OWNER = 'john_doe'" in captured["sql"]
    assert conn.committed is True
    assert "Applied ownership tags" in capsys.readouterr().out


def test_apply_table_tags_noop_on_empty(monkeypatch):
    """No tags -> no execution, no commit."""
    called = False

    def _spy(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(object_tags, "_execute_sql", _spy)
    conn = FakeConn()

    apply_table_tags(conn=conn, table_name="my_table", tags={}, schema="DATA_SCIENCE")

    assert called is False
    assert conn.committed is False
