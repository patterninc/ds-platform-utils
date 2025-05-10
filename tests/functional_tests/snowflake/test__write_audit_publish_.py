import pytest

from ds_platform_utils._snowflake.write_audit_publish import (
    substitute_map_into_string,
    write_audit_publish,
)


def test_write_audit_publish_basic():
    """Test basic write-audit-publish flow with audits."""
    table_name = "test_table"
    query = """
    CREATE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
    SELECT * FROM source_table;
    """
    audits = [
        "SELECT COUNT(*) > 0 as has_rows FROM PATTERN_DB.{{schema}}.{{table_name}}",
    ]

    # Collect all operations
    operations = list(
        write_audit_publish(table_name=table_name, query=query, audits=audits, is_test=True, cursor=None)
    )

    # Should have 4 operations: clone, write, audit, publish
    assert len(operations) == 4

    # Verify operation types
    assert operations[0].operation_type == "clone to branch"
    assert operations[1].operation_type == "write"
    assert operations[2].operation_type == "audit"
    assert operations[3].operation_type == "publish"

    # Verify schemas
    assert operations[0].schema == "DATA_SCIENCE_STAGE"
    assert operations[1].schema == "DATA_SCIENCE_STAGE"
    assert operations[2].schema == "DATA_SCIENCE_STAGE"
    assert operations[3].schema == "DATA_SCIENCE_STAGE"


def test_write_audit_publish_no_audits():
    """Test that with no audits, we just get a single write operation."""
    table_name = "test_table"
    query = """
    CREATE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
    SELECT * FROM source_table;
    """

    operations = list(write_audit_publish(table_name=table_name, query=query, audits=None, cursor=None))

    assert len(operations) == 1
    assert operations[0].operation_type == "write"
    assert operations[0].schema == "DATA_SCIENCE_STAGE"

    # Use the substitute_map_into_string function to render the query for comparison
    expected_query = substitute_map_into_string(
        query, {"schema": "DATA_SCIENCE_STAGE", "table_name": table_name}
    ).strip()
    assert operations[0].query == expected_query


def test_write_audit_publish_production():
    """Test write-audit-publish to production schema."""
    table_name = "test_table"
    query = """
    CREATE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
    SELECT * FROM source_table;
    """
    audits = [
        "SELECT COUNT(*) > 0 as has_rows FROM PATTERN_DB.{{schema}}.{{table_name}}",
    ]

    operations = list(
        write_audit_publish(
            table_name=table_name, query=query, audits=audits, is_production=True, is_test=True, cursor=None
        )
    )

    # Verify staging writes happen in stage schema
    assert operations[0].schema == "DATA_SCIENCE_STAGE"
    assert operations[1].schema == "DATA_SCIENCE_STAGE"
    assert operations[2].schema == "DATA_SCIENCE_STAGE"
    # But final publish goes to prod schema
    assert operations[3].schema == "DATA_SCIENCE"


def test__write_audit_publish__invalid_query():
    """Test that invalid queries raise appropriate errors."""
    table_name = "test_table"
    query = "SELECT * FROM source_table"  # Missing {{schema}}.{{table_name}}

    with pytest.raises(ValueError, match="must use the literal string"):
        list(write_audit_publish(table_name=table_name, query=query, audits=None, cursor=None))


def test__write_audit_publish__invalid_audit():
    """Test that invalid audit queries raise appropriate errors."""
    table_name = "test_table"
    query = """
    CREATE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
    SELECT * FROM source_table;
    """
    audits = [
        "SELECT COUNT(*) FROM wrong_table"  # Missing {{schema}}.{{table_name}}
    ]

    with pytest.raises(ValueError, match="must use the literal string"):
        list(write_audit_publish(table_name=table_name, query=query, audits=audits, cursor=None))


def test__write_audit_publish__invalid_ctx_schema():
    """Test that context containing 'schema' raises error."""
    table_name = "test_table"
    query = """
    CREATE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
    SELECT * FROM source_table;
    """
    invalid_ctx = {"schema": "INVALID_SCHEMA"}

    with pytest.raises(ValueError, match="Context must not contain 'schema' key"):
        list(write_audit_publish(table_name=table_name, query=query, audits=None, cursor=None, ctx=invalid_ctx))


def test__write_audit_publish__invalid_ctx_table_name():
    """Test that context containing 'table_name' raises error."""
    table_name = "test_table"
    query = """
    CREATE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
    SELECT * FROM source_table;
    """
    invalid_ctx = {"table_name": "INVALID_TABLE"}

    with pytest.raises(ValueError, match="Context must not contain 'table_name' key"):
        list(write_audit_publish(table_name=table_name, query=query, audits=None, cursor=None, ctx=invalid_ctx))


def test_write_audit_publish_with_ctx():
    """Test write-audit-publish with context substitution."""
    table_name = "test_table"
    query = """
    CREATE TABLE PATTERN_DB.{{schema}}.{{table_name}} AS
    SELECT * FROM source_table
    WHERE date = '{{date}}' AND region = '{{region}}';
    """
    audits = [
        """
        SELECT
            COUNT(*) > 0 as has_rows,
            region = '{{region}}' as correct_region
        FROM PATTERN_DB.{{schema}}.{{table_name}}
        WHERE date = '{{date}}'
        """
    ]
    ctx = {"date": "2024-01-01", "region": "US"}

    operations = list(
        write_audit_publish(table_name=table_name, query=query, audits=audits, is_test=True, cursor=None, ctx=ctx)
    )

    assert len(operations) == 4

    # Verify context was substituted in write operation
    assert "date = '2024-01-01'" in operations[1].query
    assert "region = 'US'" in operations[1].query

    # Verify context was substituted in audit operation
    assert "date = '2024-01-01'" in operations[2].query
    assert "region = 'US'" in operations[2].query
