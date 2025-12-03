"""Unit tests for shared utility functions."""

from unittest.mock import MagicMock, Mock

from ds_platform_utils._snowflake.shared import _execute_sql


class TestRunSql:
    """Test suite for _execute_sql utility function."""

    def test_returns_last_cursor_with_multiple_statements(self):
        """Test that _execute_sql returns the last cursor when multiple SQL statements are executed."""
        # Setup
        mock_conn = Mock()
        cursor1 = Mock()
        cursor2 = Mock()
        cursor3 = Mock()
        mock_conn.execute_string.return_value = [cursor1, cursor2, cursor3]

        sql = "SELECT 1; SELECT 2; SELECT 3;"

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is cursor3

    def test_returns_single_cursor_with_single_statement(self):
        """Test that _execute_sql returns the cursor when a single SQL statement is executed."""
        # Setup
        mock_conn = Mock()
        cursor = Mock()
        mock_conn.execute_string.return_value = [cursor]

        sql = "SELECT * FROM table;"

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is cursor

    def test_returns_none_when_no_statements_executed(self):
        """Test that _execute_sql returns None when no statements are executed."""
        # Setup
        mock_conn = Mock()
        mock_conn.execute_string.return_value = []

        sql = ""

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is None

    def test_handles_empty_sql_string(self):
        """Test that _execute_sql correctly handles empty SQL strings."""
        # Setup
        mock_conn = Mock()
        mock_conn.execute_string.return_value = []

        sql = ""

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is None

    def test_handles_whitespace_only_sql(self):
        """Test that _execute_sql correctly handles SQL strings with only whitespace."""
        # Setup
        mock_conn = Mock()
        mock_conn.execute_string.return_value = []

        sql = "   \n\t  "

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is None

    def test_handles_sql_with_comments_only(self):
        """Test that _execute_sql correctly handles SQL with only comments."""
        # Setup
        mock_conn = Mock()
        mock_conn.execute_string.return_value = []

        sql = "-- This is a comment\n/* This is another comment */"

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is None

    def test_preserves_cursor_iteration_order(self):
        """Test that _execute_sql iterates through all cursors in order."""
        # Setup
        mock_conn = Mock()
        cursors = [Mock(name=f"cursor{i}") for i in range(5)]
        mock_conn.execute_string.return_value = cursors

        sql = "SELECT 1; SELECT 2; SELECT 3; SELECT 4; SELECT 5;"

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is cursors[-1]

    def test_passes_connection_object_correctly(self):
        """Test that _execute_sql correctly uses the provided connection object."""
        # Setup
        mock_conn = MagicMock()
        cursor = Mock()
        mock_conn.execute_string.return_value = [cursor]

        sql = "SELECT * FROM table;"

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is cursor

    def test_handles_complex_multistatement_sql(self):
        """Test that _execute_sql handles complex multi-statement SQL with various statement types."""
        # Setup
        mock_conn = Mock()
        cursor1 = Mock()
        cursor2 = Mock()
        cursor3 = Mock()
        cursor4 = Mock()
        mock_conn.execute_string.return_value = [cursor1, cursor2, cursor3, cursor4]

        sql = """
        CREATE TABLE temp_table AS SELECT * FROM source;
        INSERT INTO target_table SELECT * FROM temp_table;
        UPDATE target_table SET status = 'processed';
        DROP TABLE temp_table;
        """

        # Execute
        result = _execute_sql(mock_conn, sql)

        # Verify
        mock_conn.execute_string.assert_called_once_with(sql)
        assert result is cursor4
