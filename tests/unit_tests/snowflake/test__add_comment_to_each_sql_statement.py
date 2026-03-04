from src.ds_platform_utils.sql_utils import add_comment_to_each_sql_statement


def test_add_comment_to_each_sql_statement():
    """Test adding comments to each SQL statement."""
    input_sql = "select * from foo; select * from bar; select 'abc;def' as col;"
    comment = "{'app':'test'}"

    expected_output = (
        "select * from foo\n/* {'app':'test'} */\n;"
        "\nselect * from bar\n/* {'app':'test'} */\n;"
        "\nselect 'abc;def' as col\n/* {'app':'test'} */\n;"
    )

    output = add_comment_to_each_sql_statement(input_sql, comment)

    assert output.strip() == expected_output.strip()
