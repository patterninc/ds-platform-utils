from src.ds_platform_utils.metaflow.write_audit_publish import add_comment_to_each_sql_statement


def test_add_comment_to_each_sql_statement():
    """Test adding comments to each SQL statement."""
    input_sql = "select * from foo; select * from bar; select 'abc;def' as col;"
    comment = "/* {'app':'test'} */"

    expected_output = (
        "select * from foo /* {'app':'test'} */;\n\n"
        "select * from bar /* {'app':'test'} */;\n\n"
        "select 'abc;def' as col /* {'app':'test'} */;\n"
    )

    output = add_comment_to_each_sql_statement(input_sql, comment)

    assert output.strip() == expected_output.strip()
