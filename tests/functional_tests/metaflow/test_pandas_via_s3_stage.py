"""Functional tests for Snowflake S3 stage operations.

These tests verify the S3-Snowflake bridge functionality for large-scale data operations.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from ds_platform_utils.metaflow.pandas_via_s3_stage import (
    DEV_S3_BUCKET,
    DEV_SNOWFLAKE_STAGE,
    PROD_S3_BUCKET,
    PROD_SNOWFLAKE_STAGE,
    _get_s3_config,
)


class TestS3Config:
    """Test S3 configuration selection based on environment."""

    def test_get_s3_config_dev(self):
        """Test that dev configuration is returned when is_production=False."""
        bucket, stage = _get_s3_config(is_production=False)
        assert bucket == DEV_S3_BUCKET
        assert stage == DEV_SNOWFLAKE_STAGE

    def test_get_s3_config_prod(self):
        """Test that prod configuration is returned when is_production=True."""
        bucket, stage = _get_s3_config(is_production=True)
        assert bucket == PROD_S3_BUCKET
        assert stage == PROD_SNOWFLAKE_STAGE


class TestQueryPandasFromSnowflakeViaS3Stage:
    """Test query_pandas_from_snowflake_via_s3_stage function."""

    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.get_snowflake_connection")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._execute_sql")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._get_df_from_s3_folder")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.current")
    def test_query_via_s3_stage_basic(self, mock_current, mock_get_df, mock_execute_sql, mock_get_conn):
        """Test basic query execution via S3 stage."""
        from ds_platform_utils.metaflow.pandas_via_s3_stage import (
            query_pandas_from_snowflake_via_s3_stage,
        )

        # Setup mocks
        mock_current.is_production = False
        mock_current.card = MagicMock()

        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        expected_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_get_df.return_value = expected_df

        # Execute
        query = "SELECT * FROM TEST_TABLE"
        result_df = query_pandas_from_snowflake_via_s3_stage(query=query)

        # Verify
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 3
        assert list(result_df.columns) == ["col1", "col2"]

        # Verify connection was closed
        mock_conn.close.assert_called_once()

        # Verify COPY INTO was executed
        assert mock_execute_sql.called


class TestPublishPandasViaS3Stage:
    """Test publish_pandas_via_s3_stage function."""

    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.get_snowflake_connection")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._execute_sql")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._get_metaflow_s3_client")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.current")
    @patch("os.makedirs")
    @patch("os.remove")
    @patch("os.rmdir")
    def test_publish_via_s3_stage_basic(
        self,
        mock_rmdir,
        mock_remove,
        mock_makedirs,
        mock_current,
        mock_s3_client,
        mock_execute_sql,
        mock_get_conn,
    ):
        """Test basic DataFrame publishing via S3 stage."""
        from ds_platform_utils.metaflow.pandas_via_s3_stage import (
            publish_pandas_via_s3_stage,
        )

        # Setup mocks
        mock_current.is_production = False
        mock_current.card = MagicMock()

        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_s3 = MagicMock()
        mock_s3_client.return_value.__enter__ = Mock(return_value=mock_s3)
        mock_s3_client.return_value.__exit__ = Mock(return_value=False)

        # Create test DataFrame
        df = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": ["a", "b", "c"],
            }
        )

        table_schema = [
            ("col1", "INTEGER"),
            ("col2", "VARCHAR(255)"),
        ]

        # Execute
        publish_pandas_via_s3_stage(
            table_name="TEST_TABLE",
            df=df,
            table_schema=table_schema,
            batch_size=10,
        )

        # Verify
        # Connection was closed
        mock_conn.close.assert_called_once()

        # S3 put_files was called
        assert mock_s3.put_files.called

        # SQL was executed (table creation and COPY INTO)
        assert mock_execute_sql.call_count >= 2

    def test_publish_via_s3_stage_empty_dataframe(self):
        """Test that publishing an empty DataFrame raises ValueError."""
        from ds_platform_utils.metaflow.pandas_via_s3_stage import (
            publish_pandas_via_s3_stage,
        )

        df = pd.DataFrame()
        table_schema = [("col1", "INTEGER")]

        with pytest.raises(ValueError, match="DataFrame is empty"):
            publish_pandas_via_s3_stage(
                table_name="TEST_TABLE",
                df=df,
                table_schema=table_schema,
            )

    def test_publish_via_s3_stage_invalid_type(self):
        """Test that publishing non-DataFrame raises TypeError."""
        from ds_platform_utils.metaflow.pandas_via_s3_stage import (
            publish_pandas_via_s3_stage,
        )

        table_schema = [("col1", "INTEGER")]

        with pytest.raises(TypeError, match="df must be a pandas DataFrame"):
            publish_pandas_via_s3_stage(
                table_name="TEST_TABLE",
                df="not a dataframe",
                table_schema=table_schema,
            )


class TestMakeBatchPredictionsFromSnowflakeViaS3Stage:
    """Test make_batch_predictions_from_snowflake_via_s3_stage function."""

    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.get_snowflake_connection")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._execute_sql")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._list_files_in_s3_folder")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._get_df_from_s3_files")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._get_metaflow_s3_client")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.current")
    @patch("os.remove")
    def test_batch_predictions_basic(
        self,
        mock_remove,
        mock_current,
        mock_s3_client,
        mock_get_df_files,
        mock_list_files,
        mock_execute_sql,
        mock_get_conn,
    ):
        """Test basic batch predictions pipeline."""
        from ds_platform_utils.metaflow.pandas_via_s3_stage import (
            make_batch_predictions_from_snowflake_via_s3_stage,
        )

        # Setup mocks
        mock_current.is_production = False
        mock_current.card = MagicMock()

        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_s3 = MagicMock()
        mock_s3_client.return_value.__enter__ = Mock(return_value=mock_s3)
        mock_s3_client.return_value.__exit__ = Mock(return_value=False)

        # Mock S3 file listing
        mock_list_files.return_value = [
            "s3://bucket/path/file1.parquet",
            "s3://bucket/path/file2.parquet",
        ]

        # Mock reading from S3
        input_df = pd.DataFrame({"input_col": [1, 2, 3]})
        mock_get_df_files.return_value = input_df

        # Define predictor function
        def predictor(df: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "prediction": df["input_col"] * 2,
                }
            )

        output_schema = [("prediction", "INTEGER")]

        # Execute
        make_batch_predictions_from_snowflake_via_s3_stage(
            input_query="SELECT * FROM INPUT_TABLE",
            output_table_name="OUTPUT_TABLE",
            output_table_schema=output_schema,
            model_predictor_function=predictor,
        )

        # Verify
        # Connection was closed
        mock_conn.close.assert_called_once()

        # Predictor was called for each file (2 times)
        # S3 put_files was called for predictions
        assert mock_s3.put_files.called

        # SQL was executed (COPY INTO for input, table creation, COPY INTO for output)
        assert mock_execute_sql.call_count >= 3

    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.get_snowflake_connection")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._execute_sql")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage._list_files_in_s3_folder")
    @patch("ds_platform_utils.metaflow.pandas_via_s3_stage.current")
    def test_batch_predictions_no_files_raises_error(
        self,
        mock_current,
        mock_list_files,
        mock_execute_sql,
        mock_get_conn,
    ):
        """Test that no input files raises ValueError."""
        from ds_platform_utils.metaflow.pandas_via_s3_stage import (
            make_batch_predictions_from_snowflake_via_s3_stage,
        )

        # Setup mocks
        mock_current.is_production = False
        mock_current.card = MagicMock()

        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        # Mock empty file list
        mock_list_files.return_value = []

        def predictor(df: pd.DataFrame) -> pd.DataFrame:
            return df

        output_schema = [("col1", "INTEGER")]

        # Execute and verify error
        with pytest.raises(ValueError, match="No input files found"):
            make_batch_predictions_from_snowflake_via_s3_stage(
                input_query="SELECT * FROM INPUT_TABLE",
                output_table_name="OUTPUT_TABLE",
                output_table_schema=output_schema,
                model_predictor_function=predictor,
            )
