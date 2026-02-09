import pandas as pd
from metaflow import S3


def _get_metaflow_s3_client():
    return S3(role="arn:aws:iam::209479263910:role/outerbounds_iam_role")


def _list_files_in_s3_folder(path: str) -> list:
    if not path.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'.")

    with _get_metaflow_s3_client() as s3:
        return [path.url for path in s3.list_paths([path])]


def _download_all_files_in_s3_folder(path: str) -> list:
    if not path.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'.")

    with _get_metaflow_s3_client() as s3:
        return [obj.path for obj in s3.get_many(_list_files_in_s3_folder(path))]


def _get_df_from_s3_file(path: str) -> pd.DataFrame:
    if not path.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'.")

    with _get_metaflow_s3_client() as s3:
        return pd.read_parquet(s3.get(path).path)


def _get_df_from_s3_files(paths: list[str]) -> pd.DataFrame:
    if any(not path.startswith("s3://") for path in paths):
        raise ValueError("Invalid S3 URI. All paths must start with 's3://'.")

    with _get_metaflow_s3_client() as s3:
        df_paths = [obj.path for obj in s3.get_many(paths)]
        return pd.read_parquet(df_paths)


def _get_df_from_s3_folder(path: str) -> pd.DataFrame:
    if not path.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'.")

    files = _list_files_in_s3_folder(path)
    return _get_df_from_s3_files(files)


def _put_df_to_s3_file(df: pd.DataFrame, path: str) -> None:
    if not path.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'.")

    with _get_metaflow_s3_client() as s3:
        timestamp_str = pd.Timestamp("now").strftime("%Y%m%d_%H%M%S_%f")
        local_path = f"/tmp/{timestamp_str}.parquet"
        df.to_parquet(local_path)
        s3.put_files(key_paths=[[path, local_path]])


def _put_df_to_s3_folder(df: pd.DataFrame, path: str, chunk_size=None, compression="snappy") -> None:
    if not path.startswith("s3://"):
        raise ValueError("Invalid S3 URI. Must start with 's3://'.")

    if not path.endswith("/"):
        path = path.removesuffix("/")

    target_chunk_size_mb = 50
    target_chunk_size_bytes = target_chunk_size_mb * 1024 * 1024

    def estimate_bytes_per_row(df_sample):
        return df_sample.memory_usage(deep=True).sum() / len(df_sample)

    if chunk_size is None:
        sample = df.head(10000)
        bytes_per_row = estimate_bytes_per_row(sample)
        chunk_size = int(target_chunk_size_bytes / bytes_per_row)
        chunk_size = max(1, chunk_size)

    with _get_metaflow_s3_client() as s3:
        timestamp = pd.Timestamp("now").strftime("%Y%m%d_%H%M%S_%f")
        local_path_template = f"/tmp/{timestamp}_data_part_{{}}.parquet"
        key_paths = []
        num_rows = df.shape[0]
        for i in range(0, num_rows, chunk_size):
            local_path = local_path_template.format(i // chunk_size)
            df.iloc[i : i + chunk_size].to_parquet(local_path, index=False, compression=compression)
            s3_path = f"{path}/data_part_{i // chunk_size}.parquet"
            key_paths.append([s3_path, local_path])
        s3.put_files(key_paths=key_paths)
