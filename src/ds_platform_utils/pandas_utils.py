import pandas as pd


def estimate_chunk_size(
    df: pd.DataFrame,
    target_chunk_size_in_mb: int = 20,
    sample_rows: int = 10000,
) -> int:
    if len(df) == 0:
        raise ValueError("DataFrame is empty. Cannot estimate chunk size.")

    sample = df.head(sample_rows)
    bytes_per_row = sample.memory_usage(deep=True).sum() / len(sample)
    target_chunk_size_bytes = target_chunk_size_in_mb * 1024 * 1024
    chunk_size = int(target_chunk_size_bytes / bytes_per_row)
    return max(1, chunk_size)
