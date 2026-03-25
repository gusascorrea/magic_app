from pathlib import Path

import pandas as pd


def read_parquet(path: Path, columns=None) -> pd.DataFrame:
    return pd.read_parquet(path, columns=columns)


def write_parquet(df: pd.DataFrame, path: Path, engine: str = "pyarrow") -> None:
    df.to_parquet(path, engine=engine)
