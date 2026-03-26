from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from shared.clients.parquet_client import read_parquet, write_parquet
from shared.config import INITIAL_CAPITAL, QUOTE_HISTORY_PATH


PORTFOLIO_KEYS = ["Estratégia", "Ativos na Carteira", "Volume Mínimo"]


def _deduplicate_papers(frame: pd.DataFrame) -> pd.DataFrame:
    if frame["papel"].is_unique:
        return frame
    return frame.drop_duplicates(subset=["papel"], keep="last")


def load_quote_matrix() -> pd.DataFrame:
    quotes = read_parquet(QUOTE_HISTORY_PATH)
    if "papel" not in quotes.columns:
        quotes = quotes.reset_index(names="papel")
    quotes = quotes.rename(columns={"update date": "Data"})
    quotes["Data"] = pd.to_datetime(quotes["Data"])
    quotes["Cotação"] = pd.to_numeric(quotes["Cotação"], errors="coerce")
    return (
        quotes.pivot_table(index="Data", columns="papel", values="Cotação", aggfunc="last")
        .sort_index()
        .ffill()
    )


def _get_price(quote_matrix: pd.DataFrame, date: pd.Timestamp, paper: str) -> float:
    price = quote_matrix.at[date, paper]
    if pd.isna(price):
        raise ValueError(f"Cotacao indisponivel para {paper} em {date.date()}.")
    return float(price)


def repair_performance_dataframe(
    df: pd.DataFrame,
    quote_matrix: pd.DataFrame,
    *,
    commit_sort_columns: list[str] | None = None,
) -> pd.DataFrame:
    repaired = df.copy()
    repaired["Data"] = pd.to_datetime(repaired["Data"])
    repaired["Cotação"] = pd.to_numeric(repaired["Cotação"], errors="coerce")

    sort_columns = PORTFOLIO_KEYS + ["Data"]
    if commit_sort_columns:
        sort_columns.extend(commit_sort_columns)
    repaired = repaired.sort_values(sort_columns).reset_index(drop=True)
    snapshot_group_columns = ["Data"]
    if commit_sort_columns:
        snapshot_group_columns.extend(commit_sort_columns)

    result_frames = []

    for _, portfolio_df in repaired.groupby(PORTFOLIO_KEYS, dropna=False, sort=False):
        previous_quantities: pd.Series | None = None

        for snapshot_key, daily_df in portfolio_df.groupby(
            snapshot_group_columns,
            dropna=False,
            sort=False,
        ):
            date = snapshot_key[0] if isinstance(snapshot_key, tuple) else snapshot_key
            current = _deduplicate_papers(daily_df.copy()).set_index("papel")
            prices = current["Cotação"].astype(float)
            quantities = pd.Series(0.0, index=current.index, dtype="float64")

            if previous_quantities is None:
                quantities = np.round((INITIAL_CAPITAL / len(current.index)) / prices, 0)
            else:
                kept = previous_quantities.index.intersection(current.index)
                sold = previous_quantities.index.difference(current.index)
                new_entries = current.index.difference(previous_quantities.index)

                if len(kept) > 0:
                    quantities.loc[kept] = previous_quantities.loc[kept].astype(float)

                if len(sold) > 0 and len(new_entries) > 0:
                    sold_value = sum(
                        previous_quantities.loc[paper] * _get_price(quote_matrix, date, paper)
                        for paper in sold
                    )
                    allocation_per_entry = sold_value / len(sold)
                    quantities.loc[new_entries] = np.round(
                        allocation_per_entry / prices.loc[new_entries], 0
                    )

            current["Quantidade"] = quantities.astype(float)
            current["Valor"] = current["Quantidade"] * prices
            previous_quantities = current["Quantidade"].copy()
            result_frames.append(current.reset_index())

    output = pd.concat(result_frames, ignore_index=True)
    ordered_columns = [column for column in repaired.columns if column in output.columns]
    return output[ordered_columns]


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        return pd.read_csv(path)
    if path.suffix == ".parquet":
        return read_parquet(path)
    raise ValueError(f"Formato nao suportado: {path.suffix}")


def write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".csv":
        df.to_csv(path, index=False)
        return
    if path.suffix == ".parquet":
        write_parquet(df, path, engine="pyarrow")
        return
    raise ValueError(f"Formato nao suportado: {path.suffix}")
