from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
QUOTE_HISTORY_PATH = REPO_ROOT / "data" / "fundamentus_data.parquet"
PORTFOLIO_KEYS = ["Estratégia", "Ativos na Carteira", "Volume Mínimo"]
INITIAL_CAPITAL = 100000.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recalcula Quantidade e Valor em snapshots de performance."
    )
    parser.add_argument("--input", required=True, help="Arquivo de entrada CSV ou Parquet.")
    parser.add_argument(
        "--output",
        default=None,
        help="Arquivo de saida CSV ou Parquet. Se omitido, sobrescreve a entrada.",
    )
    return parser.parse_args()


def load_quote_matrix() -> pd.DataFrame:
    quotes = pd.read_parquet(QUOTE_HISTORY_PATH)
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

    result_frames = []

    for _, portfolio_df in repaired.groupby(PORTFOLIO_KEYS, dropna=False, sort=False):
        previous_quantities: pd.Series | None = None

        for date, daily_df in portfolio_df.groupby("Data", sort=True):
            current = daily_df.copy().set_index("papel")
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
        return pd.read_parquet(path)
    raise ValueError(f"Formato nao suportado: {path.suffix}")


def write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".csv":
        df.to_csv(path, index=False)
        return
    if path.suffix == ".parquet":
        df.to_parquet(path, index=False, engine="pyarrow")
        return
    raise ValueError(f"Formato nao suportado: {path.suffix}")


def main() -> None:
    args = parse_args()
    input_path = REPO_ROOT / args.input
    output_path = REPO_ROOT / args.output if args.output else input_path

    df = read_table(input_path)
    quote_matrix = load_quote_matrix()
    commit_sort_columns = (
        ["commit_committed_at", "commit_hash"]
        if {"commit_committed_at", "commit_hash"}.issubset(df.columns)
        else None
    )
    repaired = repair_performance_dataframe(
        df,
        quote_matrix,
        commit_sort_columns=commit_sort_columns,
    )
    write_table(repaired, output_path)

    print(f"Arquivo reparado: {output_path}")
    print(f"Linhas: {len(repaired)}")
    print(f"Quantidade nula: {int(repaired['Quantidade'].isna().sum())}")
    print(f"Valor nulo: {int(repaired['Valor'].isna().sum())}")


if __name__ == "__main__":
    main()
