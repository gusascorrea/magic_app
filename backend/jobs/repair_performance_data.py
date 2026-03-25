from __future__ import annotations

import argparse

from backend.services.performance_repair_service import (
    load_quote_matrix,
    read_table,
    repair_performance_dataframe,
    write_table,
)
from shared.config import REPO_ROOT


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
