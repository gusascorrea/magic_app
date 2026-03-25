import argparse
from pathlib import Path

from backend.services.performance_analysis_service import (
    PORTFOLIO_KEYS,
    build_daily_portfolio_values,
    build_period_analysis,
    load_latest_rows,
    save_period_analysis,
)
from shared.config import PERFORMANCE_HISTORY_PATH, PERFORMANCE_PERIOD_ANALYSIS_PATH, REPO_ROOT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera uma analise de performance por combinacao de carteira "
            "em frequencias mensal, trimestral e anual."
        )
    )
    parser.add_argument(
        "--input",
        default=str(PERFORMANCE_HISTORY_PATH.relative_to(REPO_ROOT)),
        help="Arquivo parquet com o historico commitado de performance.",
    )
    parser.add_argument(
        "--output",
        default=str(PERFORMANCE_PERIOD_ANALYSIS_PATH.relative_to(REPO_ROOT)),
        help="Arquivo parquet de saida com a analise consolidada.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = REPO_ROOT / Path(args.input)
    output_path = REPO_ROOT / Path(args.output)

    latest_rows = load_latest_rows(input_path)
    daily = build_daily_portfolio_values(latest_rows)
    analysis = build_period_analysis(daily)
    save_period_analysis(analysis, output_path)

    print(f"Arquivo gerado: {output_path}")
    print(f"Carteiras unicas: {daily[PORTFOLIO_KEYS].drop_duplicates().shape[0]}")
    print(f"Datas unicas: {daily['Data'].nunique()}")
    print(f"Linhas da analise: {len(analysis)}")


if __name__ == "__main__":
    main()
