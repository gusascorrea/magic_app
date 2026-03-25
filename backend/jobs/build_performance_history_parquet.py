import argparse
from pathlib import Path

from backend.services.performance_history_service import (
    build_default_output_path,
    build_performance_history,
)


DEFAULT_INPUT = Path("data/performance.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera um arquivo Parquet com todas as versoes commitadas de um CSV no Git."
        )
    )
    parser.add_argument(
        "--since",
        default="2026-03-10 00:00:00",
        help="Data inicial para filtrar commits no formato aceito pelo git log.",
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="Caminho do CSV versionado dentro do repositorio.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Caminho de saida do parquet. Se omitido, usa data/<nome>_committed_since_<data>.parquet.",
    )
    parser.add_argument(
        "--include-working-tree",
        action="store_true",
        help="Inclui a versao atual do CSV no workspace, mesmo antes de commit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = (
        Path(args.output)
        if args.output is not None
        else build_default_output_path(input_path, args.since)
    )

    history = build_performance_history(
        input_path=input_path,
        output_path=output_path,
        since=args.since,
        include_working_tree=args.include_working_tree,
    )

    print(f"Arquivo gerado: {output_path}")
    print(f"Linhas: {len(history)}")
    print(f"Commits: {history['commit_hash'].nunique()}")


if __name__ == "__main__":
    main()
