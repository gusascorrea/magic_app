import argparse
import io
import subprocess
from pathlib import Path

import pandas as pd

"""
run:
/home/gus/.cache/pypoetry/virtualenvs/magic-app-X1JMDIUZ-py3.12/bin/python scripts/build_performance_history_parquet.py --since "2026-03-10 00:00:00"
"""


REPO_ROOT = Path(__file__).resolve().parents[1]
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
    return parser.parse_args()


def run_git_command(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True)


def build_default_output_path(input_path: Path, since: str) -> Path:
    date_part = since.split()[0]
    return input_path.with_name(f"{input_path.stem}_committed_since_{date_part}.parquet")


def load_committed_csv_versions(input_path: Path, since: str) -> pd.DataFrame:
    log_output = run_git_command(
        "log",
        f"--since={since}",
        "--reverse",
        "--format=%H|%cI",
        "--",
        str(input_path),
    ).strip()

    if not log_output:
        raise RuntimeError(
            f"Nenhum commit encontrado para {input_path} desde {since}."
        )

    frames = []
    for line in log_output.splitlines():
        commit_hash, committed_at = line.split("|", 1)
        csv_content = run_git_command("show", f"{commit_hash}:{input_path}")
        frame = pd.read_csv(io.StringIO(csv_content))
        frame["commit_hash"] = commit_hash
        frame["commit_committed_at"] = committed_at
        frames.append(frame)

    return pd.concat(frames, ignore_index=True)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = (
        Path(args.output)
        if args.output is not None
        else build_default_output_path(input_path, args.since)
    )

    history = load_committed_csv_versions(input_path, args.since)
    destination = REPO_ROOT / output_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    history.to_parquet(destination, index=False, engine="pyarrow")

    print(f"Arquivo gerado: {destination}")
    print(f"Linhas: {len(history)}")
    print(f"Commits: {history['commit_hash'].nunique()}")


if __name__ == "__main__":
    main()
