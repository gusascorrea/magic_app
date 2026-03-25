import io
import subprocess
from pathlib import Path

import pandas as pd

from backend.services.performance_repair_service import (
    load_quote_matrix,
    repair_performance_dataframe,
)
from shared.clients.parquet_client import write_parquet
from shared.config import REPO_ROOT


ROW_KEYS = ["Estratégia", "Volume Mínimo", "Ativos na Carteira", "Data", "papel"]


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


def load_working_tree_csv_version(input_path: Path) -> pd.DataFrame:
    working_tree_path = REPO_ROOT / input_path
    if not working_tree_path.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado no workspace: {working_tree_path}")

    frame = pd.read_csv(working_tree_path)
    frame["commit_hash"] = "WORKING_TREE"
    frame["commit_committed_at"] = pd.Timestamp.now(
        tz="America/Sao_Paulo"
    ).isoformat(timespec="seconds")
    return frame


def build_performance_history(
    input_path: Path,
    output_path: Path,
    since: str,
    include_working_tree: bool,
) -> pd.DataFrame:
    history = load_committed_csv_versions(input_path, since)
    if include_working_tree:
        history = pd.concat(
            [history, load_working_tree_csv_version(input_path)],
            ignore_index=True,
        )
    history = repair_performance_dataframe(
        history,
        load_quote_matrix(),
        commit_sort_columns=["commit_committed_at", "commit_hash"],
    )
    history["_commit_sort"] = pd.to_datetime(
        history["commit_committed_at"],
        utc=True,
        format="mixed",
        errors="coerce",
    )
    history = (
        history.sort_values(["_commit_sort", "commit_hash"])
        .drop_duplicates(subset=ROW_KEYS, keep="last")
        .drop(columns="_commit_sort")
        .reset_index(drop=True)
    )

    destination = REPO_ROOT / output_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(history, destination, engine="pyarrow")
    return history
