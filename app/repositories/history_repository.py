import pandas as pd
import streamlit as st

from app.clients.parquet_client import read_parquet
from app.config import (
    BENCHMARK_HISTORY_PATH,
    BENCHMARK_LABELS,
    PERFORMANCE_HISTORY_PATH,
    PORTFOLIO_KEYS,
    QUOTE_HISTORY_PATH,
    ROW_KEYS,
)


@st.cache_data(show_spinner=False)
def load_live_portfolio_history():
    if not PERFORMANCE_HISTORY_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {PERFORMANCE_HISTORY_PATH}")

    df = read_parquet(PERFORMANCE_HISTORY_PATH)
    df["Data"] = pd.to_datetime(df["Data"])
    df["commit_committed_at"] = pd.to_datetime(
        df["commit_committed_at"],
        utc=True,
        format="mixed",
        errors="coerce",
    )
    df = df.dropna(subset=["commit_committed_at"])
    return (
        df.sort_values("commit_committed_at")
        .drop_duplicates(subset=ROW_KEYS, keep="last")
        .sort_values(PORTFOLIO_KEYS + ["Data", "papel"])
        .reset_index(drop=True)
    )


@st.cache_data(show_spinner=False)
def load_live_quote_history():
    if not QUOTE_HISTORY_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {QUOTE_HISTORY_PATH}")

    df = read_parquet(QUOTE_HISTORY_PATH)
    if "papel" not in df.columns:
        df = df.reset_index(names="papel")
    df = df.rename(columns={"update date": "Data"})
    df["Data"] = pd.to_datetime(df["Data"])
    df["Cotação"] = pd.to_numeric(df["Cotação"], errors="coerce")
    return (
        df[["papel", "Data", "Cotação"]]
        .dropna(subset=["papel", "Data"])
        .sort_values(["papel", "Data"])
        .reset_index(drop=True)
    )


@st.cache_data(show_spinner=False)
def load_live_benchmark_history():
    if not BENCHMARK_HISTORY_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {BENCHMARK_HISTORY_PATH}")

    df = read_parquet(BENCHMARK_HISTORY_PATH)
    df["Data"] = pd.to_datetime(df["Data"])
    for column in BENCHMARK_LABELS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.sort_values("Data").drop_duplicates(subset=["Data"], keep="last").reset_index(
        drop=True
    )
