from pathlib import Path

import pandas as pd

from shared.clients.parquet_client import read_parquet, write_parquet


PORTFOLIO_KEYS = ["Estratégia", "Volume Mínimo", "Ativos na Carteira"]
ROW_KEYS = PORTFOLIO_KEYS + ["Data", "papel"]


def load_latest_rows(input_path: Path) -> pd.DataFrame:
    df = read_parquet(input_path)
    df["Data"] = pd.to_datetime(df["Data"])
    df["commit_committed_at"] = pd.to_datetime(df["commit_committed_at"], utc=True)
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    df = (
        df.sort_values("commit_committed_at")
        .drop_duplicates(subset=ROW_KEYS, keep="last")
        .sort_values(PORTFOLIO_KEYS + ["Data", "papel"])
        .reset_index(drop=True)
    )
    return df


def build_daily_portfolio_values(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby(PORTFOLIO_KEYS + ["Data"], dropna=False)
        .agg(
            valor_total=("Valor", "sum"),
            ativos=("papel", "nunique"),
            ativos_sem_valor=("Valor", lambda s: int(s.isna().sum())),
            ultimo_commit=("commit_committed_at", "max"),
        )
        .reset_index()
        .sort_values(PORTFOLIO_KEYS + ["Data"])
        .reset_index(drop=True)
    )

    daily["retorno_diario"] = daily.groupby(PORTFOLIO_KEYS)["valor_total"].pct_change()
    daily["retorno_acumulado"] = daily.groupby(PORTFOLIO_KEYS)["valor_total"].transform(
        lambda s: s / s.iloc[0] - 1
    )
    return daily


def build_period_analysis(daily: pd.DataFrame) -> pd.DataFrame:
    period_frames = []
    period_map = {"mensal": "M", "trimestral": "Q", "anual": "Y"}

    for periodo, freq in period_map.items():
        period_df = daily.copy()
        period_df["periodo"] = period_df["Data"].dt.to_period(freq).dt.to_timestamp(freq)

        grouped = []
        for _, portfolio_df in period_df.groupby(PORTFOLIO_KEYS, dropna=False):
            portfolio_df = portfolio_df.sort_values("Data")
            consolidated = (
                portfolio_df.groupby("periodo", as_index=False)
                .agg(
                    data_inicial=("Data", "min"),
                    data_final=("Data", "max"),
                    valor_inicial=("valor_total", "first"),
                    valor_final=("valor_total", "last"),
                    dias_observados=("Data", "nunique"),
                    ativos=("ativos", "last"),
                    ativos_sem_valor=("ativos_sem_valor", "last"),
                    ultimo_commit=("ultimo_commit", "max"),
                )
            )
            for key in PORTFOLIO_KEYS:
                consolidated[key] = portfolio_df[key].iloc[0]
            grouped.append(consolidated)

        consolidated = pd.concat(grouped, ignore_index=True)
        consolidated["frequencia"] = periodo
        consolidated["retorno_periodo"] = (
            consolidated["valor_final"] / consolidated["valor_inicial"] - 1
        )
        period_frames.append(consolidated)

    return (
        pd.concat(period_frames, ignore_index=True)
        .sort_values(["frequencia"] + PORTFOLIO_KEYS + ["periodo"])
        .reset_index(drop=True)
    )


def save_period_analysis(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(df, output_path, engine="pyarrow")
