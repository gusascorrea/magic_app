import numpy as np
import pandas as pd
import streamlit as st

from app.config import (
    INITIAL_CAPITAL,
    LIVE_ANALYSIS_START,
    MIN_ANALYSIS_TRADING_DAYS,
    PORTFOLIO_KEYS,
    REALLOCATION_FREQUENCIES,
)
from app.repositories.history_repository import (
    load_live_portfolio_history,
    load_live_quote_history,
)


def get_effective_start_date(requested_date, available_dates):
    for snapshot_date in available_dates:
        if snapshot_date >= requested_date:
            return snapshot_date
    return None


def compute_rebalance_dates(start_date, end_date, months, available_dates):
    rebalance_dates = []
    target_date = start_date + pd.DateOffset(months=months)

    while target_date <= end_date:
        valid_dates = [
            snapshot_date
            for snapshot_date in available_dates
            if snapshot_date >= target_date and snapshot_date <= end_date
        ]
        if not valid_dates:
            break
        actual_date = valid_dates[0]
        rebalance_dates.append(actual_date)
        target_date = actual_date + pd.DateOffset(months=months)

    return rebalance_dates


def safe_ratio(return_value, drawdown_value):
    if pd.isna(return_value) or pd.isna(drawdown_value):
        return np.nan
    if abs(drawdown_value) < 1e-12:
        return np.nan
    return return_value / abs(drawdown_value)


def compute_cagr(initial_value, final_value, start_date, end_date):
    if any(pd.isna(value) for value in [initial_value, final_value, start_date, end_date]):
        return np.nan
    if initial_value <= 0 or final_value <= 0:
        return np.nan

    total_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
    if total_days <= 0:
        return np.nan

    years = total_days / 365.25
    if years <= 0:
        return np.nan

    return (final_value / initial_value) ** (1 / years) - 1


def build_config_label(row):
    return (
        f"{row['Estratégia']} | vol {int(row['Volume Mínimo'])}"
        f" | {int(row['Ativos na Carteira'])} ativos"
        f" | {row['frequencia_realocacao']}"
    )


def resolve_chart_start_date(history: pd.DataFrame, config_label: str) -> pd.Timestamp | None:
    config_dates = (
        history.loc[history["configuracao"] == config_label, "data_inicio_solicitada"]
        .dropna()
        .sort_values()
        .unique()
    )
    if len(config_dates) == 0:
        return None
    if LIVE_ANALYSIS_START in config_dates:
        return LIVE_ANALYSIS_START
    return config_dates[0]


@st.cache_data(show_spinner=False)
def build_live_reallocation_analysis():
    portfolio_history = load_live_portfolio_history()
    quote_history = load_live_quote_history()
    quote_matrix = (
        quote_history.pivot_table(
            index="Data",
            columns="papel",
            values="Cotação",
            aggfunc="last",
        )
        .sort_index()
        .ffill()
    )

    available_snapshot_dates = sorted(portfolio_history["Data"].unique())
    all_requested_start_dates = pd.date_range(
        LIVE_ANALYSIS_START, max(available_snapshot_dates), freq="D"
    )
    requested_start_dates = [
        snapshot_date
        for snapshot_date in available_snapshot_dates
        if snapshot_date >= LIVE_ANALYSIS_START
        and int((quote_matrix.index[quote_matrix.index >= snapshot_date]).shape[0])
        >= MIN_ANALYSIS_TRADING_DAYS
    ]
    latest_quote_date = quote_matrix.index.max()

    simulation_rows = []
    history_frames = []

    for key_values, portfolio_df in portfolio_history.groupby(PORTFOLIO_KEYS, dropna=False):
        key_dict = dict(zip(PORTFOLIO_KEYS, key_values))
        holdings_by_date = {
            date: sorted(group["papel"].tolist())
            for date, group in portfolio_df.groupby("Data")
        }

        for requested_start_date in requested_start_dates:
            effective_start_date = get_effective_start_date(
                requested_start_date, available_snapshot_dates
            )
            if effective_start_date is None or effective_start_date not in holdings_by_date:
                continue

            start_holdings = holdings_by_date[effective_start_date]
            start_prices = quote_matrix.loc[effective_start_date, start_holdings].dropna()
            if len(start_prices) != len(start_holdings):
                continue

            for frequency_name, months in REALLOCATION_FREQUENCIES.items():
                quantities = {
                    paper: round(
                        (INITIAL_CAPITAL / len(start_holdings)) / start_prices[paper], 0
                    )
                    for paper in start_holdings
                }
                current_holdings = start_holdings.copy()
                rebalance_dates = compute_rebalance_dates(
                    effective_start_date,
                    latest_quote_date,
                    months,
                    available_snapshot_dates,
                )
                rebalance_date_set = set(rebalance_dates)
                total_entries = 0
                total_exits = 0
                total_swaps = 0
                history_rows = []

                for day in quote_matrix.index[quote_matrix.index >= effective_start_date]:
                    if day in rebalance_date_set:
                        target_holdings = holdings_by_date.get(day)
                        if target_holdings is not None:
                            current_set = set(current_holdings)
                            target_set = set(target_holdings)
                            exits = sorted(current_set - target_set)
                            entries = sorted(target_set - current_set)
                            current_value = 0.0

                            for paper, quantity in quantities.items():
                                price = quote_matrix.at[day, paper]
                                current_value += quantity * (
                                    0.0 if pd.isna(price) else float(price)
                                )

                            target_prices = quote_matrix.loc[day, target_holdings].dropna()
                            if len(target_prices) == len(target_holdings):
                                quantities = {
                                    paper: round(
                                        (current_value / len(target_holdings))
                                        / target_prices[paper],
                                        0,
                                    )
                                    for paper in target_holdings
                                }
                                current_holdings = target_holdings.copy()
                                total_entries += len(entries)
                                total_exits += len(exits)
                                total_swaps += max(len(entries), len(exits))

                    portfolio_value = 0.0
                    for paper, quantity in quantities.items():
                        price = quote_matrix.at[day, paper]
                        portfolio_value += quantity * (
                            0.0 if pd.isna(price) else float(price)
                        )

                    history_rows.append(
                        {
                            "Data": day,
                            "valor_carteira": portfolio_value,
                            "data_inicio_solicitada": requested_start_date,
                            "data_inicio_efetiva": effective_start_date,
                            "frequencia_realocacao": frequency_name,
                            **key_dict,
                        }
                    )

                history = pd.DataFrame(history_rows)
                history["retorno_diario"] = history["valor_carteira"].pct_change()
                history["pico"] = history["valor_carteira"].cummax()
                history["drawdown"] = history["valor_carteira"] / history["pico"] - 1
                history["retorno_acumulado"] = (
                    history["valor_carteira"] / INITIAL_CAPITAL - 1
                )
                cagr = compute_cagr(
                    INITIAL_CAPITAL,
                    history["valor_carteira"].iloc[-1],
                    history["Data"].iloc[0],
                    history["Data"].iloc[-1],
                )
                history_frames.append(history)

                simulation_rows.append(
                    {
                        **key_dict,
                        "data_inicio_solicitada": requested_start_date,
                        "data_inicio_efetiva": effective_start_date,
                        "data_fim": history["Data"].max(),
                        "frequencia_realocacao": frequency_name,
                        "retorno_total": history["retorno_acumulado"].iloc[-1],
                        "cagr": cagr,
                        "drawdown_maximo": history["drawdown"].min(),
                        "volatilidade_anualizada": history["retorno_diario"].std()
                        * np.sqrt(252),
                        "rebalanceamentos_executados": len(rebalance_dates),
                        "ativos_trocados_total": total_swaps,
                        "entradas_total": total_entries,
                        "saidas_total": total_exits,
                    }
                )

    simulation_summary = pd.DataFrame(simulation_rows).sort_values(
        ["data_inicio_solicitada", "frequencia_realocacao"] + PORTFOLIO_KEYS
    )
    simulation_summary["razao_cagr_drawdown"] = simulation_summary.apply(
        lambda row: safe_ratio(row["cagr"], row["drawdown_maximo"]),
        axis=1,
    )
    simulation_summary["configuracao"] = simulation_summary.apply(
        build_config_label, axis=1
    )

    simulation_history = pd.concat(history_frames, ignore_index=True)
    simulation_history["configuracao"] = simulation_history.apply(
        build_config_label, axis=1
    )

    best_return_by_period = simulation_summary.groupby("data_inicio_solicitada")["cagr"].transform(
        "max"
    )
    simulation_summary["venceu_periodo"] = (
        simulation_summary["cagr"] == best_return_by_period
    ).astype(int)

    configuration_group_keys = [
        "Estratégia",
        "Volume Mínimo",
        "Ativos na Carteira",
        "frequencia_realocacao",
        "configuracao",
    ]

    best_start_dates = (
        simulation_summary.loc[
            simulation_summary.groupby(configuration_group_keys)["cagr"].idxmax(),
            configuration_group_keys + ["data_inicio_solicitada"],
        ]
        .rename(columns={"data_inicio_solicitada": "data_inicio_maior_retorno"})
        .reset_index(drop=True)
    )
    worst_drawdown_start_dates = (
        simulation_summary.loc[
            simulation_summary.groupby(configuration_group_keys)["drawdown_maximo"].idxmin(),
            configuration_group_keys + ["data_inicio_solicitada"],
        ]
        .rename(columns={"data_inicio_solicitada": "data_inicio_maior_drawdown"})
        .reset_index(drop=True)
    )

    configuration_summary = (
        simulation_summary.groupby(
            configuration_group_keys,
            as_index=False,
        )
        .agg(
            cagr_medio=("cagr", "mean"),
            cagr_mediano=("cagr", "median"),
            cagr_min=("cagr", "min"),
            cagr_max=("cagr", "max"),
            cagr_q1=("cagr", lambda series: series.quantile(0.25)),
            drawdown_medio=("drawdown_maximo", "mean"),
            drawdown_pior=("drawdown_maximo", "min"),
            drawdown_q1=("drawdown_maximo", lambda series: series.quantile(0.25)),
            volatilidade_media=("volatilidade_anualizada", "mean"),
            vitorias_por_periodo=("venceu_periodo", "sum"),
            trocas_totais=("ativos_trocados_total", "sum"),
        )
        .assign(
            razao_q1_cagr_q1_drawdown=lambda df: df.apply(
                lambda row: safe_ratio(row["cagr_q1"], row["drawdown_q1"]),
                axis=1,
            )
        )
        .sort_values(
            [
                "razao_q1_cagr_q1_drawdown",
                "cagr_q1",
                "drawdown_q1",
                "vitorias_por_periodo",
            ],
            ascending=[False, False, False, False],
        )
        .reset_index(drop=True)
    )
    configuration_summary = configuration_summary.merge(
        best_start_dates,
        on=configuration_group_keys,
        how="left",
    ).merge(
        worst_drawdown_start_dates,
        on=configuration_group_keys,
        how="left",
    )

    return {
        "simulation_summary": simulation_summary,
        "simulation_history": simulation_history,
        "configuration_summary": configuration_summary,
        "available_snapshot_dates": available_snapshot_dates,
        "all_requested_start_dates": all_requested_start_dates,
        "requested_start_dates": requested_start_dates,
        "latest_quote_date": latest_quote_date,
    }
