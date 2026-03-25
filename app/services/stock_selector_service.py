import pandas as pd

from app.repositories.fundamentus_repository import (
    load_financial_sector_tickers,
    load_raw_fundamentus_result,
)


def build_investment_table(strategy, portfolio_size, total_investment, minimum_volume):
    base_df = load_raw_fundamentus_result().copy()
    financial_tickers = load_financial_sector_tickers()

    filtered_df = base_df[~base_df.index.isin(financial_tickers)]
    filtered_df = filtered_df[
        ~(
            filtered_df.index.astype(str).str.contains("33")
            | filtered_df.index.astype(str).str.contains("34")
        )
    ]
    filtered_df = filtered_df.loc[filtered_df["Mrg Ebit"] > 0]
    filtered_df = filtered_df.loc[filtered_df["Liq.2meses"] >= minimum_volume].copy()

    filtered_df["First4Chars"] = filtered_df.index.str[:4]
    duplicates = filtered_df.duplicated(subset="First4Chars", keep=False)
    max_values = filtered_df.groupby("First4Chars")["Liq.2meses"].transform("max")
    filtered_df = filtered_df[(~duplicates) | (filtered_df["Liq.2meses"] == max_values)]
    filtered_df = filtered_df.drop(columns="First4Chars")
    filtered_df["Earnings Yield"] = round(1 / filtered_df["EV/EBIT"] * 100, 1)
    filtered_df["ROIC"] = round(filtered_df["ROIC"] * 100, 1)

    strategy_builders = {
        "Earnings Yield": _build_earnings_yield_table,
        "Magic Formula": _build_magic_formula_table,
        "ROIC": _build_roic_table,
    }
    investment_table = strategy_builders[strategy](
        filtered_df,
        portfolio_size=portfolio_size,
        total_investment=total_investment,
    )

    return investment_table


def _assign_position_data(df, portfolio_size, total_investment):
    df["Quantidade"] = round(
        (total_investment / portfolio_size) / df["Cotação"],
        0,
    )
    df["Valor"] = df["Quantidade"] * df["Cotação"]
    return df


def _append_total_row(df, columns_without_total):
    df.loc["Total"] = df.sum()
    df.loc["Total", columns_without_total] = "-"
    return df


def _build_earnings_yield_table(df, portfolio_size, total_investment):
    ranked_df = df.sort_values(by=["EV/EBIT", "Liq.2meses"], ascending=[True, False]).copy()
    ranked_df = _assign_position_data(ranked_df, portfolio_size, total_investment)
    ranked_df = ranked_df[
        ["Cotação", "Earnings Yield", "Liq.2meses", "Quantidade", "Valor"]
    ].head(portfolio_size)
    return _append_total_row(
        ranked_df,
        ["Cotação", "Earnings Yield", "Liq.2meses", "Quantidade"],
    )


def _build_magic_formula_table(df, portfolio_size, total_investment):
    ranked_df = df.copy()
    ranked_df["Ranking_Earning_Yield"] = ranked_df["Earnings Yield"].rank(ascending=False)
    ranked_df["Ranking_ROIC"] = ranked_df["ROIC"].rank(ascending=False)
    ranked_df["Magic Formula"] = (
        ranked_df["Ranking_Earning_Yield"] + ranked_df["Ranking_ROIC"]
    )
    ranked_df = ranked_df.sort_values(
        by=["Magic Formula", "Liq.2meses"], ascending=[True, False]
    ).copy()
    ranked_df = _assign_position_data(ranked_df, portfolio_size, total_investment)
    ranked_df = ranked_df[
        [
            "Cotação",
            "Earnings Yield",
            "ROIC",
            "Magic Formula",
            "Liq.2meses",
            "Quantidade",
            "Valor",
        ]
    ].head(portfolio_size)
    return _append_total_row(
        ranked_df,
        [
            "Cotação",
            "Earnings Yield",
            "ROIC",
            "Magic Formula",
            "Liq.2meses",
            "Quantidade",
        ],
    )


def _build_roic_table(df, portfolio_size, total_investment):
    ranked_df = df.sort_values(by=["ROIC", "Liq.2meses"], ascending=[False, False]).copy()
    ranked_df = _assign_position_data(ranked_df, portfolio_size, total_investment)
    ranked_df = ranked_df[
        ["Cotação", "ROIC", "Liq.2meses", "Quantidade", "Valor"]
    ].head(portfolio_size)
    return _append_total_row(
        ranked_df,
        ["Cotação", "ROIC", "Liq.2meses", "Quantidade"],
    )
