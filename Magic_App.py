import altair as alt
import fundamentus as fd
import numpy as np
import pandas as pd
import streamlit as st
import re
import unicodedata
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
PERFORMANCE_HISTORY_PATH = (
    REPO_ROOT / "data" / "performance_committed_since_2026-03-10.parquet"
)
QUOTE_HISTORY_PATH = REPO_ROOT / "data" / "fundamentus_data.parquet"
BENCHMARK_HISTORY_PATH = REPO_ROOT / "data" / "benchmark_history.parquet"
INITIAL_CAPITAL = 100000.0
PORTFOLIO_KEYS = ["Estratégia", "Volume Mínimo", "Ativos na Carteira"]
ROW_KEYS = PORTFOLIO_KEYS + ["Data", "papel"]
REALLOCATION_FREQUENCIES = {"mensal": 1, "trimestral": 3, "anual": 12}
LIVE_ANALYSIS_START = pd.Timestamp("2026-03-12")
MIN_ANALYSIS_TRADING_DAYS = 4
BENCHMARK_LABELS = {
    "ibov_close": "IBOV",
    "cdi_rate_aa": "CDI",
    "sp500_close": "S&P500",
    "bitcoin_close": "Bitcoin",
}


def _to_snake_case(value):
    text = str(value).strip()
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    snake_text = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_text).strip("_").lower()
    return snake_text or text


def _prepare_streamlit_dataframe(df):
    prepared = df.copy()
    for column in prepared.columns:
        if pd.api.types.is_object_dtype(prepared[column]):
            prepared[column] = prepared[column].astype("string")
    return prepared


def _prepare_snake_case_table(df):
    prepared = _prepare_streamlit_dataframe(df.copy())
    prepared.columns = [_to_snake_case(column) for column in prepared.columns]

    if not isinstance(prepared.index, pd.RangeIndex):
        prepared.index = [
            _to_snake_case(index)
            if isinstance(index, str) and not re.fullmatch(r"[A-Z0-9]+", index)
            else index
            for index in prepared.index
        ]

    return prepared


def _safe_list_papel_setor(setor_id):
    try:
        papeis = fd.list_papel_setor(setor_id)
    except Exception:
        return []
    return papeis if isinstance(papeis, list) else []


def _load_financial_sector_tickers():
    fin = []
    seg = []

    for setor_id in range(200):
        papeis = _safe_list_papel_setor(setor_id)
        if not fin and "BBAS3" in papeis:
            fin = papeis
        if not seg and "WIZC3" in papeis:
            seg = papeis
        if fin and seg:
            financeiras = fin + seg
            financeiras.remove("WIZC3")
            return financeiras

    return []


@st.cache_data(show_spinner=False)
def load_live_portfolio_history():
    if not PERFORMANCE_HISTORY_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {PERFORMANCE_HISTORY_PATH}")

    df = pd.read_parquet(PERFORMANCE_HISTORY_PATH)
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

    df = pd.read_parquet(QUOTE_HISTORY_PATH)
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


def _get_effective_start_date(requested_date, available_dates):
    for snapshot_date in available_dates:
        if snapshot_date >= requested_date:
            return snapshot_date
    return None


def _compute_rebalance_dates(start_date, end_date, months, available_dates):
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


def _safe_ratio(return_value, drawdown_value):
    if pd.isna(return_value) or pd.isna(drawdown_value):
        return np.nan
    if abs(drawdown_value) < 1e-12:
        return np.nan
    return return_value / abs(drawdown_value)


def _compute_cagr(initial_value, final_value, start_date, end_date):
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


def _build_config_label(row):
    return (
        f"{row['Estratégia']} | vol {int(row['Volume Mínimo'])}"
        f" | {int(row['Ativos na Carteira'])} ativos"
        f" | {row['frequencia_realocacao']}"
    )


def _resolve_chart_start_date(history: pd.DataFrame, config_label: str) -> pd.Timestamp | None:
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
def load_live_benchmark_history():
    if not BENCHMARK_HISTORY_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {BENCHMARK_HISTORY_PATH}")

    df = pd.read_parquet(BENCHMARK_HISTORY_PATH)
    df["Data"] = pd.to_datetime(df["Data"])
    for column in BENCHMARK_LABELS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.sort_values("Data").drop_duplicates(subset=["Data"], keep="last").reset_index(
        drop=True
    )


def build_live_benchmark_chart(chart_dates):
    chart_index = pd.DatetimeIndex(chart_dates).sort_values()
    warnings = []

    if chart_index.empty:
        return pd.DataFrame(index=chart_index), warnings

    try:
        benchmark_history = load_live_benchmark_history()
        benchmark_history = benchmark_history[
            benchmark_history["Data"].between(
                chart_index.min(), chart_index.max()
            )
        ]
        benchmark_history = benchmark_history.set_index("Data").sort_index()
        benchmark_series = {}

        for column, label in BENCHMARK_LABELS.items():
            if column not in benchmark_history.columns:
                continue

            series = benchmark_history[column].reindex(chart_index).ffill().dropna()
            if series.empty:
                continue

            if column == "cdi_rate_aa":
                daily_factor = (1 + (series / 100)) ** (1 / 252)
                benchmark_series[label] = 100 * (
                    daily_factor.cumprod() / daily_factor.iloc[0]
                )
            else:
                benchmark_series[label] = (series / series.iloc[0]) * 100

        benchmark_chart = pd.DataFrame(benchmark_series, index=chart_index)
        if benchmark_chart.empty:
            warnings.append(
                "Benchmarks carregados, mas sem interseção de datas com o período exibido."
            )
    except Exception as exc:
        warnings.append(f"Benchmarks indisponíveis no momento: {exc}")
        benchmark_chart = pd.DataFrame(index=chart_index)

    return benchmark_chart, warnings


def render_zoomed_line_chart(
    chart_df,
    series_name="Série",
    color_name="Legenda",
    highlight_best_worst=False,
    best_config_label=None,
    worst_config_label=None,
):
    if chart_df.empty:
        st.info("Não há dados suficientes para exibir o gráfico.")
        return

    value_min = float(chart_df.min().min())
    value_max = float(chart_df.max().max())
    padding = max((value_max - value_min) * 0.15, 0.5)
    y_domain = [value_min - padding, value_max + padding]

    index_name = chart_df.index.name or "Data"
    chart_data = chart_df.reset_index()
    if index_name != "Data":
        chart_data = chart_data.rename(columns={index_name: "Data"})
    chart_data = (
        chart_data.melt(id_vars="Data", var_name=color_name, value_name=series_name)
        .dropna(subset=[series_name])
    )

    if highlight_best_worst:
        available_configs = set(chart_data[color_name].dropna().unique())
        best_config = (
            best_config_label if best_config_label in available_configs else None
        )
        worst_config = (
            worst_config_label if worst_config_label in available_configs else None
        )

        chart_data["categoria_destaque"] = "Demais"
        if best_config is not None:
            chart_data.loc[
                chart_data[color_name] == best_config, "categoria_destaque"
            ] = "Melhor"
        if worst_config is not None:
            chart_data.loc[
                chart_data[color_name] == worst_config, "categoria_destaque"
            ] = "Pior"

        base_chart = alt.Chart(chart_data).encode(
            x=alt.X("Data:T", title=None),
            y=alt.Y(f"{series_name}:Q", title=None, scale=alt.Scale(domain=y_domain)),
            detail=alt.Detail(f"{color_name}:N"),
            tooltip=[
                alt.Tooltip("Data:T", title="Data"),
                alt.Tooltip(f"{color_name}:N", title="Série"),
                alt.Tooltip(
                    "categoria_destaque:N", title="Classificação no fim do período"
                ),
                alt.Tooltip(f"{series_name}:Q", title="Valor", format=".2f"),
            ],
        )

        demais_chart = (
            base_chart.transform_filter(alt.datum.categoria_destaque == "Demais")
            .mark_line(strokeWidth=1.5, opacity=0.55, color="#9ca3af")
        )

        pior_chart = (
            base_chart.transform_filter(alt.datum.categoria_destaque == "Pior")
            .mark_line(strokeWidth=3, color="#ef4444")
        )

        melhor_chart = (
            base_chart.transform_filter(alt.datum.categoria_destaque == "Melhor")
            .mark_line(strokeWidth=3, color="#22c55e")
        )

        legend_data = pd.DataFrame(
            {
                "categoria_destaque": ["Melhor", "Pior", "Demais"],
                "cor": ["#22c55e", "#ef4444", "#9ca3af"],
            }
        )

        legend = (
            alt.Chart(legend_data)
            .mark_point(opacity=0)
            .encode(
                color=alt.Color(
                    "categoria_destaque:N",
                    title=None,
                    scale=alt.Scale(
                        domain=["Melhor", "Pior", "Demais"],
                        range=["#22c55e", "#ef4444", "#9ca3af"],
                    ),
                    legend=alt.Legend(orient="right"),
                )
            )
        )

        chart = (demais_chart + pior_chart + melhor_chart + legend).properties(
            height=360
        )
    else:
        chart = (
            alt.Chart(chart_data)
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X("Data:T", title=None),
                y=alt.Y(
                    f"{series_name}:Q", title=None, scale=alt.Scale(domain=y_domain)
                ),
                color=alt.Color(f"{color_name}:N", title=None),
                tooltip=[
                    alt.Tooltip("Data:T", title="Data"),
                    alt.Tooltip(f"{color_name}:N", title="Série"),
                    alt.Tooltip(f"{series_name}:Q", title="Valor", format=".2f"),
                ],
            )
            .properties(height=360)
        )

    st.altair_chart(chart, width="stretch")


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
            effective_start_date = _get_effective_start_date(
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
                rebalance_dates = _compute_rebalance_dates(
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
                cagr = _compute_cagr(
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
        lambda row: _safe_ratio(row["cagr"], row["drawdown_maximo"]),
        axis=1,
    )
    simulation_summary["configuracao"] = simulation_summary.apply(
        _build_config_label, axis=1
    )

    simulation_history = pd.concat(history_frames, ignore_index=True)
    simulation_history["configuracao"] = simulation_history.apply(
        _build_config_label, axis=1
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
                lambda row: _safe_ratio(row["cagr_q1"], row["drawdown_q1"]),
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


def credits():
    st.sidebar.header("Sobre o Autor")
    st.sidebar.write("Este aplicativo foi criado por Gustavo Correa usando Streamlit.")
    st.sidebar.markdown(
        "Para mais informações, visite [meu github](https://github.com/gusascorrea) \
                        ou [meu linkedin](https://linkedin.com/in/gustavo-correa--)."
    )
    st.sidebar.markdown("---")


def credits_eng():
    st.sidebar.header("About the Author")
    st.sidebar.write("This application was created by XXXXXXX Correa using Streamlit.")
    st.sidebar.markdown(
        "For more information, visit [my github](https://github.com/gusascorrea) \
                        or [my linkedin](https://linkedin.com/in/gustavo-correa--)."
    )
    st.sidebar.markdown("---")


def homepage():
    st.header("Início")
    st.markdown("---")
    st.header("Objetivo")
    st.write(
        "Esta aplicação foi desenvolvida com o intuito de informar, capacitar e facilitar \
                o usuário a selecionar ações de forma eficiente e eficaz. A técnica abordada torna objetivo \
                o processo de decisão de compra e venda, com risco abaixo do risco médio do mercado (em nosso caso, o Ibovespa) e retornos \
                consistentemente superiores."
    )
    st.write(
        "**Aviso Legal: esta aplicação não possui caráter de recomendação de investimento, somente informativo.\
              A decisão de investimento deve ser tomada individualmente pelo usuário que utiliza dessas informações.**"
    )

    st.markdown("---")
    st.header("Estratégia")
    st.write(
        "A estratégia é baseada na geração de um ranking de ações que possuem seu valor de mercado \
             descontado com relação ao seu resultado operacional. Existem diversas métricas que nos fornecem essa informação.\
             As métricas utilizadas neste trabalho são:"
    )
    st.write(
        "1. Earnings Yield: Razão entre o lucro operacional (aqui representado pelo EBIT) e o valor de mercado da empresa (Enterprise Value)."
    )
    st.write("2. ROIC: Retorno sobre o capital investido.")
    st.write(
        "3. Magic Formula: Estratégia que combina o ranking Earnings Yield com o ranking ROIC."
    )
    st.write(
        "Com os métodos de ranking explicitados, compra-se em mesma proporção um conjunto de **N** ações que se encontram\
              no topo desse ranking e que possuam Margem EBIT positiva e uma liquidez mínima."
    )
    st.write(
        "A cada período de balanceamento, vende-se as ações que saíram da lista e compra-se as que entraram com o valor de venda."
    )
    st.write(
        "Os períodos de balanceamentos mais comuns são o mensal, trimestral (na frequência de divulgação dos balanços)\
              e anual (na frequência de divulgação dos balanços mais completos)."
    )


def homepage_eng():
    st.header("Home")
    st.markdown("---")
    st.header("Goal")
    st.write(
        "This application was developed with the intention of informing, \
                enabling and facilitating the user to select stocks in a efficient and effective way. \
                The technique used in this application is to generate a ranking of stocks that have their market value \
                discounted with respect to their operational results. There are several metrics that provide this information. \
                The metrics used in this work are:"
    )
    st.write(
        "1. Earnings Yield: The ratio between the operational profit (here represented by EBIT) and the market value of the company (Enterprise Value)."
    )
    st.write("2. ROIC: Return on Invested Capital.")
    st.write(
        "3. Magic Formula: Strategy that combines the ranking Earnings Yield with the ranking ROIC."
    )
    st.write(
        "With the ranking methods explicitly stated, buy-in in the same proportion a set of **N** stocks that are at the top of this ranking \
              and that have a positive Margem EBIT and a minimum liquidity."
    )


def study():
    st.header("Estudo")
    st.markdown("---")
    st.write(
        "Este trabalho possui base em um estudo realizado no trabalho de \
             conclusão do Curso de Graduação em Ciências Econômicas na Universidade Federal do Rio Grande do Sul\
             do Economista Gabriel Roman."
    )

    st.write(
        "O trabalho de referência possui também estratégias de dois fatores que não foram contempladas\
             neste trabalho pelo fato de atualmente não existirem fontes estáveis, confiáveis e gratuitas de dados de\
             cotações em tempo real. Quando houverem melhores soluções, contemplaremos as demais estratégias abordadas\
             no trabalho de conclusão de curso."
    )

    st.markdown("---")
    st.header("Resultados")

    st.subheader("Retornos Anuais")
    st.image("images/retornos_RomanGabriel.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Fonte: ROMAN, Gabriel. 2021.")

    st.subheader("Volatilidade Anual")
    st.image("images/volatilidade_RomanGabriel.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Fonte: ROMAN, Gabriel. 2021.")

    st.subheader("Dados de Performance")
    st.image("images/performance_RomanGabriel.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Fonte: ROMAN, Gabriel. 2021.")

    st.write("CAGR: retorno anual composto")
    st.write(
        "Índice de Sharpe: comparação entre o risco/retorno do portfolio com a taxa livre de risco (SELIC)"
    )

    st.write(
        "Rolling-Year-Win: tem como objetivo\
            comparar o desempenho da estratégia com o mercado em janelas móveis e identificar\
            qual o percentual de tempo em que as carteiras performaram acima do mercado nos\
            mesmos períodos"
    )

    st.subheader(
        "Retorno acumulado para R$100,00 da Estratégia Earnings Yield (2000 - 2020)"
    )
    st.image("images/ey_ibov.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Fonte: ROMAN, Gabriel. 2021.")

    st.subheader(
        "Comparação entre balanceamento anual e trimestral para estratégia Earnings Yield"
    )
    st.image("images/balanceamento.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Fonte: ROMAN, Gabriel. 2021.")


def live_study():
    st.header("Estudo em Tempo Real")
    st.markdown("---")
    st.write(
        "Esta seção replica a ideia do estudo histórico, mas com base na composição das "
        "carteiras e nas cotações mais recentes disponíveis no projeto."
    )
    st.write(
        "A análise considera a combinação completa de parâmetros: estratégia, volume "
        "mínimo, quantidade de ativos na carteira e frequência de realocação."
    )

    try:
        analysis = build_live_reallocation_analysis()
    except FileNotFoundError as exc:
        st.error(str(exc))
        return

    summary = analysis["simulation_summary"]
    history = analysis["simulation_history"]
    configuration_summary = analysis["configuration_summary"]

    if summary.empty or history.empty:
        st.warning("Não há dados suficientes para montar o estudo em tempo real.")
        return

    best_configuration = configuration_summary.iloc[0]
    worst_configuration = configuration_summary.sort_values(
        [
            "razao_q1_cagr_q1_drawdown",
            "cagr_q1",
            "drawdown_q1",
            "vitorias_por_periodo",
        ],
        ascending=[True, True, True, True],
    ).iloc[0]

    st.markdown("---")
    st.header("Resultados")

    col1, col2, col3 = st.columns(3)
    col1.metric("Configurações avaliadas", int(configuration_summary.shape[0]))
    col2.metric(
        "Último dia com cotação",
        pd.Timestamp(analysis["latest_quote_date"]).strftime("%d/%m/%Y"),
    )
    col3.metric(
        "Datas iniciais testadas",
        len(analysis["all_requested_start_dates"]),
    )

    st.subheader("Dados de Performance")
    st.write("Melhor configuração consolidada")
    st.dataframe(
        _prepare_snake_case_table(best_configuration.to_frame(name="valor")),
        width="stretch",
    )

    st.write("Pior configuração consolidada")
    st.dataframe(
        _prepare_snake_case_table(worst_configuration.to_frame(name="valor")),
        width="stretch",
    )

    st.write("Top 10 configurações por Q1 de CAGR / Q1 de drawdown")
    st.dataframe(
        _prepare_snake_case_table(
            configuration_summary[
                [
                    "Estratégia",
                    "Volume Mínimo",
                    "Ativos na Carteira",
                    "frequencia_realocacao",
                    "razao_q1_cagr_q1_drawdown",
                    "cagr_q1",
                    "drawdown_q1",
                    "cagr_medio",
                    "volatilidade_media",
                    "drawdown_medio",
                    "data_inicio_maior_retorno",
                    "data_inicio_maior_drawdown",
                    "vitorias_por_periodo",
                    "trocas_totais",
                ]
            ].head(10)
        ),
        width="stretch",
    )

    st.write("CAGR: retorno anual composto")
    st.write(
        "Volatilidade anualizada: dispersão dos retornos diários da carteira "
        "trazidos para uma base anual."
    )
    st.write(
        "Drawdown máximo: maior queda acumulada observada desde o pico até o vale "
        "dentro da janela analisada."
    )

    st.subheader("Retorno Acumulado para R$100,00")
    best_label = best_configuration["configuracao"]
    worst_label = worst_configuration["configuracao"]
    selected_histories = []
    for config_label in [best_label, worst_label]:
        chart_start_date = _resolve_chart_start_date(history, config_label)
        if chart_start_date is None:
            continue
        selected_histories.append(
            history[
                (history["configuracao"] == config_label)
                & (history["data_inicio_solicitada"] == chart_start_date)
            ].copy()
        )

    selected_history = pd.concat(selected_histories, ignore_index=True)
    selected_history["valor_base_100"] = (
        selected_history["valor_carteira"] / INITIAL_CAPITAL
    ) * 100
    line_chart = selected_history.pivot_table(
        index="Data",
        columns="configuracao",
        values="valor_base_100",
        aggfunc="last",
    )
    benchmark_chart, benchmark_warnings = build_live_benchmark_chart(line_chart.index)
    render_zoomed_line_chart(
        line_chart.join(benchmark_chart, how="left"),
        series_name="Valor Base 100",
        color_name="Configuração",
    )
    for warning in benchmark_warnings:
        st.caption(warning)

    st.subheader("Todas as Configurações Consolidadas")
    st.dataframe(
        _prepare_snake_case_table(configuration_summary),
        width="stretch",
    )

    st.subheader("Performance de Todas as Configurações com Início em 12/03/2026")
    start_date_history = history[
        history["data_inicio_solicitada"] == LIVE_ANALYSIS_START
    ].copy()
    start_date_history["valor_base_100"] = (
        start_date_history["valor_carteira"] / INITIAL_CAPITAL
    ) * 100
    start_date_chart = start_date_history.pivot_table(
        index="Data",
        columns="configuracao",
        values="valor_base_100",
        aggfunc="last",
    )
    render_zoomed_line_chart(
        start_date_chart,
        series_name="Valor Base 100",
        color_name="Configuração",
        highlight_best_worst=True,
        best_config_label=best_label,
        worst_config_label=worst_label,
    )


def study_eng():
    st.header("Study")
    st.markdown("---")
    st.write(
        "This work was based on a real study done in the work of \
             graduation in Economics at the Federal University of Rio Grande do Sul \
             of Economist Gabriel Roman."
    )

    st.write(
        "The reference work also has strategies of two factors that were not considered\
             in this work because currently there are no reliable, free and gratuitable data sources\
             of real-time quotes. When there are better solutions, we will consider the other strategies\
             discussed in the work of graduation."
    )

    st.markdown("---")
    st.header("Results")

    st.subheader("Annual Returns")
    st.image("images/retornos_RomanGabriel.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Source: ROMAN, Gabriel. 2021.")

    st.subheader("Annual Volatility")
    st.image("images/volatilidade_RomanGabriel.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Source: ROMAN, Gabriel. 2021.")

    st.subheader("Performance Data")
    st.image("images/performance_RomanGabriel.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Source: ROMAN, Gabriel. 2021.")

    st.write("CAGR: cumulative annual return")
    st.write(
        "Sharpe Index: compares the portfolio risk/return with the risk free rate (SELIC)"
    )
    st.write(
        "Rolling-Year-Win: aims to compare the performance of the strategy with the market in moving windows and identify the percentage of time in which the portfolios performed above the market in the same periods"
    )
    st.write(
        "Retorno acumulado para R$100, 00 da Estratégia Earnings Yield (2000 - 2020)"
    )
    st.image("images/ey_ibov.PNG")
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write("Source: ROMAN, Gabriel. 2021.")


def stock_list():
    st.header("Lista de Ações")
    st.markdown("---")

    st.subheader("Estratégia")

    estrategia = st.selectbox(
        "Selecione a estratégia", ["Earnings Yield", "Magic Formula", "ROIC"]
    )

    ativos_na_carteira = st.number_input("Quantidade de ativos na carteira:", value=20)

    st.markdown("---")

    st.subheader("Investimento")

    valor_total = st.number_input(
        "Adicione aqui o valor que deseja investir na estratégia:", value=0
    )

    st.markdown("---")

    st.subheader("Filtro de Liquidez")

    vol_min = st.number_input(
        "Digite o volume financeiro mínimo dos útimos 2 meses:", value=400000
    )

    st.markdown("---")

    # Removendo financeiras menos WIZC3
    df = fd.get_resultado_raw()

    financeiras = _load_financial_sector_tickers()

    df = df[~df.index.isin(financeiras)]

    # Removendo ADRs e BDRs

    df = df[
        ~(
            df.index.astype(str).str.contains("33")
            | df.index.astype(str).str.contains("34")
        )
    ]

    # Mantendo Margem Ebit Apenas Maior que Zero

    df = df.loc[df["Mrg Ebit"] > 0]

    # Mantendo Volume Financeiro Apenas Maior que 500k a cada 2 meses

    df = df.loc[df["Liq.2meses"] >= vol_min]
    # st.write(df['Liq.2meses'].unique())

    # Step 1: Extract the first 4 characters from the index
    df["First4Chars"] = df.index.str[:4]

    # Step 2: Determine duplicates in the first 4 characters of the index
    duplicates = df.duplicated(subset="First4Chars", keep=False)

    # Step 3: For each group of duplicates, identify the row with the maximum 'Liq.2meses' value
    max_values = df.groupby("First4Chars")["Liq.2meses"].transform("max")

    # Step 4: Filter the DataFrame to keep only the rows with the maximum 'Liq.2meses' value for each group of duplicates
    filtered_df = df[(~duplicates) | (df["Liq.2meses"] == max_values)]

    # Drop the temporary column used for grouping
    filtered_df = filtered_df.drop(columns="First4Chars")
    filtered_df["Earnings Yield"] = round(1 / filtered_df["EV/EBIT"] * 100, 1)
    filtered_df["ROIC"] = round(filtered_df["ROIC"] * 100, 1)

    if estrategia == "Earnings Yield":
        st.subheader("Earnings Yield")
        st.write(f"Primeiros {ativos_na_carteira} ativos")
        sorted_df = filtered_df.sort_values(
            by=["EV/EBIT", "Liq.2meses"], ascending=[True, False]
        )

        sorted_df["Quantidade"] = round(
            (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
        )

        sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

        sorted_df = sorted_df[
            ["Cotação", "Earnings Yield", "Liq.2meses", "Quantidade", "Valor"]
        ].head(ativos_na_carteira)

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc["Total"] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc[
            "Total", ["Cotação", "Earnings Yield", "Liq.2meses", "Quantidade"]
        ] = "-"

    elif estrategia == "Magic Formula":
        st.subheader("Magic Formula")
        st.write(f"Primeiros {ativos_na_carteira} ativos")
        filtered_df["Ranking_Earning_Yield"] = filtered_df["Earnings Yield"].rank(
            ascending=False
        )
        filtered_df["Ranking_ROIC"] = filtered_df["ROIC"].rank(ascending=False)
        filtered_df["Magic Formula"] = (
            filtered_df["Ranking_Earning_Yield"] + filtered_df["Ranking_ROIC"]
        )

        sorted_df = filtered_df.sort_values(
            by=["Magic Formula", "Liq.2meses"], ascending=[True, False]
        )

        sorted_df["Quantidade"] = round(
            (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
        )

        sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

        sorted_df = sorted_df[
            [
                "Cotação",
                "Earnings Yield",
                "ROIC",
                "Magic Formula",
                "Liq.2meses",
                "Quantidade",
                "Valor",
            ]
        ].head(ativos_na_carteira)
        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc["Total"] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc[
            "Total",
            [
                "Cotação",
                "Earnings Yield",
                "ROIC",
                "Magic Formula",
                "Liq.2meses",
                "Quantidade",
            ],
        ] = "-"

    elif estrategia == "ROIC":
        st.subheader("ROIC")
        st.write(f"Primeiros {ativos_na_carteira} ativos")
        sorted_df = filtered_df.sort_values(
            by=["ROIC", "Liq.2meses"], ascending=[False, False]
        )

        sorted_df["Quantidade"] = round(
            (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
        )

        sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

        sorted_df = sorted_df[
            ["Cotação", "ROIC", "Liq.2meses", "Quantidade", "Valor"]
        ].head(ativos_na_carteira)

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc["Total"] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc["Total", ["Cotação", "ROIC", "Liq.2meses", "Quantidade"]] = "-"

    sorted_df = _prepare_snake_case_table(sorted_df)
    st.table(sorted_df.style.format(precision=2))


def stock_list_eng():
    st.header("Stock List")
    st.markdown("---")

    st.subheader("Strategy")

    estrategia = st.selectbox(
        "Select the strategy", ["Earnings Yield", "Magic Formula", "ROIC"]
    )

    ativos_na_carteira = st.number_input(
        "Enter the number of assets in your portfolio:", value=20
    )

    st.markdown("---")

    st.subheader("Investment")

    valor_total = st.number_input(
        "Enter the amount you want to invest in the strategy:", value=0
    )

    st.markdown("---")

    st.subheader("Liquidity Filter")

    vol_min = st.number_input(
        "Enter the minimum financial volume of the last 2 months:", value=400000
    )

    st.markdown("---")

    # Removendo financeiras menos WIZC3
    df = fd.get_resultado_raw()

    financeiras = _load_financial_sector_tickers()

    df = df[~df.index.isin(financeiras)]

    # Removendo ADRs e BDRs

    df = df[
        ~(
            df.index.astype(str).str.contains("33")
            | df.index.astype(str).str.contains("34")
        )
    ]

    # Mantendo Margem Ebit Apenas Maior que Zero

    df = df.loc[df["Mrg Ebit"] > 0]

    # Mantendo Volume Financeiro Apenas Maior que 500k a cada 2 meses

    df = df.loc[df["Liq.2meses"] >= vol_min]
    # st.write(df['Liq.2meses'].unique())

    # Step 1: Extract the first 4 characters from the index
    df["First4Chars"] = df.index.str[:4]

    # Step 2: Determine duplicates in the first 4 characters of the index
    duplicates = df.duplicated(subset="First4Chars", keep=False)

    # Step 3: For each group of duplicates, identify the row with the maximum 'Liq.2meses' value
    max_values = df.groupby("First4Chars")["Liq.2meses"].transform("max")

    # Step 4: Filter the DataFrame to keep only the rows with the maximum 'Liq.2meses' value for each group of duplicates
    filtered_df = df[(~duplicates) | (df["Liq.2meses"] == max_values)]

    # Drop the temporary column used for grouping
    filtered_df = filtered_df.drop(columns="First4Chars")
    filtered_df["Earnings Yield"] = round(1 / filtered_df["EV/EBIT"] * 100, 1)
    filtered_df["ROIC"] = round(filtered_df["ROIC"] * 100, 1)

    if estrategia == "Earnings Yield":
        st.subheader("Earnings Yield")
        st.write(f"First {ativos_na_carteira} assets")
        sorted_df = filtered_df.sort_values(
            by=["EV/EBIT", "Liq.2meses"], ascending=[True, False]
        )

        sorted_df["Quantidade"] = round(
            (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
        )

        sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

        sorted_df = sorted_df[
            ["Cotação", "Earnings Yield", "Liq.2meses", "Quantidade", "Valor"]
        ].head(ativos_na_carteira)

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc["Total"] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc[
            "Total", ["Cotação", "Earnings Yield", "Liq.2meses", "Quantidade"]
        ] = "-"

    elif estrategia == "Magic Formula":
        st.subheader("Magic Formula")
        st.write(f"First {ativos_na_carteira} assets")
        filtered_df["Ranking_Earning_Yield"] = filtered_df["Earnings Yield"].rank(
            ascending=False
        )
        filtered_df["Ranking_ROIC"] = filtered_df["ROIC"].rank(ascending=False)
        filtered_df["Magic Formula"] = (
            filtered_df["Ranking_Earning_Yield"] + filtered_df["Ranking_ROIC"]
        )

        sorted_df = filtered_df.sort_values(
            by=["Magic Formula", "Liq.2meses"], ascending=[True, False]
        )
        sorted_df["Quantidade"] = round(
            (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
        )

        sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

        sorted_df = sorted_df[
            [
                "Cotação",
                "Earnings Yield",
                "ROIC",
                "Magic Formula",
                "Liq.2meses",
                "Quantidade",
                "Valor",
            ]
        ].head(ativos_na_carteira)
        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc["Total"] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc[
            "Total",
            [
                "Cotação",
                "Earnings Yield",
                "ROIC",
                "Magic Formula",
                "Liq.2meses",
                "Quantidade",
            ],
        ] = "-"

    elif estrategia == "ROIC":
        st.subheader("ROIC")
        st.write(f"First {ativos_na_carteira} assets")
        sorted_df = filtered_df.sort_values(
            by=["ROIC", "Liq.2meses"], ascending=[False, False]
        )

        sorted_df["Quantidade"] = round(
            (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
        )

        sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

        sorted_df = sorted_df[
            ["Cotação", "ROIC", "Liq.2meses", "Quantidade", "Valor"]
        ].head(ativos_na_carteira)

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc["Total"] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc["Total", ["Cotação", "ROIC", "Liq.2meses", "Quantidade"]] = "-"

    sorted_df = _prepare_snake_case_table(sorted_df)
    st.table(sorted_df.style.format(precision=2))


def rebalancing():
    st.header("Rebalanceamento")
    st.markdown("---")
    st.write("Escrever Rebalanceamento")


def references():
    st.sidebar.markdown("---")
    st.sidebar.subheader("Referências:")
    st.sidebar.write(
        "ROMAN, Gabriel. Avaliação da Eficiência da Magic Formula e de Estratégias de Value Investing para o Mercado Brasileiro. Porto Alegre: UFRGS, 2021."
    )
    st.sidebar.download_button(
        label="Download PDF",
        data="references/TCC Magic Formula.pdf",
        file_name="TCC_Magic_Formula.pdf",
        mime="application/pdf",
    )
    st.sidebar.markdown(
        "Magic Formula para o mercado americano: [Site Magic Formula](https://www.magicformulainvesting.com/)"
    )
    st.sidebar.markdown(
        "Fonte de dados: [API do site Fundamentus](https://pypi.org/project/fundamentus/)"
    )


def references_eng():
    st.sidebar.markdown("---")
    st.sidebar.subheader("References:")
    st.sidebar.write(
        "ROMAN, Gabriel. Evaluation of the Efficiency of the Magic Formula and Value Investing Strategies for the Brazilian Market. Porto Alegre: UFRGS, 2021."
    )
    st.sidebar.download_button(
        label="Download PDF",
        data="references/TCC Magic Formula.pdf",
        file_name="TCC_Magic_Formula.pdf",
        mime="application/pdf",
    )
    st.sidebar.markdown(
        "Magic Formula for the US market: [Site Magic Formula](https://www.magicformulainvesting.com/)"
    )
    st.sidebar.markdown(
        "Source of data: [API of Fundamentus website](https://pypi.org/project/fundamentus/)"
    )


def main():

    # Lançado oficialmente 03/04/2024
    # language = st.sidebar.radio("Select Language", ['Português', 'English'])
    language = "Português"

    if language == "Português":
        credits()
        st.title("Value Investing")
        st.markdown("---")

        painel = st.sidebar.radio(
            "Painel",
            ["Início", "Estudo", "Estudo em Tempo Real", "Lista de Ações"],
        )

        if painel == "Início":
            homepage()
        elif painel == "Estudo":
            study()
        elif painel == "Estudo em Tempo Real":
            live_study()
        elif painel == "Lista de Ações":
            stock_list()
        elif painel == "Rebalanceamento":
            rebalancing()

        references()
    elif language == "English":
        credits_eng()
        st.title("Value Investing")
        st.markdown("---")

        painel = st.sidebar.radio("Panel", ["Home", "Study", "Stock List"])

        if painel == "Home":
            homepage_eng()
        elif painel == "Study":
            study_eng()
        elif painel == "Stock List":
            stock_list_eng()
        elif painel == "Rebalanceamento":
            rebalancing_eng()

        references_eng()


if __name__ == "__main__":
    main()
