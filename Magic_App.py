import altair as alt
import fundamentus as fd
import numpy as np
import pandas as pd
import streamlit as st
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


def _prepare_streamlit_dataframe(df):
    prepared = df.copy()
    for column in prepared.columns:
        if pd.api.types.is_object_dtype(prepared[column]):
            prepared[column] = prepared[column].astype("string")
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
    df["commit_committed_at"] = pd.to_datetime(df["commit_committed_at"], utc=True)
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
    if drawdown_value == 0 and return_value > 0:
        return float("inf")
    if drawdown_value == 0:
        return 0.0
    return return_value / abs(drawdown_value)


def _build_config_label(row):
    return (
        f"{row['Estratégia']} | vol {int(row['Volume Mínimo'])}"
        f" | {int(row['Ativos na Carteira'])} ativos"
        f" | {row['frequencia_realocacao']}"
    )


@st.cache_data(show_spinner=False)
def load_live_benchmark_history():
    if not BENCHMARK_HISTORY_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {BENCHMARK_HISTORY_PATH}")

    df = pd.read_parquet(BENCHMARK_HISTORY_PATH)
    df["Data"] = pd.to_datetime(df["Data"])
    for column in ["ibov_close", "cdi_rate_aa"]:
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

        ibov_series = (
            benchmark_history["ibov_close"]
            .reindex(chart_index)
            .ffill()
            .dropna()
        )
        if not ibov_series.empty:
            benchmark_series["IBOV"] = (ibov_series / ibov_series.iloc[0]) * 100

        cdi_series = (
            benchmark_history["cdi_rate_aa"]
            .reindex(chart_index)
            .ffill()
            .dropna()
        )
        if not cdi_series.empty:
            daily_factor = (1 + (cdi_series / 100)) ** (1 / 252)
            benchmark_series["CDI"] = 100 * (
                daily_factor.cumprod() / daily_factor.iloc[0]
            )

        benchmark_chart = pd.DataFrame(benchmark_series, index=chart_index)
        if benchmark_chart.empty:
            warnings.append(
                "Benchmarks carregados, mas sem interseção de datas com o período exibido."
            )
    except Exception as exc:
        warnings.append(f"Benchmarks indisponíveis no momento: {exc}")
        benchmark_chart = pd.DataFrame(index=chart_index)

    return benchmark_chart, warnings


def render_zoomed_line_chart(chart_df, series_name="Série", color_name="Legenda"):
    if chart_df.empty:
        st.info("Não há dados suficientes para exibir o gráfico.")
        return

    value_min = float(chart_df.min().min())
    value_max = float(chart_df.max().max())
    padding = max((value_max - value_min) * 0.15, 0.5)
    y_domain = [value_min - padding, value_max + padding]

    chart_data = (
        chart_df.reset_index()
        .melt(id_vars="Data", var_name=color_name, value_name=series_name)
        .dropna(subset=[series_name])
    )

    chart = (
        alt.Chart(chart_data)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("Data:T", title=None),
            y=alt.Y(f"{series_name}:Q", title=None, scale=alt.Scale(domain=y_domain)),
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
    requested_start_dates = pd.date_range(
        LIVE_ANALYSIS_START, max(available_snapshot_dates), freq="D"
    )
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
                history_frames.append(history)

                simulation_rows.append(
                    {
                        **key_dict,
                        "data_inicio_solicitada": requested_start_date,
                        "data_inicio_efetiva": effective_start_date,
                        "data_fim": history["Data"].max(),
                        "frequencia_realocacao": frequency_name,
                        "retorno_total": history["retorno_acumulado"].iloc[-1],
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
    simulation_summary["razao_retorno_drawdown"] = simulation_summary.apply(
        lambda row: _safe_ratio(row["retorno_total"], row["drawdown_maximo"]),
        axis=1,
    )
    simulation_summary["configuracao"] = simulation_summary.apply(
        _build_config_label, axis=1
    )

    simulation_history = pd.concat(history_frames, ignore_index=True)
    simulation_history["configuracao"] = simulation_history.apply(
        _build_config_label, axis=1
    )

    best_return_by_period = simulation_summary.groupby("data_inicio_solicitada")[
        "retorno_total"
    ].transform("max")
    simulation_summary["venceu_periodo"] = (
        simulation_summary["retorno_total"] == best_return_by_period
    ).astype(int)

    configuration_summary = (
        simulation_summary.groupby(
            [
                "Estratégia",
                "Volume Mínimo",
                "Ativos na Carteira",
                "frequencia_realocacao",
                "configuracao",
            ],
            as_index=False,
        )
        .agg(
            retorno_medio=("retorno_total", "mean"),
            retorno_mediano=("retorno_total", "median"),
            retorno_min=("retorno_total", "min"),
            retorno_max=("retorno_total", "max"),
            drawdown_medio=("drawdown_maximo", "mean"),
            drawdown_pior=("drawdown_maximo", "min"),
            volatilidade_media=("volatilidade_anualizada", "mean"),
            razao_media_retorno_drawdown=("razao_retorno_drawdown", "mean"),
            vitorias_por_periodo=("venceu_periodo", "sum"),
            trocas_totais=("ativos_trocados_total", "sum"),
        )
        .sort_values(
            ["vitorias_por_periodo", "retorno_medio", "razao_media_retorno_drawdown"],
            ascending=[False, False, False],
        )
        .reset_index(drop=True)
    )

    return {
        "simulation_summary": simulation_summary,
        "simulation_history": simulation_history,
        "configuration_summary": configuration_summary,
        "available_snapshot_dates": available_snapshot_dates,
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
        ["retorno_medio", "razao_media_retorno_drawdown", "drawdown_pior"],
        ascending=[True, True, True],
    ).iloc[0]

    st.markdown("---")
    st.header("Resultados")

    col1, col2, col3 = st.columns(3)
    col1.metric("Configurações avaliadas", int(configuration_summary.shape[0]))
    col2.metric(
        "Último dia com cotação",
        pd.Timestamp(analysis["latest_quote_date"]).strftime("%d/%m/%Y"),
    )
    col3.metric("Datas iniciais testadas", len(analysis["requested_start_dates"]))

    st.subheader("Retornos do Período")
    returns_chart = (
        configuration_summary.nlargest(10, "retorno_medio")
        .set_index("configuracao")[["retorno_medio"]]
        .rename(columns={"retorno_medio": "Retorno Médio"})
    )
    st.bar_chart(returns_chart)

    st.subheader("Volatilidade Anualizada")
    volatility_chart = (
        configuration_summary.nsmallest(10, "volatilidade_media")
        .set_index("configuracao")[["volatilidade_media"]]
        .rename(columns={"volatilidade_media": "Volatilidade Média"})
    )
    st.bar_chart(volatility_chart)

    st.subheader("Dados de Performance")
    st.write("Melhor configuração consolidada")
    st.dataframe(
        _prepare_streamlit_dataframe(best_configuration.to_frame(name="valor")),
        width="stretch",
    )

    st.write("Pior configuração consolidada")
    st.dataframe(
        _prepare_streamlit_dataframe(worst_configuration.to_frame(name="valor")),
        width="stretch",
    )

    st.write("Top 10 configurações por retorno médio")
    st.dataframe(
        _prepare_streamlit_dataframe(
            configuration_summary[
                [
                    "Estratégia",
                    "Volume Mínimo",
                    "Ativos na Carteira",
                    "frequencia_realocacao",
                    "retorno_medio",
                    "volatilidade_media",
                    "drawdown_medio",
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
    selected_history = history[
        history["configuracao"].isin([best_label, worst_label])
    ].copy()
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

    st.subheader("Comparação entre Balanceamento Anual e Trimestral")
    comparison_base = best_configuration
    rebalancing_comparison = history[
        (history["Estratégia"] == comparison_base["Estratégia"])
        & (history["Volume Mínimo"] == comparison_base["Volume Mínimo"])
        & (history["Ativos na Carteira"] == comparison_base["Ativos na Carteira"])
        & (
            history["data_inicio_solicitada"]
            == summary[
                summary["configuracao"] == comparison_base["configuracao"]
            ]["data_inicio_solicitada"].min()
        )
        & (history["frequencia_realocacao"].isin(["anual", "trimestral"]))
    ].copy()
    rebalancing_comparison["valor_base_100"] = (
        rebalancing_comparison["valor_carteira"] / INITIAL_CAPITAL
    ) * 100
    comparison_chart = rebalancing_comparison.pivot_table(
        index="Data",
        columns="frequencia_realocacao",
        values="valor_base_100",
        aggfunc="last",
    )
    render_zoomed_line_chart(
        comparison_chart,
        series_name="Valor Base 100",
        color_name="Frequência",
    )

    if comparison_chart.nunique().max() <= 1:
        st.info(
            "Na janela atual, anual e trimestral ainda aparecem iguais porque não "
            "houve tempo suficiente para disparar um novo rebalanceamento."
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
