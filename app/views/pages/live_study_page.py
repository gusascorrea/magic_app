import pandas as pd
import streamlit as st

from app.controllers.live_study_controller import (
    DEFAULT_COMPARISON_KEY,
    get_live_study_comparison_options,
    get_live_study_view_model,
)
from app.services.benchmark_service import build_live_benchmark_chart
from app.services.reallocation_service import resolve_chart_start_date
from app.utils.formatting import prepare_snake_case_table
from app.views.components.charts import render_zoomed_line_chart
from shared.config import INITIAL_CAPITAL


def render_live_study_page():
    st.header("Estudo em Tempo Real")
    st.markdown("---")
    st.write(
        "Esta seção replica a ideia do estudo histórico, mas com base na composição "
        "das carteiras e nas cotações mais recentes disponíveis no projeto."
    )
    st.write(
        "A análise considera a combinação completa de parâmetros: estratégia, volume "
        "mínimo, quantidade de ativos na carteira e frequência de realocação."
    )

    comparison_options = get_live_study_comparison_options()
    selected_comparison_key = st.selectbox(
        "Parâmetro principal de comparação",
        options=list(comparison_options.keys()),
        index=list(comparison_options.keys()).index(DEFAULT_COMPARISON_KEY),
        format_func=lambda key: comparison_options[key]["label"],
        help=(
            "Escolha o critério que define a melhor e a pior configuração, "
            "além da ordenação do ranking consolidado."
        ),
    )
    st.caption(comparison_options[selected_comparison_key]["description"])

    try:
        view_model = get_live_study_view_model(selected_comparison_key)
    except FileNotFoundError as exc:
        st.error(str(exc))
        return

    analysis = view_model["analysis"]
    summary = analysis["simulation_summary"]
    history = analysis["simulation_history"]
    configuration_summary = view_model["configuration_summary"]
    comparison = view_model["comparison"]

    if summary.empty or history.empty:
        st.warning("Não há dados suficientes para montar o estudo em tempo real.")
        return

    best_configuration = view_model["best_configuration"]
    worst_configuration = view_model["worst_configuration"]

    st.markdown("---")
    st.header("Resultados")

    col1, col2, col3 = st.columns(3)
    col1.metric("Configurações avaliadas", int(configuration_summary.shape[0]))
    col2.metric(
        "Último dia com cotação",
        pd.Timestamp(analysis["latest_quote_date"]).strftime("%d/%m/%Y"),
    )
    col3.metric("Datas iniciais testadas", len(analysis["all_requested_start_dates"]))

    st.subheader("Dados de Performance")
    st.write("Melhor configuração consolidada")
    st.dataframe(
        prepare_snake_case_table(best_configuration.to_frame(name="valor")),
        width="stretch",
    )

    st.write("Pior configuração consolidada")
    st.dataframe(
        prepare_snake_case_table(worst_configuration.to_frame(name="valor")),
        width="stretch",
    )

    st.write(f"Top 10 configurações por {comparison['label']}")
    st.dataframe(
        prepare_snake_case_table(
            configuration_summary[
                [
                    "Estratégia",
                    "Volume Mínimo",
                    "Ativos na Carteira",
                    "frequencia_realocacao",
                    "razao_q1_cagr_q1_drawdown",
                    "razao_q1_cagr_drawdown_pior",
                    "razao_q1_retorno_q1_drawdown",
                    "razao_q1_retorno_drawdown_pior",
                    "retorno_q1",
                    "retorno_medio",
                    "cagr_q1",
                    "cagr_medio",
                    "drawdown_q1",
                    "drawdown_pior",
                    "drawdown_medio",
                    "volatilidade_media",
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
        "Volatilidade anualizada: dispersão dos retornos diários da carteira trazidos "
        "para uma base anual."
    )
    st.write(
        "Drawdown máximo: maior queda acumulada observada desde o pico até o vale "
        "dentro da janela analisada."
    )

    selected_start_date = _render_start_date_selector(history, best_configuration)

    _render_best_vs_worst_chart(
        history,
        best_configuration,
        worst_configuration,
        comparison["label"],
        selected_start_date,
    )

    st.subheader("Todas as Configurações Consolidadas")
    st.dataframe(prepare_snake_case_table(configuration_summary), width="stretch")

    _render_all_configurations_chart(
        history,
        best_configuration,
        worst_configuration,
        selected_start_date,
    )


def _render_start_date_selector(history, best_configuration):
    available_start_dates = pd.DatetimeIndex(
        history["data_inicio_solicitada"].dropna().sort_values().unique()
    )

    if available_start_dates.empty:
        return None

    default_chart_start = resolve_chart_start_date(
        history, best_configuration["configuracao"]
    )
    if default_chart_start is None:
        default_chart_start = available_start_dates.min()

    selected_chart_start = st.date_input(
        "Data inicial do gráfico comparativo",
        value=default_chart_start.date(),
        min_value=available_start_dates.min().date(),
        max_value=available_start_dates.max().date(),
        help=(
            "Escolha a data inicial da simulação exibida. As carteiras e benchmarks "
            "passam a ser carregados a partir dessa data."
        ),
    )
    effective_chart_start = available_start_dates[
        available_start_dates >= pd.Timestamp(selected_chart_start)
    ][0]
    if effective_chart_start.date() != selected_chart_start:
        st.caption(
            "A data escolhida foi ajustada para o próximo pregão disponível: "
            f"{effective_chart_start.strftime('%d/%m/%Y')}."
        )

    return effective_chart_start


def _render_best_vs_worst_chart(
    history,
    best_configuration,
    worst_configuration,
    comparison_label,
    selected_start_date,
):
    st.subheader("Retorno Acumulado para R$100,00 com Benchmarks")
    best_label = best_configuration["configuracao"]
    worst_label = worst_configuration["configuracao"]

    if selected_start_date is None:
        st.info("Não há dados suficientes para exibir o comparativo com benchmarks.")
        return

    selected_histories = []
    for config_label in [best_label, worst_label]:
        config_history = history[
            (history["configuracao"] == config_label)
            & (history["data_inicio_solicitada"] == selected_start_date)
        ].copy()
        if not config_history.empty:
            selected_histories.append(config_history)

    if not selected_histories:
        st.info(
            "Não há dados suficientes para exibir melhor e pior configuração na data inicial selecionada."
        )
        return

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
    st.caption(
        f"Ranking atual definido por {comparison_label}. Melhor e pior configuração abaixo seguem esse critério para início em {selected_start_date.strftime('%d/%m/%Y')}."
    )
    render_zoomed_line_chart(
        line_chart.join(benchmark_chart, how="left"),
        series_name="Valor Base 100",
        color_name="Configuração",
    )
    for warning in benchmark_warnings:
        st.caption(warning)


def _render_all_configurations_chart(
    history,
    best_configuration,
    worst_configuration,
    selected_start_date,
):
    if selected_start_date is None:
        st.info("Não há dados suficientes para exibir a performance das configurações.")
        return

    st.subheader("Performance de Todas as Configurações")
    start_date_history = history[
        history["data_inicio_solicitada"] == selected_start_date
    ].copy()
    if start_date_history.empty:
        st.info("Não há configurações disponíveis para a data inicial selecionada.")
        return

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
        best_config_label=best_configuration["configuracao"],
        worst_config_label=worst_configuration["configuracao"],
    )
