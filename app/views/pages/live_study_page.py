import pandas as pd
import streamlit as st

from app.controllers.live_study_controller import get_live_study_view_model
from app.services.benchmark_service import build_live_benchmark_chart
from app.services.reallocation_service import resolve_chart_start_date
from app.utils.formatting import prepare_snake_case_table
from app.views.components.charts import render_zoomed_line_chart
from shared.config import INITIAL_CAPITAL, LIVE_ANALYSIS_START


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

    try:
        view_model = get_live_study_view_model()
    except FileNotFoundError as exc:
        st.error(str(exc))
        return

    analysis = view_model["analysis"]
    summary = analysis["simulation_summary"]
    history = analysis["simulation_history"]
    configuration_summary = analysis["configuration_summary"]

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

    st.write("Top 10 configurações por Q1 de CAGR / Q1 de drawdown")
    st.dataframe(
        prepare_snake_case_table(
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
        "Volatilidade anualizada: dispersão dos retornos diários da carteira trazidos "
        "para uma base anual."
    )
    st.write(
        "Drawdown máximo: maior queda acumulada observada desde o pico até o vale "
        "dentro da janela analisada."
    )

    _render_best_vs_worst_chart(history, best_configuration, worst_configuration)

    st.subheader("Todas as Configurações Consolidadas")
    st.dataframe(prepare_snake_case_table(configuration_summary), width="stretch")

    _render_all_configurations_chart(history, best_configuration, worst_configuration)


def _render_best_vs_worst_chart(history, best_configuration, worst_configuration):
    st.subheader("Retorno Acumulado para R$100,00")
    best_label = best_configuration["configuracao"]
    worst_label = worst_configuration["configuracao"]

    selected_histories = []
    for config_label in [best_label, worst_label]:
        chart_start_date = resolve_chart_start_date(history, config_label)
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


def _render_all_configurations_chart(history, best_configuration, worst_configuration):
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
        best_config_label=best_configuration["configuracao"],
        worst_config_label=worst_configuration["configuracao"],
    )
