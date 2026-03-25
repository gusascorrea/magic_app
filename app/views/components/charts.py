import altair as alt
import pandas as pd
import streamlit as st


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
        best_config = best_config_label if best_config_label in available_configs else None
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

        chart = (demais_chart + pior_chart + melhor_chart + legend).properties(height=360)
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
