import pandas as pd

from app.config import BENCHMARK_LABELS
from app.repositories.history_repository import load_live_benchmark_history


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
