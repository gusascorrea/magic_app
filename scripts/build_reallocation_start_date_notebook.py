from pathlib import Path

import nbformat as nbf


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "analise_realocacao_por_data_inicial.ipynb"


def build_notebook() -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    cells = []

    cells.append(
        nbf.v4.new_markdown_cell(
            """# Analise de performance por data inicial e frequencia de realocacao

Este notebook compara a performance das carteiras do projeto considerando:

- data inicial solicitada a partir de `12/03/2026`;
- frequencias de realocacao `mensal`, `trimestral` e `anual`;
- metricas de `retorno total`, `drawdown maximo` e `trocas de ativos`.

## Fontes usadas

- `data/performance_committed_since_2026-03-10.parquet`: composicao historica das carteiras ja commitadas no projeto;
- `data/fundamentus_data.parquet`: historico de cotacoes e fundamentos por ativo;
- `references/TCC Magic Formula.pdf`: referencia conceitual para o trade-off entre permanencia maior na carteira e menor giro.

## Observacoes metodologicas

- O TCC destaca que um periodo maior de permanencia tende a reduzir giro e custos, enquanto o balanceamento trimestral pode capturar ajustes mais rapidamente.
- Como o historico de carteiras commitado vai ate `18/03/2026`, as entradas analisadas usam as composicoes disponiveis entre `12/03/2026` e `18/03/2026`.
- O mark-to-market vai ate a ultima cotacao disponivel em `fundamentus_data.parquet`.
- Para datas sem pregao, a data inicial efetiva e deslocada para o primeiro pregao com carteira disponivel.
- Nesta janela curta, e esperado que `mensal`, `trimestral` e `anual` coincidam, porque nao ha um novo gatilho de rebalanceamento antes do fim da amostra. Mesmo assim, o notebook deixa a simulacao pronta para janelas futuras mais longas.
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

plt.style.use("ggplot")
pd.set_option("display.max_columns", 50)
pd.set_option("display.float_format", lambda value: f"{value:,.4f}")

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")


def discover_repo_root() -> Path:
    candidates = [Path.cwd(), Path.cwd().parent]
    for candidate in candidates:
        if (candidate / "data").exists() and (candidate / "notebooks").exists():
            return candidate
    raise FileNotFoundError("Nao foi possivel localizar a raiz do repositorio.")


REPO_ROOT = discover_repo_root()
PERFORMANCE_PATH = REPO_ROOT / "data" / "performance_committed_since_2026-03-10.parquet"
FUNDAMENTUS_PATH = REPO_ROOT / "data" / "fundamentus_data.parquet"

PORTFOLIO_KEYS = ["Estrategia", "Volume Minimo", "Ativos na Carteira"]
SOURCE_PORTFOLIO_KEYS = ["Estratégia", "Volume Mínimo", "Ativos na Carteira"]
ROW_KEYS = SOURCE_PORTFOLIO_KEYS + ["Data", "papel"]
FREQUENCIES = {"mensal": 1, "trimestral": 3, "anual": 12}
INITIAL_CAPITAL = 100_000.0
REQUESTED_START = pd.Timestamp("2026-03-12")
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """def load_portfolio_history(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["Data"] = pd.to_datetime(df["Data"])
    df["commit_committed_at"] = pd.to_datetime(df["commit_committed_at"], utc=True)
    df = (
        df.sort_values("commit_committed_at")
        .drop_duplicates(subset=ROW_KEYS, keep="last")
        .sort_values(SOURCE_PORTFOLIO_KEYS + ["Data", "papel"])
        .reset_index(drop=True)
        .rename(
            columns={
                "Estratégia": "Estrategia",
                "Volume Mínimo": "Volume Minimo",
            }
        )
    )
    return df


def load_quote_history(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
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


portfolio_history = load_portfolio_history(PERFORMANCE_PATH)
quote_history = load_quote_history(FUNDAMENTUS_PATH)
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
requested_start_dates = pd.date_range(REQUESTED_START, max(available_snapshot_dates), freq="D")
latest_quote_date = quote_matrix.index.max()

print("Carteiras unicas:", portfolio_history[PORTFOLIO_KEYS].drop_duplicates().shape[0])
print("Snapshots disponiveis:", [date.strftime("%Y-%m-%d") for date in available_snapshot_dates])
print("Ultima cotacao disponivel:", latest_quote_date.strftime("%Y-%m-%d"))
print("Datas iniciais solicitadas:", [date.strftime("%Y-%m-%d") for date in requested_start_dates])
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """def get_effective_start_date(requested_date: pd.Timestamp) -> pd.Timestamp | None:
    for snapshot_date in available_snapshot_dates:
        if snapshot_date >= requested_date:
            return snapshot_date
    return None


def compute_rebalance_dates(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    months: int,
) -> list[pd.Timestamp]:
    rebalance_dates = []
    target_date = start_date + pd.DateOffset(months=months)

    while target_date <= end_date:
        valid_dates = [
            snapshot_date
            for snapshot_date in available_snapshot_dates
            if snapshot_date >= target_date and snapshot_date <= end_date
        ]
        if not valid_dates:
            break
        actual_date = valid_dates[0]
        rebalance_dates.append(actual_date)
        target_date = actual_date + pd.DateOffset(months=months)

    return rebalance_dates


def get_price(day: pd.Timestamp, paper: str) -> float:
    try:
        return float(quote_matrix.at[day, paper])
    except KeyError:
        return float("nan")


def simulate_portfolio(
    holdings_by_date: dict[pd.Timestamp, list[str]],
    requested_start_date: pd.Timestamp,
    frequency_name: str,
    months: int,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    effective_start_date = get_effective_start_date(requested_start_date)
    if effective_start_date is None or effective_start_date not in holdings_by_date:
        return pd.DataFrame(), []

    start_holdings = holdings_by_date[effective_start_date]
    start_prices = quote_matrix.loc[effective_start_date, start_holdings].dropna()
    if len(start_prices) != len(start_holdings):
        return pd.DataFrame(), []

    quantities = {
        paper: round((INITIAL_CAPITAL / len(start_holdings)) / start_prices[paper], 0)
        for paper in start_holdings
    }
    current_holdings = start_holdings.copy()
    rebalance_dates = compute_rebalance_dates(
        start_date=effective_start_date,
        end_date=latest_quote_date,
        months=months,
    )
    rebalance_date_set = set(rebalance_dates)
    rebalance_events = []
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
                    price = get_price(day, paper)
                    current_value += quantity * (0.0 if pd.isna(price) else price)

                target_prices = quote_matrix.loc[day, target_holdings].dropna()
                if len(target_prices) == len(target_holdings) and len(target_holdings) > 0:
                    quantities = {
                        paper: round((current_value / len(target_holdings)) / target_prices[paper], 0)
                        for paper in target_holdings
                    }
                    current_holdings = target_holdings.copy()
                    rebalance_events.append(
                        {
                            "Data": day,
                            "frequencia_realocacao": frequency_name,
                            "entradas": len(entries),
                            "saidas": len(exits),
                            "ativos_trocados": max(len(entries), len(exits)),
                            "papers_entrando": ", ".join(entries),
                            "papers_saindo": ", ".join(exits),
                        }
                    )

        portfolio_value = 0.0
        for paper, quantity in quantities.items():
            price = get_price(day, paper)
            portfolio_value += quantity * (0.0 if pd.isna(price) else price)

        history_rows.append(
            {
                "Data": day,
                "valor_carteira": portfolio_value,
                "data_inicio_solicitada": requested_start_date,
                "data_inicio_efetiva": effective_start_date,
                "frequencia_realocacao": frequency_name,
            }
        )

    history = pd.DataFrame(history_rows)
    history["pico"] = history["valor_carteira"].cummax()
    history["drawdown"] = history["valor_carteira"] / history["pico"] - 1
    history["retorno_acumulado"] = history["valor_carteira"] / INITIAL_CAPITAL - 1
    return history, rebalance_events
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """simulation_rows = []
history_frames = []
rebalance_frames = []

for key_values, portfolio_df in portfolio_history.groupby(PORTFOLIO_KEYS, dropna=False):
    key_dict = dict(zip(PORTFOLIO_KEYS, key_values))
    holdings_by_date = {
        date: sorted(group["papel"].tolist())
        for date, group in portfolio_df.groupby("Data")
    }

    for requested_start_date in requested_start_dates:
        effective_start_date = get_effective_start_date(requested_start_date)
        if effective_start_date is None or effective_start_date not in holdings_by_date:
            continue

        for frequency_name, months in FREQUENCIES.items():
            history, rebalance_events = simulate_portfolio(
                holdings_by_date=holdings_by_date,
                requested_start_date=requested_start_date,
                frequency_name=frequency_name,
                months=months,
            )
            if history.empty:
                continue

            history = history.assign(**key_dict)
            history_frames.append(history)

            if rebalance_events:
                rebalance_df = pd.DataFrame(rebalance_events).assign(**key_dict)
                rebalance_frames.append(rebalance_df)

            simulation_rows.append(
                {
                    **key_dict,
                    "data_inicio_solicitada": requested_start_date,
                    "data_inicio_efetiva": history["data_inicio_efetiva"].iloc[0],
                    "data_fim": history["Data"].max(),
                    "frequencia_realocacao": frequency_name,
                    "retorno_total": history["retorno_acumulado"].iloc[-1],
                    "drawdown_maximo": history["drawdown"].min(),
                    "rebalanceamentos_executados": len(rebalance_events),
                    "ativos_trocados_total": int(
                        sum(event["ativos_trocados"] for event in rebalance_events)
                    ),
                    "entradas_total": int(sum(event["entradas"] for event in rebalance_events)),
                    "saidas_total": int(sum(event["saidas"] for event in rebalance_events)),
                }
            )

simulation_summary = pd.DataFrame(simulation_rows).sort_values(
    ["data_inicio_solicitada", "frequencia_realocacao"] + PORTFOLIO_KEYS
).reset_index(drop=True)

simulation_history = pd.concat(history_frames, ignore_index=True)
rebalance_log = (
    pd.concat(rebalance_frames, ignore_index=True)
    if rebalance_frames
    else pd.DataFrame(
        columns=[
            "Data",
            "frequencia_realocacao",
            "entradas",
            "saidas",
            "ativos_trocados",
            "papers_entrando",
            "papers_saindo",
            *PORTFOLIO_KEYS,
        ]
    )
)

simulation_summary.head()
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """coverage = (
    simulation_summary[["data_inicio_solicitada", "data_inicio_efetiva"]]
    .drop_duplicates()
    .sort_values("data_inicio_solicitada")
    .reset_index(drop=True)
)
coverage
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """aggregated_by_start = (
    simulation_summary.groupby(["data_inicio_solicitada", "frequencia_realocacao"], as_index=False)
    .agg(
        retorno_medio=("retorno_total", "mean"),
        retorno_mediano=("retorno_total", "median"),
        melhor_retorno=("retorno_total", "max"),
        pior_retorno=("retorno_total", "min"),
        drawdown_medio=("drawdown_maximo", "mean"),
        drawdown_pior=("drawdown_maximo", "min"),
        trocas_totais=("ativos_trocados_total", "sum"),
        rebalanceamentos=("rebalanceamentos_executados", "sum"),
    )
)

aggregated_by_start
"""
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            """## Melhor combinacao geral

As tabelas abaixo consolidam o desempenho pela combinacao completa de parametros ao longo de todas as datas iniciais analisadas:

- `Estrategia`;
- `Volume Minimo`;
- `Ativos na Carteira`;
- `frequencia_realocacao`.

- `retorno_medio` e `retorno_mediano` ajudam a medir o desempenho tipico;
- `drawdown_medio` e `drawdown_pior` mostram o lado do risco;
- `vitorias_por_periodo` conta quantas vezes a combinacao ficou empatada ou liderou em retorno total dentro de cada data inicial.
- A secao seguinte tambem destaca a pior combinacao geral e resume a distribuicao dos retornos.
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """strategy_configuration_base = simulation_summary.copy()
strategy_configuration_base["razao_retorno_drawdown"] = strategy_configuration_base.apply(
    lambda row: (
        float("inf")
        if row["drawdown_maximo"] == 0 and row["retorno_total"] > 0
        else (
            0.0
            if row["drawdown_maximo"] == 0
            else row["retorno_total"] / abs(row["drawdown_maximo"])
        )
    ),
    axis=1,
)

best_return_by_period = strategy_configuration_base.groupby("data_inicio_solicitada")["retorno_total"].transform("max")
strategy_configuration_base["venceu_periodo"] = (
    strategy_configuration_base["retorno_total"] == best_return_by_period
).astype(int)

strategy_configuration_summary = (
    strategy_configuration_base.groupby(
        ["Estrategia", "Volume Minimo", "Ativos na Carteira", "frequencia_realocacao"],
        as_index=False,
    )
    .agg(
        retorno_medio=("retorno_total", "mean"),
        retorno_mediano=("retorno_total", "median"),
        melhor_retorno=("retorno_total", "max"),
        pior_retorno=("retorno_total", "min"),
        drawdown_medio=("drawdown_maximo", "mean"),
        drawdown_pior=("drawdown_maximo", "min"),
        razao_media_retorno_drawdown=("razao_retorno_drawdown", "mean"),
        vitorias_por_periodo=("venceu_periodo", "sum"),
    )
    .sort_values(
        ["vitorias_por_periodo", "retorno_medio", "razao_media_retorno_drawdown"],
        ascending=[False, False, False],
    )
    .reset_index(drop=True)
)

strategy_configuration_summary
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """best_overall_strategy_configuration = strategy_configuration_summary.iloc[0]
best_overall_strategy_configuration.to_frame(name="valor")
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """worst_overall_strategy_configuration = strategy_configuration_summary.sort_values(
    ["retorno_medio", "razao_media_retorno_drawdown", "drawdown_pior"],
    ascending=[True, True, True],
).iloc[0]

worst_overall_strategy_configuration.to_frame(name="valor")
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """best_by_requested_start = (
    strategy_configuration_base.sort_values(
        ["data_inicio_solicitada", "retorno_total", "razao_retorno_drawdown"],
        ascending=[True, False, False],
    )
    .groupby("data_inicio_solicitada", as_index=False)
    .first()
    [
        [
            "data_inicio_solicitada",
            "Estrategia",
            "Volume Minimo",
            "Ativos na Carteira",
            "frequencia_realocacao",
            "retorno_total",
            "drawdown_maximo",
            "razao_retorno_drawdown",
        ]
    ]
)

best_by_requested_start
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """worst_by_requested_start = (
    strategy_configuration_base.sort_values(
        ["data_inicio_solicitada", "retorno_total", "razao_retorno_drawdown"],
        ascending=[True, True, True],
    )
    .groupby("data_inicio_solicitada", as_index=False)
    .first()
    [
        [
            "data_inicio_solicitada",
            "Estrategia",
            "Volume Minimo",
            "Ativos na Carteira",
            "frequencia_realocacao",
            "retorno_total",
            "drawdown_maximo",
            "razao_retorno_drawdown",
        ]
    ]
)

worst_by_requested_start
"""
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            """## Distribuicao das performances

As proximas celulas ajudam a enxergar nao apenas o melhor e o pior caso, mas tambem a dispersao dos resultados entre os diferentes periodos de entrada.
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """distribution_summary = (
    strategy_configuration_base.groupby(
        ["Estrategia", "Volume Minimo", "Ativos na Carteira", "frequencia_realocacao"],
        as_index=False,
    )
    .agg(
        qtd_periodos=("retorno_total", "size"),
        retorno_min=("retorno_total", "min"),
        retorno_p25=("retorno_total", lambda values: values.quantile(0.25)),
        retorno_mediano=("retorno_total", "median"),
        retorno_p75=("retorno_total", lambda values: values.quantile(0.75)),
        retorno_max=("retorno_total", "max"),
        retorno_std=("retorno_total", "std"),
    )
    .sort_values(["retorno_mediano", "retorno_p75"], ascending=[False, False])
    .reset_index(drop=True)
)

distribution_summary
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """fig, ax = plt.subplots(figsize=(11, 5))
strategy_configuration_base.boxplot(
    column="retorno_total",
    by=["Estrategia", "Volume Minimo", "Ativos na Carteira", "frequencia_realocacao"],
    ax=ax,
    grid=False,
)
ax.set_title("Distribuicao do retorno total por configuracao")
ax.set_xlabel("Estrategia | Volume | Ativos | Frequencia")
ax.set_ylabel("Retorno total")
plt.suptitle("")
plt.xticks(rotation=45, ha="right")
plt.show()
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """fig, ax = plt.subplots(figsize=(11, 5))
for (
    strategy_name,
    minimum_volume,
    holdings_count,
    frequency_name,
), group in strategy_configuration_base.groupby(
    ["Estrategia", "Volume Minimo", "Ativos na Carteira", "frequencia_realocacao"]
):
    group = group.sort_values("data_inicio_solicitada")
    ax.plot(
        group["data_inicio_solicitada"],
        group["retorno_total"],
        marker="o",
        label=f"{strategy_name} | vol {int(minimum_volume)} | {int(holdings_count)} ativos | {frequency_name}",
    )

ax.set_title("Retorno total por data inicial")
ax.set_xlabel("Data inicial solicitada")
ax.set_ylabel("Retorno total")
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
plt.tight_layout()
plt.show()
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """frequency_consistency = (
    simulation_summary.groupby(
        ["data_inicio_solicitada", "Estrategia", "Volume Minimo", "Ativos na Carteira"]
    )
    .agg(
        retorno_min=("retorno_total", "min"),
        retorno_max=("retorno_total", "max"),
        drawdown_min=("drawdown_maximo", "min"),
        drawdown_max=("drawdown_maximo", "max"),
        trocas_max=("ativos_trocados_total", "max"),
    )
    .reset_index()
)
frequency_consistency["retorno_dif"] = (
    frequency_consistency["retorno_max"] - frequency_consistency["retorno_min"]
)
frequency_consistency["drawdown_dif"] = (
    frequency_consistency["drawdown_max"] - frequency_consistency["drawdown_min"]
)

frequency_consistency[["retorno_dif", "drawdown_dif", "trocas_max"]].describe()
"""
        )
    )

    cells.append(
        nbf.v4.new_markdown_cell(
            """## Leitura rapida

Se `retorno_dif`, `drawdown_dif` e `trocas_max` estiverem zerados, as tres frequencias entregaram exatamente o mesmo resultado na amostra atual. Isso acontece quando nao houve tempo suficiente para acionar um novo rebalanceamento entre a data de entrada e a ultima cotacao disponivel.
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """heatmap_data = aggregated_by_start.pivot(
    index="data_inicio_solicitada",
    columns="frequencia_realocacao",
    values="retorno_medio",
)
heatmap_data.index = heatmap_data.index.strftime("%Y-%m-%d")

ax = heatmap_data.mul(100).plot(
    kind="bar",
    figsize=(10, 5),
    rot=0,
    title="Retorno medio (%) por data inicial e frequencia de realocacao",
)
ax.set_xlabel("Data inicial solicitada")
ax.set_ylabel("Retorno medio (%)")
plt.show()
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """selection_base = simulation_summary.copy()
selection_base["razao_retorno_drawdown"] = selection_base.apply(
    lambda row: (
        float("inf")
        if row["drawdown_maximo"] == 0 and row["retorno_total"] > 0
        else (
            0.0
            if row["drawdown_maximo"] == 0
            else row["retorno_total"] / abs(row["drawdown_maximo"])
        )
    ),
    axis=1,
)

best_example = selection_base.sort_values(
    ["razao_retorno_drawdown", "retorno_total"],
    ascending=[False, False],
).iloc[0]

best_example.to_frame(name="valor")
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """example_history = simulation_history.loc[
    (simulation_history["Estrategia"] == best_example["Estrategia"])
    & (simulation_history["Volume Minimo"] == best_example["Volume Minimo"])
    & (simulation_history["Ativos na Carteira"] == best_example["Ativos na Carteira"])
    & (simulation_history["data_inicio_solicitada"] == best_example["data_inicio_solicitada"])
]

fig, ax = plt.subplots(figsize=(10, 5))
for frequency_name, group in example_history.groupby("frequencia_realocacao"):
    group.plot(
        x="Data",
        y="valor_carteira",
        ax=ax,
        label=frequency_name,
    )

ax.set_title(
    " | ".join(
        [
            str(best_example["Estrategia"]),
            f'{int(best_example["Ativos na Carteira"])} ativos',
            f'vol minimo {int(best_example["Volume Minimo"])}',
            f'inicio em {best_example["data_inicio_solicitada"].strftime("%d/%m/%Y")}',
            f'R/DD={best_example["razao_retorno_drawdown"]:.2f}',
        ]
    )
)
ax.set_ylabel("Valor da carteira")
ax.set_xlabel("Data")
plt.show()
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """top_portfolios = simulation_summary.sort_values(
    ["retorno_total", "drawdown_maximo"],
    ascending=[False, False],
).head(20)

top_portfolios
"""
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """rebalance_log.head(20)
"""
        )
    )

    nb["cells"] = cells
    nb["metadata"] = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.12",
        },
    }
    return nb


def main() -> None:
    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    notebook = build_notebook()
    with NOTEBOOK_PATH.open("w", encoding="utf-8") as file_handle:
        nbf.write(notebook, file_handle)
    print(f"Notebook gerado em: {NOTEBOOK_PATH}")


if __name__ == "__main__":
    main()
