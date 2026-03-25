from pathlib import Path

import nbformat as nbf

from shared.config import REPO_ROOT


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
"""
        )
    )

    nb["cells"] = cells
    return nb


def main():
    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    notebook = build_notebook()
    with NOTEBOOK_PATH.open("w", encoding="utf-8") as handle:
        nbf.write(notebook, handle)
    print(f"Notebook gerado em: {NOTEBOOK_PATH}")


if __name__ == "__main__":
    main()
