from io import StringIO

import pandas as pd
import requests

from shared.clients.parquet_client import read_parquet, write_parquet
from shared.config import QUOTE_HISTORY_PATH


RESULTADO_URLS = [
    "https://www.fundamentus.com.br/resultado.php",
    "http://www.fundamentus.com.br/resultado.php",
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

PERCENT_COLUMNS = [
    "Div.Yield",
    "Mrg Ebit",
    "Mrg. Líq.",
    "ROIC",
    "ROE",
    "Cresc. Rec.5a",
]


def parse_percent_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace({"": pd.NA, "nan": pd.NA, "-": pd.NA})
    )
    return pd.to_numeric(cleaned, errors="coerce") / 100


def fetch_resultado_raw() -> pd.DataFrame:
    last_error = None

    for url in RESULTADO_URLS:
        try:
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
            response.raise_for_status()

            tables = pd.read_html(
                StringIO(response.text),
                decimal=",",
                thousands=".",
                flavor="lxml",
            )
            if not tables:
                raise ValueError("No tables found")

            data = tables[0]
            if "Papel" not in data.columns:
                raise ValueError(
                    f"Unexpected table columns from {response.url}: {list(data.columns)}"
                )

            for column in PERCENT_COLUMNS:
                if column in data.columns:
                    data[column] = parse_percent_series(data[column])

            data.index = data["Papel"]
            data.drop(columns="Papel", inplace=True)
            data.sort_index(inplace=True)
            return data
        except Exception as exc:
            last_error = exc

    if last_error is None:
        raise RuntimeError(
            "Nao foi possivel obter a tabela do Fundamentus em nenhum endpoint."
        )

    raise RuntimeError(
        f"Nao foi possivel obter a tabela do Fundamentus em nenhum endpoint. {last_error}"
    ) from last_error


def update_fundamentus_history() -> int:
    latest_data = fetch_resultado_raw()
    latest_data["update date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    previous_data = read_parquet(QUOTE_HISTORY_PATH) if QUOTE_HISTORY_PATH.exists() else pd.DataFrame()

    output = pd.concat([previous_data, latest_data])
    output["ticker"] = output.index
    output.drop_duplicates(subset=["ticker", "update date"], keep="last", inplace=True)

    new_rows = len(output) - len(previous_data)
    output.drop(columns="ticker", inplace=True)
    write_parquet(output, QUOTE_HISTORY_PATH, engine="pyarrow")
    return new_rows
