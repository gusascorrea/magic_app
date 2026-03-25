from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
IMAGES_DIR = REPO_ROOT / "images"
REFERENCES_DIR = REPO_ROOT / "references"

PERFORMANCE_HISTORY_PATH = DATA_DIR / "performance_committed_since_2026-03-10.parquet"
QUOTE_HISTORY_PATH = DATA_DIR / "fundamentus_data.parquet"
BENCHMARK_HISTORY_PATH = DATA_DIR / "benchmark_history.parquet"
REFERENCE_PDF_PATH = REFERENCES_DIR / "TCC Magic Formula.pdf"
PERFORMANCE_CSV_PATH = DATA_DIR / "performance.csv"
PERFORMANCE_PERIOD_ANALYSIS_PATH = DATA_DIR / "performance_period_analysis.parquet"

CDI_FTP_HOST = "ftp.cetip.com.br"
CDI_FTP_DIR = "MediaCDI"
DEFAULT_BENCHMARK_START_DATE = pd.Timestamp("2026-03-01")
BENCHMARK_REFRESH_LOOKBACK_DAYS = 10

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
YAHOO_BENCHMARKS = {
    "ibov_close": {"ticker": "^BVSP", "label": "IBOV"},
    "sp500_close": {"ticker": "^GSPC", "label": "S&P500"},
    "bitcoin_close": {"ticker": "BTC-USD", "label": "Bitcoin"},
}
BENCHMARK_COLUMNS = ["Data", *YAHOO_BENCHMARKS.keys(), "cdi_rate_aa"]
