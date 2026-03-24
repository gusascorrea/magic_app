from datetime import timedelta
from ftplib import FTP
from io import BytesIO
from pathlib import Path

import pandas as pd
import yfinance as yf


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_HISTORY_PATH = REPO_ROOT / "data" / "benchmark_history.parquet"
PERFORMANCE_HISTORY_PATH = (
    REPO_ROOT / "data" / "performance_committed_since_2026-03-10.parquet"
)
IBOV_TICKER = "^BVSP"
CDI_FTP_HOST = "ftp.cetip.com.br"
CDI_FTP_DIR = "MediaCDI"
DEFAULT_START_DATE = pd.Timestamp("2026-03-01")
REFRESH_LOOKBACK_DAYS = 10


def load_existing_benchmark_history() -> pd.DataFrame:
    if not BENCHMARK_HISTORY_PATH.exists():
        return pd.DataFrame(columns=["Data", "ibov_close", "cdi_rate_aa"])

    df = pd.read_parquet(BENCHMARK_HISTORY_PATH)
    df["Data"] = pd.to_datetime(df["Data"])
    for column in ["ibov_close", "cdi_rate_aa"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def resolve_default_start_date() -> pd.Timestamp:
    candidates = [DEFAULT_START_DATE]

    if PERFORMANCE_HISTORY_PATH.exists():
        performance_history = pd.read_parquet(PERFORMANCE_HISTORY_PATH, columns=["Data"])
        performance_history["Data"] = pd.to_datetime(performance_history["Data"])
        if not performance_history.empty:
            candidates.append(performance_history["Data"].min())

    return min(candidates)


def resolve_fetch_start_date(
    existing_history: pd.DataFrame, value_column: str
) -> pd.Timestamp:
    default_start_date = resolve_default_start_date()

    if existing_history.empty or value_column not in existing_history.columns:
        return default_start_date

    available_dates = existing_history.loc[existing_history[value_column].notna(), "Data"]
    if available_dates.empty:
        return default_start_date

    watermark_start_date = available_dates.max() - timedelta(days=REFRESH_LOOKBACK_DAYS)
    return max(default_start_date, watermark_start_date)


def download_ibov_history(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    df = yf.download(
        IBOV_TICKER,
        start=start_date.strftime("%Y-%m-%d"),
        end=(end_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df.empty:
        return pd.DataFrame(columns=["Data", "ibov_close"])

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    price_column = "Adj Close" if "Adj Close" in df.columns else "Close"
    df = df.reset_index().rename(columns={"Date": "Data", price_column: "ibov_close"})
    df["Data"] = pd.to_datetime(df["Data"])
    df["ibov_close"] = pd.to_numeric(df["ibov_close"], errors="coerce")
    return df[["Data", "ibov_close"]].dropna().reset_index(drop=True)


def parse_cdi_rate(raw_text: str) -> float:
    value = raw_text.strip()
    if not value:
        raise ValueError("Arquivo do CDI vazio.")
    if not value.isdigit():
        raise ValueError(f"Conteudo inesperado para a taxa CDI: {value!r}")
    return int(value) / 100


def download_cdi_rate(reference_date: pd.Timestamp) -> dict:
    file_name = reference_date.strftime("%Y%m%d") + ".txt"
    buffer = BytesIO()

    with FTP(host=CDI_FTP_HOST, timeout=30) as ftp:
        ftp.login()
        ftp.cwd(CDI_FTP_DIR)
        ftp.retrbinary(f"RETR {file_name}", buffer.write)

    rate = parse_cdi_rate(buffer.getvalue().decode("utf-8", errors="ignore"))
    return {"Data": reference_date.normalize(), "cdi_rate_aa": rate}


def download_cdi_history(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for reference_date in pd.date_range(start_date, end_date, freq="B"):
        try:
            rows.append(download_cdi_rate(reference_date))
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["Data", "cdi_rate_aa"])

    return pd.DataFrame(rows).sort_values("Data").reset_index(drop=True)


def merge_histories(
    existing_history: pd.DataFrame,
    ibov_history: pd.DataFrame,
    cdi_history: pd.DataFrame,
) -> pd.DataFrame:
    existing = existing_history.copy()
    if existing.empty:
        existing = pd.DataFrame(columns=["Data", "ibov_close", "cdi_rate_aa"])

    refreshed = existing[
        existing["Data"] < min(
            frame["Data"].min()
            for frame in [ibov_history, cdi_history]
            if not frame.empty
        )
    ]

    merged = ibov_history.merge(cdi_history, on="Data", how="outer")
    frames = [frame for frame in [refreshed, merged] if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["Data", "ibov_close", "cdi_rate_aa"])

    output = pd.concat(frames, ignore_index=True)
    output = output.sort_values("Data").drop_duplicates(subset=["Data"], keep="last")
    return output.reset_index(drop=True)


def build_change_summary(
    existing_history: pd.DataFrame, updated_history: pd.DataFrame
) -> dict:
    existing = existing_history.copy()
    updated = updated_history.copy()

    for frame in [existing, updated]:
        if frame.empty:
            continue
        frame["Data"] = pd.to_datetime(frame["Data"])
        for column in ["ibov_close", "cdi_rate_aa"]:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

    existing = existing.set_index("Data") if not existing.empty else pd.DataFrame()
    updated = updated.set_index("Data") if not updated.empty else pd.DataFrame()

    existing_dates = set(existing.index) if not existing.empty else set()
    updated_dates = set(updated.index) if not updated.empty else set()
    inserted_dates = updated_dates - existing_dates
    common_dates = updated_dates & existing_dates

    ibov_updated = 0
    cdi_updated = 0

    for date in common_dates:
        existing_ibov = existing.at[date, "ibov_close"] if "ibov_close" in existing.columns else pd.NA
        updated_ibov = updated.at[date, "ibov_close"] if "ibov_close" in updated.columns else pd.NA
        if pd.isna(existing_ibov) != pd.isna(updated_ibov) or (
            pd.notna(existing_ibov) and pd.notna(updated_ibov) and existing_ibov != updated_ibov
        ):
            ibov_updated += 1

        existing_cdi = existing.at[date, "cdi_rate_aa"] if "cdi_rate_aa" in existing.columns else pd.NA
        updated_cdi = updated.at[date, "cdi_rate_aa"] if "cdi_rate_aa" in updated.columns else pd.NA
        if pd.isna(existing_cdi) != pd.isna(updated_cdi) or (
            pd.notna(existing_cdi) and pd.notna(updated_cdi) and existing_cdi != updated_cdi
        ):
            cdi_updated += 1

    return {
        "existing_rows": 0 if existing.empty else len(existing),
        "updated_rows": 0 if updated.empty else len(updated),
        "inserted_dates": len(inserted_dates),
        "ibov_updated_dates": ibov_updated,
        "cdi_updated_dates": cdi_updated,
        "has_changes": bool(inserted_dates or ibov_updated or cdi_updated),
    }


def main():
    BENCHMARK_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing_history = load_existing_benchmark_history()
    end_date = pd.Timestamp.today().normalize()

    ibov_start_date = resolve_fetch_start_date(existing_history, "ibov_close")
    cdi_start_date = resolve_fetch_start_date(existing_history, "cdi_rate_aa")

    ibov_history = download_ibov_history(ibov_start_date, end_date)
    cdi_history = download_cdi_history(cdi_start_date, end_date)

    if ibov_history.empty and cdi_history.empty:
        raise RuntimeError("Nao foi possivel atualizar IBOV ou CDI.")

    output = merge_histories(existing_history, ibov_history, cdi_history)
    change_summary = build_change_summary(existing_history, output)

    print(f"Linhas existentes: {change_summary['existing_rows']}")
    print(f"Linhas apos merge: {change_summary['updated_rows']}")
    print(f"Novas datas inseridas: {change_summary['inserted_dates']}")
    print(f"Datas de IBOV atualizadas: {change_summary['ibov_updated_dates']}")
    print(f"Datas de CDI atualizadas: {change_summary['cdi_updated_dates']}")
    print(f"IBOV buscado desde {ibov_start_date.date()}")
    print(f"CDI buscado desde {cdi_start_date.date()}")
    if not ibov_history.empty:
        print(f"IBOV atualizado ate {ibov_history['Data'].max().date()}")
    if not cdi_history.empty:
        print(f"CDI atualizado ate {cdi_history['Data'].max().date()}")

    if not change_summary["has_changes"]:
        print("Nenhuma mudanca detectada. Parquet mantido sem regravacao.")
        return

    output.to_parquet(BENCHMARK_HISTORY_PATH, engine="pyarrow")
    print(f"Benchmarks atualizados: {len(output)} linhas salvas em {BENCHMARK_HISTORY_PATH}")


if __name__ == "__main__":
    main()
