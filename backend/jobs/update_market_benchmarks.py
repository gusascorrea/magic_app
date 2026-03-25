from datetime import timedelta

import pandas as pd

from shared.clients.cdi_benchmark_client import download_history as download_cdi_history
from shared.clients.parquet_client import read_parquet, write_parquet
from shared.clients.yahoo_benchmark_client import download_history as download_yahoo_history
from shared.config import (
    BENCHMARK_COLUMNS,
    BENCHMARK_HISTORY_PATH,
    BENCHMARK_REFRESH_LOOKBACK_DAYS,
    DEFAULT_BENCHMARK_START_DATE,
    PERFORMANCE_HISTORY_PATH,
    YAHOO_BENCHMARKS,
)


def load_existing_benchmark_history() -> pd.DataFrame:
    if not BENCHMARK_HISTORY_PATH.exists():
        return pd.DataFrame(columns=BENCHMARK_COLUMNS)

    df = read_parquet(BENCHMARK_HISTORY_PATH)
    df["Data"] = pd.to_datetime(df["Data"])
    for column in BENCHMARK_COLUMNS:
        if column == "Data":
            continue
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def resolve_default_start_date() -> pd.Timestamp:
    candidates = [DEFAULT_BENCHMARK_START_DATE]

    if PERFORMANCE_HISTORY_PATH.exists():
        performance_history = read_parquet(PERFORMANCE_HISTORY_PATH, columns=["Data"])
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

    watermark_start_date = available_dates.max() - timedelta(days=BENCHMARK_REFRESH_LOOKBACK_DAYS)
    return max(default_start_date, watermark_start_date)


def merge_histories(
    existing_history: pd.DataFrame,
    yahoo_histories: dict[str, pd.DataFrame],
    cdi_history: pd.DataFrame,
) -> pd.DataFrame:
    existing = existing_history.copy()
    if existing.empty:
        existing = pd.DataFrame(columns=BENCHMARK_COLUMNS)

    refreshed_sources = [
        frame["Data"].min() for frame in [*yahoo_histories.values(), cdi_history] if not frame.empty
    ]
    refreshed = (
        existing[existing["Data"] < min(refreshed_sources)]
        if refreshed_sources
        else existing.iloc[0:0].copy()
    )
    existing_refresh_window = (
        existing[existing["Data"] >= min(refreshed_sources)]
        if refreshed_sources
        else existing.iloc[0:0].copy()
    )

    merged = pd.DataFrame(columns=["Data"])
    for history in yahoo_histories.values():
        if history.empty:
            continue
        merged = history if merged.empty else merged.merge(history, on="Data", how="outer")
    if not cdi_history.empty:
        merged = cdi_history if merged.empty else merged.merge(cdi_history, on="Data", how="outer")

    if not merged.empty and not existing_refresh_window.empty:
        merged = (
            merged.set_index("Data")
            .combine_first(existing_refresh_window.set_index("Data"))
            .reset_index()
        )

    frames = [frame for frame in [refreshed, merged] if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=BENCHMARK_COLUMNS)

    output = pd.concat(frames, ignore_index=True)
    output = output.sort_values("Data").drop_duplicates(subset=["Data"], keep="last")
    for column in BENCHMARK_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    return output[BENCHMARK_COLUMNS].reset_index(drop=True)


def build_change_summary(
    existing_history: pd.DataFrame, updated_history: pd.DataFrame
) -> dict:
    existing = existing_history.copy()
    updated = updated_history.copy()

    for frame in [existing, updated]:
        if frame.empty:
            continue
        frame["Data"] = pd.to_datetime(frame["Data"])
        for column in BENCHMARK_COLUMNS:
            if column == "Data":
                continue
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

    existing = existing.set_index("Data") if not existing.empty else pd.DataFrame()
    updated = updated.set_index("Data") if not updated.empty else pd.DataFrame()

    existing_dates = set(existing.index) if not existing.empty else set()
    updated_dates = set(updated.index) if not updated.empty else set()
    inserted_dates = updated_dates - existing_dates
    common_dates = updated_dates & existing_dates

    updated_counts = {column: 0 for column in BENCHMARK_COLUMNS if column != "Data"}

    for date in common_dates:
        for column in updated_counts:
            existing_value = existing.at[date, column] if column in existing.columns else pd.NA
            updated_value = updated.at[date, column] if column in updated.columns else pd.NA
            if pd.isna(existing_value) != pd.isna(updated_value) or (
                pd.notna(existing_value)
                and pd.notna(updated_value)
                and existing_value != updated_value
            ):
                updated_counts[column] += 1

    return {
        "existing_rows": 0 if existing.empty else len(existing),
        "updated_rows": 0 if updated.empty else len(updated),
        "inserted_dates": len(inserted_dates),
        "updated_counts": updated_counts,
        "has_changes": bool(inserted_dates or any(updated_counts.values())),
    }


def main():
    BENCHMARK_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing_history = load_existing_benchmark_history()
    end_date = pd.Timestamp.today().normalize()

    yahoo_start_dates = {
        value_column: resolve_fetch_start_date(existing_history, value_column)
        for value_column in YAHOO_BENCHMARKS
    }
    cdi_start_date = resolve_fetch_start_date(existing_history, "cdi_rate_aa")

    yahoo_histories = {
        value_column: download_yahoo_history(
            config["ticker"], value_column, yahoo_start_dates[value_column], end_date
        )
        for value_column, config in YAHOO_BENCHMARKS.items()
    }
    cdi_history = download_cdi_history(cdi_start_date, end_date)

    if all(history.empty for history in yahoo_histories.values()) and cdi_history.empty:
        raise RuntimeError("Nao foi possivel atualizar nenhum benchmark.")

    output = merge_histories(existing_history, yahoo_histories, cdi_history)
    change_summary = build_change_summary(existing_history, output)

    print(f"Linhas existentes: {change_summary['existing_rows']}")
    print(f"Linhas apos merge: {change_summary['updated_rows']}")
    print(f"Novas datas inseridas: {change_summary['inserted_dates']}")
    for value_column, config in YAHOO_BENCHMARKS.items():
        print(f"Datas de {config['label']} atualizadas: {change_summary['updated_counts'][value_column]}")
    print(f"Datas de CDI atualizadas: {change_summary['updated_counts']['cdi_rate_aa']}")
    for value_column, config in YAHOO_BENCHMARKS.items():
        print(f"{config['label']} buscado desde {yahoo_start_dates[value_column].date()}")
    print(f"CDI buscado desde {cdi_start_date.date()}")
    for value_column, config in YAHOO_BENCHMARKS.items():
        history = yahoo_histories[value_column]
        if not history.empty:
            print(f"{config['label']} atualizado ate {history['Data'].max().date()}")
    if not cdi_history.empty:
        print(f"CDI atualizado ate {cdi_history['Data'].max().date()}")

    if not change_summary["has_changes"]:
        print("Nenhuma mudanca detectada. Parquet mantido sem regravacao.")
        return

    write_parquet(output, BENCHMARK_HISTORY_PATH, engine="pyarrow")
    print(f"Benchmarks atualizados: {len(output)} linhas salvas em {BENCHMARK_HISTORY_PATH}")


if __name__ == "__main__":
    main()
