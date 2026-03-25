import pandas as pd
import yfinance as yf


def download_history(
    ticker: str, value_column: str, start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    df = yf.download(
        ticker,
        start=start_date.strftime("%Y-%m-%d"),
        end=(end_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df.empty:
        return pd.DataFrame(columns=["Data", value_column])

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    price_column = "Adj Close" if "Adj Close" in df.columns else "Close"
    df = df.reset_index().rename(columns={"Date": "Data", price_column: value_column})
    df["Data"] = pd.to_datetime(df["Data"])
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")
    return df[["Data", value_column]].dropna().reset_index(drop=True)
