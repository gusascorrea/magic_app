from pathlib import Path
import warnings

import fundamentus as fd
import fundamentus.detalhes as fd_detalhes
import pandas as pd
from pandas.io.html import read_html as pandas_read_html


warnings.filterwarnings("ignore")

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "fundamentus_data.parquet"


def _read_html_lxml(*args, **kwargs):
    kwargs.setdefault("flavor", "lxml")
    return pandas_read_html(*args, **kwargs)


def safe_div(numerator, denominator):
    denominator = denominator.replace(0, pd.NA)
    return numerator.divide(denominator)


def build_transformed_data(raw_data):
    fd_detalhes.pd.read_html = _read_html_lxml

    detailed_frames = []
    failed_tickers = []
    total_tickers = len(raw_data.index)

    for i, papel in enumerate(raw_data.index, start=1):
        print(f"Progresso: {i}/{total_tickers} | Papel: {papel}", end="\r", flush=True)
        try:
            detailed_frames.append(fd.get_papel(papel))
        except Exception as exc:
            failed_tickers.append((papel, str(exc)))

    detailed_data = (
        pd.concat(detailed_frames, ignore_index=False)
        if detailed_frames
        else pd.DataFrame(index=raw_data.index)
    )
    if not detailed_data.empty:
        detailed_data = detailed_data[~detailed_data.index.duplicated(keep="last")]
        detailed_data = detailed_data.reindex(raw_data.index)

    numeric_columns = [
        "Cotacao",
        "Valor_de_mercado",
        "Lucro_Liquido_12m",
        "Patrim_Liq",
        "Receita_Liquida_12m",
        "Div_Yield",
        "Ativo",
        "PCap_Giro",
        "EBIT_12m",
        "PAtiv_Circ_Liq",
        "Valor_da_firma",
        "EV_EBITDA",
        "Liquidez_Corr",
        "ROIC",
        "Vol_med_2m",
        "Div_Bruta",
        "Cres_Rec_5a",
    ]
    available_numeric_columns = [
        column for column in numeric_columns if column in detailed_data.columns
    ]
    detailed_data[available_numeric_columns] = detailed_data[available_numeric_columns].apply(
        pd.to_numeric,
        errors="coerce",
    )

    transformed_data = pd.DataFrame(index=detailed_data.index)
    transformed_data["Cotação"] = detailed_data["Cotacao"]
    transformed_data["P/L"] = safe_div(
        detailed_data["Valor_de_mercado"],
        detailed_data["Lucro_Liquido_12m"],
    )
    transformed_data["P/VP"] = safe_div(
        detailed_data["Valor_de_mercado"],
        detailed_data["Patrim_Liq"],
    )
    transformed_data["PSR"] = safe_div(
        detailed_data["Valor_de_mercado"],
        detailed_data["Receita_Liquida_12m"],
    )
    transformed_data["Div.Yield"] = detailed_data["Div_Yield"]
    transformed_data["P/Ativo"] = safe_div(
        detailed_data["Valor_de_mercado"],
        detailed_data["Ativo"],
    )
    transformed_data["P/Cap.Giro"] = detailed_data["PCap_Giro"]
    transformed_data["P/EBIT"] = safe_div(
        detailed_data["Valor_de_mercado"],
        detailed_data["EBIT_12m"],
    )
    transformed_data["P/Ativ Circ.Liq"] = detailed_data["PAtiv_Circ_Liq"]
    transformed_data["EV/EBIT"] = safe_div(
        detailed_data["Valor_da_firma"],
        detailed_data["EBIT_12m"],
    )
    transformed_data["EV/EBITDA"] = detailed_data["EV_EBITDA"]
    transformed_data["Mrg Ebit"] = safe_div(
        detailed_data["EBIT_12m"],
        detailed_data["Receita_Liquida_12m"],
    )
    transformed_data["Mrg. Líq."] = safe_div(
        detailed_data["Lucro_Liquido_12m"],
        detailed_data["Receita_Liquida_12m"],
    )
    transformed_data["Liq. Corr."] = detailed_data["Liquidez_Corr"]
    transformed_data["ROIC"] = detailed_data["ROIC"]
    transformed_data["ROE"] = safe_div(
        detailed_data["Lucro_Liquido_12m"],
        detailed_data["Patrim_Liq"],
    )
    transformed_data["Liq.2meses"] = detailed_data["Vol_med_2m"]
    transformed_data["Patrim. Líq"] = detailed_data["Patrim_Liq"]
    transformed_data["Dív.Brut/ Patrim."] = safe_div(
        detailed_data["Div_Bruta"],
        detailed_data["Patrim_Liq"],
    )
    transformed_data["Cresc. Rec.5a"] = detailed_data["Cres_Rec_5a"]
    transformed_data = transformed_data.reindex(columns=raw_data.columns)

    print(f"\nProgresso: {total_tickers}/{total_tickers} | Concluido")
    print("Tickers sem detalhes:", len(failed_tickers))

    return transformed_data.reindex(index=raw_data.index, columns=raw_data.columns)


def main():
    raw_data = fd.get_resultado_raw()
    data = build_transformed_data(raw_data)
    data["update date"] = pd.to_datetime("today").strftime("%Y-%m-%d")

    previous_data = pd.read_parquet(DATA_PATH)
    data = pd.concat([previous_data, data])
    data["ticker"] = data.index
    data.drop_duplicates(subset=["ticker", "update date"], keep="last", inplace=True)

    print("Novos dados:", len(data) - len(previous_data))

    data.drop(columns="ticker", inplace=True)
    data.to_parquet(DATA_PATH, engine="pyarrow")


if __name__ == "__main__":
    main()
