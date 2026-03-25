import os
import sys
import warnings

import numpy as np
import pandas as pd

from shared.clients.fundamentus_client import get_resultado_raw, list_papel_setor
from shared.config import PERFORMANCE_CSV_PATH


warnings.filterwarnings("ignore")


def carregar_estado_anterior():
    if PERFORMANCE_CSV_PATH.exists():
        return pd.read_csv(PERFORMANCE_CSV_PATH, index_col=0)
    return pd.DataFrame()


def performance():
    estrategias = ["Earnings Yield", "Magic Formula", "ROIC"]
    combinacao_ativos_na_carteira = [5, 10, 20, 30]
    valor_total = 100000
    volumes = [400000, 1000000]

    performance = carregar_estado_anterior()
    performance_final = pd.DataFrame()

    if not performance.empty:
        performance["Data"] = pd.to_datetime(performance["Data"]).dt.date
        ultima_data = performance["Data"].max()
        data_atual = pd.to_datetime("today").date()

        print("Última data:", ultima_data)
        print("Data atual:", data_atual)

        if ultima_data == data_atual:
            print("Dados já atualizados")
            sys.exit()

    print("Atualizando dados...")
    requisicao = get_resultado_raw()

    for estrategia in estrategias:
        for ativos_na_carteira in combinacao_ativos_na_carteira:
            for vol_min in volumes:
                df = requisicao.copy(deep=True)

                financeiras = set(list_papel_setor(0)) - {"WIZC3"}
                df = df[~df.index.isin(financeiras)]
                df = df[~df.index.str.contains("33|34")]
                df = df[df["Mrg Ebit"] > 0]
                df = df[df["Liq.2meses"] >= vol_min]

                df["First4Chars"] = df.index.str[:4]
                max_values = df.groupby("First4Chars")["Liq.2meses"].transform("max")
                df = df[df["Liq.2meses"] == max_values]
                df.drop(columns="First4Chars", inplace=True)

                df["Earnings Yield"] = round(1 / df["EV/EBIT"] * 100, 1)
                df["ROIC"] = round(df["ROIC"] * 100, 1)

                if estrategia == "Earnings Yield":
                    sorted_df = df.sort_values(
                        by=["EV/EBIT", "Liq.2meses"], ascending=[True, False]
                    )
                elif estrategia == "Magic Formula":
                    df["Ranking_EY"] = df["Earnings Yield"].rank(ascending=False)
                    df["Ranking_ROIC"] = df["ROIC"].rank(ascending=False)
                    df["Magic Formula"] = df["Ranking_EY"] + df["Ranking_ROIC"]
                    sorted_df = df.sort_values(
                        by=["Magic Formula", "Liq.2meses"], ascending=[True, False]
                    )
                else:
                    sorted_df = df.sort_values(
                        by=["ROIC", "Liq.2meses"], ascending=[False, False]
                    )

                sorted_df = sorted_df.head(ativos_na_carteira)
                sorted_df["Quantidade"] = round(
                    (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
                )
                sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]
                if not performance.empty:
                    carteira_anterior = performance[
                        (performance["Estratégia"] == estrategia)
                        & (performance["Ativos na Carteira"] == ativos_na_carteira)
                        & (performance["Volume Mínimo"] == vol_min)
                        & (performance["Data"] == performance["Data"].max())
                    ].drop_duplicates()
                    ativos_atuais = set(sorted_df.index)
                    if not carteira_anterior.empty:
                        vendidos = carteira_anterior.loc[
                            ~carteira_anterior.index.isin(ativos_atuais)
                        ]
                        comprados = carteira_anterior.loc[
                            carteira_anterior.index.isin(ativos_atuais)
                        ]
                        comprados.drop(columns=["Cotação"], inplace=True)
                        comprados = comprados.merge(
                            sorted_df[["Cotação"]],
                            left_index=True,
                            right_index=True,
                            how="left",
                        )
                        comprados["Valor"] = comprados["Cotação"] * comprados["Quantidade"]
                        if len(vendidos) > 0:
                            vendidos.drop(columns=["Cotação"], inplace=True)
                            vendidos = vendidos.merge(
                                df[["Cotação"]],
                                left_index=True,
                                right_index=True,
                                how="left",
                            )
                            vendidos["Valor"] = vendidos["Cotação"] * vendidos["Quantidade"]
                            valor_vendido = vendidos["Valor"].sum()
                            ativos_mantidos = comprados.index
                            sorted_df["Quantidade"] = np.where(
                                ~sorted_df.index.isin(ativos_mantidos),
                                round((valor_vendido / len(vendidos)) / sorted_df["Cotação"], 0),
                                0,
                            )
                        sorted_df = sorted_df.merge(
                            comprados[["Quantidade"]],
                            left_index=True,
                            right_index=True,
                            how="left",
                        )
                        sorted_df[["Quantidade_x", "Quantidade_y"]] = sorted_df[
                            ["Quantidade_x", "Quantidade_y"]
                        ].fillna(0)
                        sorted_df["Quantidade"] = (
                            sorted_df["Quantidade_x"] + sorted_df["Quantidade_y"]
                        )
                        sorted_df.drop(columns=["Quantidade_x", "Quantidade_y"], inplace=True)
                sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]
                sorted_df["Estratégia"] = estrategia
                sorted_df["Ativos na Carteira"] = ativos_na_carteira
                sorted_df["Volume Mínimo"] = vol_min
                sorted_df["Data"] = pd.to_datetime("today").date()
                performance_final = pd.concat([performance_final, sorted_df])

    performance_final.to_csv(PERFORMANCE_CSV_PATH)
    print("Atualização concluída e salva.")


def main():
    performance()


if __name__ == "__main__":
    main()
