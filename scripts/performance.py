import os
import sys
import warnings

import fundamentus as fd
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


def carregar_estado_anterior():
    """Carrega o estado anterior da carteira, se existir."""
    if os.path.exists("data/performance.csv"):
        return pd.read_csv("data/performance.csv", index_col=0)
    else:
        return pd.DataFrame()


def performance():
    estrategias = ["Earnings Yield", "Magic Formula", "ROIC"]
    combinacao_ativos_na_carteira = [5, 10, 20, 30]
    valor_total = 100000
    volumes = [400000, 1000000]

    # Carrega o estado anterior
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
    requisicao = fd.get_resultado_raw()

    for estrategia in estrategias:
        for ativos_na_carteira in combinacao_ativos_na_carteira:
            for vol_min in volumes:
                df = requisicao.copy(deep=True)

                # Remover ações financeiras (exceto WIZC3)
                financeiras = set(fd.list_papel_setor(0)) - {"WIZC3"}
                df = df[~df.index.isin(financeiras)]

                # Remover ADRs e BDRs
                df = df[~df.index.str.contains("33|34")]
                df = df[df["Mrg Ebit"] > 0]
                df = df[df["Liq.2meses"] >= vol_min]

                # Remover duplicatas com base nos 4 primeiros caracteres do ticker
                df["First4Chars"] = df.index.str[:4]
                max_values = df.groupby("First4Chars")["Liq.2meses"].transform("max")
                df = df[df["Liq.2meses"] == max_values]
                df.drop(columns="First4Chars", inplace=True)

                # Calcular métricas
                df["Earnings Yield"] = round(1 / df["EV/EBIT"] * 100, 1)
                df["ROIC"] = round(df["ROIC"] * 100, 1)

                # Ordenação
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
                else:  # ROIC
                    sorted_df = df.sort_values(
                        by=["ROIC", "Liq.2meses"], ascending=[False, False]
                    )

                sorted_df = sorted_df.head(ativos_na_carteira)
                sorted_df["Quantidade"] = (
                    valor_total / ativos_na_carteira
                ) / sorted_df["Cotação"]
                sorted_df["Quantidade"] = sorted_df["Quantidade"].round(0)
                sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

                # Adicionar metadados
                sorted_df["Estratégia"] = estrategia
                sorted_df["Ativos na Carteira"] = ativos_na_carteira
                sorted_df["Volume Mínimo"] = vol_min
                sorted_df["Data"] = pd.to_datetime("today").date()

                # Atualizar dataframe final
                performance_final = pd.concat([performance_final, sorted_df])

    # Salvar os resultados
    performance_final.to_csv("data/performance.csv")
    print("Atualização concluída e salva.")


performance()
