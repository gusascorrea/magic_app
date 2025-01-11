import os

import fundamentus as fd
import numpy as np
import pandas as pd


def carregar_estado_anterior():
    """Carrega o estado anterior da carteira, se existir."""
    if os.path.exists("data/carteira_anterior.csv"):
        return pd.read_csv("data/carteira_anterior.csv", index_col=0)
    else:
        return pd.DataFrame()


def salvar_novo_estado(nova_carteira):
    """Salva o estado atual da carteira."""
    nova_carteira.to_csv("data/carteira_anterior.csv")


def performance():

    estrategias = ["Earnings Yield", "Magic Formula", "ROIC"]
    combinacao_ativos_na_carteira = [5, 10, 20, 30]
    valor_total = 100000
    volumes = [400000, 1000000]

    # Carrega o estado anterior
    carteira_anterior = carregar_estado_anterior()

    requisicao = fd.get_resultado_raw()

    for estrategia in estrategias:
        for ativos_na_carteira in combinacao_ativos_na_carteira:
            for vol_min in volumes:

                # Cópia dos dados
                df = requisicao.copy(deep=True)

                # Remoção de ações financeiras (exceto WIZC3)
                i, j, k = 0, 0, 0
                while True:
                    fin_ = fd.list_papel_setor(i)
                    if "BBAS3" in fin_:
                        fin = fin_
                        j = 1
                    seg_ = fd.list_papel_setor(i)
                    if "WIZC3" in seg_:
                        seg = seg_
                        k = 1
                    if j == 1 and k == 1:
                        financeiras = fin + seg
                        break
                    i += 1
                financeiras.remove("WIZC3")
                df = df[~df.index.isin(financeiras)]

                # Remover ADRs e BDRs
                df = df[~(df.index.str.contains("33") | df.index.str.contains("34"))]
                df = df[df["Mrg Ebit"] > 0]
                df = df[df["Liq.2meses"] >= vol_min]

                # Duplicatas por prefixo do ticker
                df["First4Chars"] = df.index.str[:4]
                duplicates = df.duplicated(subset="First4Chars", keep=False)
                max_values = df.groupby("First4Chars")["Liq.2meses"].transform("max")
                df = df[(~duplicates) | (df["Liq.2meses"] == max_values)]
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
                elif estrategia == "ROIC":
                    sorted_df = df.sort_values(
                        by=["ROIC", "Liq.2meses"], ascending=[False, False]
                    )

                # Seleção dos ativos
                sorted_df = sorted_df.head(ativos_na_carteira)
                sorted_df["Quantidade"] = round(
                    (valor_total / ativos_na_carteira) / sorted_df["Cotação"], 0
                )
                sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

                # Identificar entradas e saídas
                ativos_atuais = set(sorted_df.index)
                if not carteira_anterior.empty:
                    ativos_anteriores = set(carteira_anterior.index)
                    vendidos = ativos_anteriores - ativos_atuais
                    comprados = ativos_atuais - ativos_anteriores

                    # Calcular o valor de venda
                    valor_vendido = carteira_anterior.loc[
                        carteira_anterior.index.isin(vendidos), "Valor"
                    ].sum()
                    valor_total_atualizado = valor_total + valor_vendido
                else:
                    valor_total_atualizado = valor_total

                # Ajusta quantidade de compra com valor atualizado
                sorted_df["Quantidade"] = round(
                    (valor_total_atualizado / ativos_na_carteira)
                    / sorted_df["Cotação"],
                    0,
                )
                sorted_df["Valor"] = sorted_df["Quantidade"] * sorted_df["Cotação"]

                # Adicionar metadados
                sorted_df["Estratégia"] = estrategia
                sorted_df["Ativos na Carteira"] = ativos_na_carteira
                sorted_df["Volume Mínimo"] = vol_min
                sorted_df["Data"] = pd.to_datetime("today").date()

                # Salvar a performance
                try:
                    performance_anterior = pd.read_csv("   data/performance.csv")
                    # buscar ativos na data mais recente de performance_anterior que não estão em sorted_df

                    performance_final = pd.concat([performance_anterior, sorted_df])
                    performance_final.to_csv("data/performance.csv", index=False)
                except:
                    sorted_df.to_csv("data/performance.csv", index=False)


performance()
