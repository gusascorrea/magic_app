import streamlit as st


def render_home_page(language):
    if language == "Português":
        st.header("Início")
        st.markdown("---")
        st.header("Objetivo")
        st.write(
            "Esta aplicação foi desenvolvida com o intuito de informar, capacitar e "
            "facilitar o usuário a selecionar ações de forma eficiente e eficaz. A "
            "técnica abordada torna objetivo o processo de decisão de compra e venda, "
            "com risco abaixo do risco médio do mercado (em nosso caso, o Ibovespa) e "
            "retornos consistentemente superiores."
        )
        st.write(
            "**Aviso Legal: esta aplicação não possui caráter de recomendação de "
            "investimento, somente informativo. A decisão de investimento deve ser "
            "tomada individualmente pelo usuário que utiliza dessas informações.**"
        )

        st.markdown("---")
        st.header("Estratégia")
        st.write(
            "A estratégia é baseada na geração de um ranking de ações que possuem seu "
            "valor de mercado descontado com relação ao seu resultado operacional. "
            "Existem diversas métricas que nos fornecem essa informação. As métricas "
            "utilizadas neste trabalho são:"
        )
        st.write(
            "1. Earnings Yield: Razão entre o lucro operacional (aqui representado pelo "
            "EBIT) e o valor de mercado da empresa (Enterprise Value)."
        )
        st.write("2. ROIC: Retorno sobre o capital investido.")
        st.write(
            "3. Magic Formula: Estratégia que combina o ranking Earnings Yield com o "
            "ranking ROIC."
        )
        st.write(
            "Com os métodos de ranking explicitados, compra-se em mesma proporção um "
            "conjunto de **N** ações que se encontram no topo desse ranking e que "
            "possuam Margem EBIT positiva e uma liquidez mínima."
        )
        st.write(
            "A cada período de balanceamento, vende-se as ações que saíram da lista e "
            "compra-se as que entraram com o valor de venda."
        )
        st.write(
            "Os períodos de balanceamentos mais comuns são o mensal, trimestral "
            "(na frequência de divulgação dos balanços) e anual "
            "(na frequência de divulgação dos balanços mais completos)."
        )
        return

    st.header("Home")
    st.markdown("---")
    st.header("Goal")
    st.write(
        "This application was developed with the intention of informing, enabling and "
        "facilitating the user to select stocks in a efficient and effective way. The "
        "technique used in this application is to generate a ranking of stocks that "
        "have their market value discounted with respect to their operational results. "
        "There are several metrics that provide this information. The metrics used in "
        "this work are:"
    )
    st.write(
        "1. Earnings Yield: The ratio between the operational profit "
        "(here represented by EBIT) and the market value of the company "
        "(Enterprise Value)."
    )
    st.write("2. ROIC: Return on Invested Capital.")
    st.write(
        "3. Magic Formula: Strategy that combines the ranking Earnings Yield with the "
        "ranking ROIC."
    )
    st.write(
        "With the ranking methods explicitly stated, buy-in in the same proportion a "
        "set of **N** stocks that are at the top of this ranking and that have a "
        "positive Margem EBIT and a minimum liquidity."
    )
