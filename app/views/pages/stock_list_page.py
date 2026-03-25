import streamlit as st

from app.controllers.stock_list_controller import get_investment_table
from app.utils.formatting import prepare_snake_case_table


def render_stock_list_page(language):
    texts = _get_texts(language)

    st.header(texts["title"])
    st.markdown("---")
    st.subheader(texts["strategy"])

    strategy = st.selectbox(texts["select_strategy"], ["Earnings Yield", "Magic Formula", "ROIC"])
    portfolio_size = st.number_input(texts["portfolio_size"], value=20)

    st.markdown("---")
    st.subheader(texts["investment"])
    total_investment = st.number_input(texts["investment_input"], value=0)

    st.markdown("---")
    st.subheader(texts["liquidity"])
    minimum_volume = st.number_input(texts["liquidity_input"], value=400000)

    st.markdown("---")

    investment_table = get_investment_table(
        strategy=strategy,
        portfolio_size=portfolio_size,
        total_investment=total_investment,
        minimum_volume=minimum_volume,
    )

    st.subheader(strategy)
    st.write(texts["top_assets"].format(portfolio_size=int(portfolio_size)))
    st.table(prepare_snake_case_table(investment_table).style.format(precision=2))


def _get_texts(language):
    if language == "Português":
        return {
            "title": "Lista de Ações",
            "strategy": "Estratégia",
            "select_strategy": "Selecione a estratégia",
            "portfolio_size": "Quantidade de ativos na carteira:",
            "investment": "Investimento",
            "investment_input": "Adicione aqui o valor que deseja investir na estratégia:",
            "liquidity": "Filtro de Liquidez",
            "liquidity_input": "Digite o volume financeiro mínimo dos útimos 2 meses:",
            "top_assets": "Primeiros {portfolio_size} ativos",
        }

    return {
        "title": "Stock List",
        "strategy": "Strategy",
        "select_strategy": "Select the strategy",
        "portfolio_size": "Enter the number of assets in your portfolio:",
        "investment": "Investment",
        "investment_input": "Enter the amount you want to invest in the strategy:",
        "liquidity": "Liquidity Filter",
        "liquidity_input": "Enter the minimum financial volume of the last 2 months:",
        "top_assets": "First {portfolio_size} assets",
    }
