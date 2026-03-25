import streamlit as st

from app.views.components.sidebar import render_credits, render_references
from app.views.pages.home_page import render_home_page
from app.views.pages.live_study_page import render_live_study_page
from app.views.pages.rebalancing_page import render_rebalancing_page
from app.views.pages.stock_list_page import render_stock_list_page
from app.views.pages.study_page import render_study_page


def run():
    language = "Português"

    if language == "Português":
        render_credits(language)
        st.title("Value Investing")
        st.markdown("---")

        panel = st.sidebar.radio(
            "Painel",
            ["Início", "Estudo", "Estudo em Tempo Real", "Lista de Ações"],
        )

        if panel == "Início":
            render_home_page(language)
        elif panel == "Estudo":
            render_study_page(language)
        elif panel == "Estudo em Tempo Real":
            render_live_study_page()
        elif panel == "Lista de Ações":
            render_stock_list_page(language)
        elif panel == "Rebalanceamento":
            render_rebalancing_page()

        render_references(language)
        return

    render_credits(language)
    st.title("Value Investing")
    st.markdown("---")

    panel = st.sidebar.radio("Panel", ["Home", "Study", "Stock List"])

    if panel == "Home":
        render_home_page(language)
    elif panel == "Study":
        render_study_page(language)
    elif panel == "Stock List":
        render_stock_list_page(language)
    elif panel == "Rebalanceamento":
        render_rebalancing_page()

    render_references(language)
