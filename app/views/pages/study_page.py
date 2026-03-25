import streamlit as st

from shared.config import IMAGES_DIR


def render_study_page(language):
    if language == "Português":
        st.header("Estudo")
        st.markdown("---")
        st.write(
            "Este trabalho possui base em um estudo realizado no trabalho de conclusão "
            "do Curso de Graduação em Ciências Econômicas na Universidade Federal do "
            "Rio Grande do Sul do Economista Gabriel Roman."
        )
        st.write(
            "O trabalho de referência possui também estratégias de dois fatores que não "
            "foram contempladas neste trabalho pelo fato de atualmente não existirem "
            "fontes estáveis, confiáveis e gratuitas de dados de cotações em tempo real. "
            "Quando houverem melhores soluções, contemplaremos as demais estratégias "
            "abordadas no trabalho de conclusão de curso."
        )
        st.markdown("---")
        st.header("Resultados")
        _render_study_images(language)
        st.write("CAGR: retorno anual composto")
        st.write(
            "Índice de Sharpe: comparação entre o risco/retorno do portfolio com a taxa "
            "livre de risco (SELIC)"
        )
        st.write(
            "Rolling-Year-Win: tem como objetivo comparar o desempenho da estratégia com "
            "o mercado em janelas móveis e identificar qual o percentual de tempo em que "
            "as carteiras performaram acima do mercado nos mesmos períodos"
        )
        return

    st.header("Study")
    st.markdown("---")
    st.write(
        "This work was based on a real study done in the work of graduation in "
        "Economics at the Federal University of Rio Grande do Sul of Economist "
        "Gabriel Roman."
    )
    st.write(
        "The reference work also has strategies of two factors that were not considered "
        "in this work because currently there are no reliable, free and gratuitable "
        "data sources of real-time quotes. When there are better solutions, we will "
        "consider the other strategies discussed in the work of graduation."
    )
    st.markdown("---")
    st.header("Results")
    _render_study_images(language)
    st.write("CAGR: cumulative annual return")
    st.write(
        "Sharpe Index: compares the portfolio risk/return with the risk free rate "
        "(SELIC)"
    )
    st.write(
        "Rolling-Year-Win: aims to compare the performance of the strategy with the "
        "market in moving windows and identify the percentage of time in which the "
        "portfolios performed above the market in the same periods"
    )


def _render_study_images(language):
    labels = {
        "Português": {
            "annual_returns": "Retornos Anuais",
            "annual_volatility": "Volatilidade Anual",
            "performance_data": "Dados de Performance",
            "cumulative_return": "Retorno acumulado para R$100,00 da Estratégia Earnings Yield (2000 - 2020)",
            "rebalancing": "Comparação entre balanceamento anual e trimestral para estratégia Earnings Yield",
            "source": "Fonte: ROMAN, Gabriel. 2021.",
        },
        "English": {
            "annual_returns": "Annual Returns",
            "annual_volatility": "Annual Volatility",
            "performance_data": "Performance Data",
            "cumulative_return": "Retorno acumulado para R$100, 00 da Estratégia Earnings Yield (2000 - 2020)",
            "rebalancing": "Comparison between annual and quarterly rebalancing for Earnings Yield strategy",
            "source": "Source: ROMAN, Gabriel. 2021.",
        },
    }[language]

    _image_with_source(labels["annual_returns"], IMAGES_DIR / "retornos_RomanGabriel.PNG", labels["source"])
    _image_with_source(labels["annual_volatility"], IMAGES_DIR / "volatilidade_RomanGabriel.PNG", labels["source"])
    _image_with_source(labels["performance_data"], IMAGES_DIR / "performance_RomanGabriel.PNG", labels["source"])
    _image_with_source(labels["cumulative_return"], IMAGES_DIR / "ey_ibov.PNG", labels["source"])

    if language == "Português":
        _image_with_source(labels["rebalancing"], IMAGES_DIR / "balanceamento.PNG", labels["source"])


def _image_with_source(title, path, source_label):
    st.subheader(title)
    st.image(str(path))
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write(source_label)
