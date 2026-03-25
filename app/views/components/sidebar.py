import streamlit as st

from app.config import REFERENCE_PDF_PATH


def render_credits(language):
    st.sidebar.header("Sobre o Autor" if language == "Português" else "About the Author")
    if language == "Português":
        st.sidebar.write("Este aplicativo foi criado por Gustavo Correa usando Streamlit.")
        st.sidebar.markdown(
            "Para mais informações, visite [meu github](https://github.com/gusascorrea) "
            "ou [meu linkedin](https://linkedin.com/in/gustavo-correa--)."
        )
    else:
        st.sidebar.write("This application was created by Gustavo Correa using Streamlit.")
        st.sidebar.markdown(
            "For more information, visit [my github](https://github.com/gusascorrea) "
            "or [my linkedin](https://linkedin.com/in/gustavo-correa--)."
        )
    st.sidebar.markdown("---")


def render_references(language):
    st.sidebar.markdown("---")
    st.sidebar.subheader("Referências:" if language == "Português" else "References:")

    if language == "Português":
        st.sidebar.write(
            "ROMAN, Gabriel. Avaliação da Eficiência da Magic Formula e de Estratégias de "
            "Value Investing para o Mercado Brasileiro. Porto Alegre: UFRGS, 2021."
        )
    else:
        st.sidebar.write(
            "ROMAN, Gabriel. Evaluation of the Efficiency of the Magic Formula and Value "
            "Investing Strategies for the Brazilian Market. Porto Alegre: UFRGS, 2021."
        )

    st.sidebar.download_button(
        label="Download PDF",
        data=REFERENCE_PDF_PATH.read_bytes(),
        file_name="TCC_Magic_Formula.pdf",
        mime="application/pdf",
    )
    if language == "Português":
        st.sidebar.markdown(
            "Magic Formula para o mercado americano: "
            "[Site Magic Formula](https://www.magicformulainvesting.com/)"
        )
        st.sidebar.markdown(
            "Fonte de dados: [API do site Fundamentus](https://pypi.org/project/fundamentus/)"
        )
    else:
        st.sidebar.markdown(
            "Magic Formula for the US market: "
            "[Site Magic Formula](https://www.magicformulainvesting.com/)"
        )
        st.sidebar.markdown(
            "Source of data: [API of Fundamentus website](https://pypi.org/project/fundamentus/)"
        )
