import streamlit as st

from shared.clients.fundamentus_client import get_resultado_raw, list_papel_setor


def _safe_list_papel_setor(setor_id):
    try:
        papeis = list_papel_setor(setor_id)
    except Exception:
        return []
    return papeis if isinstance(papeis, list) else []


@st.cache_data(show_spinner=False)
def load_financial_sector_tickers():
    fin = []
    seg = []

    for setor_id in range(200):
        papeis = _safe_list_papel_setor(setor_id)
        if not fin and "BBAS3" in papeis:
            fin = papeis
        if not seg and "WIZC3" in papeis:
            seg = papeis
        if fin and seg:
            financeiras = fin + seg
            financeiras.remove("WIZC3")
            return financeiras

    return []


@st.cache_data(show_spinner=False)
def load_raw_fundamentus_result():
    return get_resultado_raw()
