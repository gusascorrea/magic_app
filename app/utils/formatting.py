import re
import unicodedata

import pandas as pd


def to_snake_case(value):
    text = str(value).strip()
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    snake_text = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_text).strip("_").lower()
    return snake_text or text


def prepare_streamlit_dataframe(df):
    prepared = df.copy()
    for column in prepared.columns:
        if pd.api.types.is_object_dtype(prepared[column]):
            prepared[column] = prepared[column].astype("string")
    return prepared


def prepare_snake_case_table(df):
    prepared = prepare_streamlit_dataframe(df.copy())
    prepared.columns = [to_snake_case(column) for column in prepared.columns]

    if not isinstance(prepared.index, pd.RangeIndex):
        prepared.index = [
            to_snake_case(index)
            if isinstance(index, str) and not re.fullmatch(r"[A-Z0-9]+", index)
            else index
            for index in prepared.index
        ]

    return prepared
