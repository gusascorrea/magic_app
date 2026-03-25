from ftplib import FTP
from io import BytesIO

import pandas as pd

from app.config import CDI_FTP_DIR, CDI_FTP_HOST


def parse_cdi_rate(raw_text: str) -> float:
    value = raw_text.strip()
    if not value:
        raise ValueError("Arquivo do CDI vazio.")
    if not value.isdigit():
        raise ValueError(f"Conteudo inesperado para a taxa CDI: {value!r}")
    return int(value) / 100


def download_rate(reference_date: pd.Timestamp) -> dict:
    file_name = reference_date.strftime("%Y%m%d") + ".txt"
    buffer = BytesIO()

    with FTP(host=CDI_FTP_HOST, timeout=30) as ftp:
        ftp.login()
        ftp.cwd(CDI_FTP_DIR)
        ftp.retrbinary(f"RETR {file_name}", buffer.write)

    rate = parse_cdi_rate(buffer.getvalue().decode("utf-8", errors="ignore"))
    return {"Data": reference_date.normalize(), "cdi_rate_aa": rate}


def download_history(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for reference_date in pd.date_range(start_date, end_date, freq="B"):
        try:
            rows.append(download_rate(reference_date))
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["Data", "cdi_rate_aa"])

    return pd.DataFrame(rows).sort_values("Data").reset_index(drop=True)
