from __future__ import annotations

import re
import unicodedata

import pandas as pd


def normalize_column_name(column_name: str) -> str:
    normalized = unicodedata.normalize("NFD", column_name)
    ascii_only = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    snake = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_only).strip("_").lower()
    return snake


def parse_brl_currency(value: object) -> object:
    if not isinstance(value, str):
        return value

    cleaned = value.strip()
    if not cleaned.startswith("R$"):
        return value

    normalized = cleaned.replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(normalized)
    except ValueError:
        return value


def read_lotofacil_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, engine="openpyxl")
    df.columns = [normalize_column_name(column) for column in df.columns]
    return df


def transform_lotofacil_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    monetary_columns = [
        "rateio_15_acertos",
        "rateio_14_acertos",
        "rateio_13_acertos",
        "rateio_12_acertos",
        "rateio_11_acertos",
        "acumulado_15_acertos",
        "arrecadacao_total",
        "estimativa_premio",
        "acumulado_sorteio_especial_lotofacil_da_independencia",
    ]

    working_df = df.copy()
    for column in monetary_columns:
        if column in working_df.columns:
            working_df[column] = working_df[column].map(parse_brl_currency)

    return working_df
