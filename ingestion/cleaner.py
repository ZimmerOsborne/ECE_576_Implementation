"""
Data cleaning for CIC-IDS2017 flow records.

Handles timestamp parsing (multiple formats), invalid row removal,
and label normalization.
"""

import logging

import pandas as pd

from config import COL_FLOW_DURATION, COL_LABEL, COL_TIMESTAMP, LABEL_BENIGN

logger = logging.getLogger(__name__)


def parse_timestamps(df: pd.DataFrame, col: str = COL_TIMESTAMP) -> pd.DataFrame:
    """
    Parse the Timestamp column into pandas datetime.

    CIC-IDS2017 uses 'dd/MM/yyyy HH:mm:ss' or 'dd/MM/yyyy HH:mm'.
    Tries multiple formats, drops rows that cannot be parsed.
    """
    if col not in df.columns:
        logger.warning("Timestamp column '%s' not found", col)
        return df

    parsed = pd.to_datetime(df[col], dayfirst=True, format="mixed", errors="coerce")
    n_failed = parsed.isna().sum() - df[col].isna().sum()
    if n_failed > 0:
        logger.warning("Dropped %d rows with unparseable timestamps", n_failed)

    df = df.copy()
    df[col] = parsed
    df = df.dropna(subset=[col])
    return df


def drop_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with clearly invalid data.

    Drops rows with NaN in Timestamp or Label, and rows with
    negative Flow Duration (if the column is present).
    """
    initial_len = len(df)
    df = df.dropna(subset=[c for c in [COL_TIMESTAMP, COL_LABEL] if c in df.columns])

    if COL_FLOW_DURATION in df.columns:
        df = df[df[COL_FLOW_DURATION] >= 0]

    dropped = initial_len - len(df)
    if dropped > 0:
        logger.info("Dropped %d invalid rows", dropped)
    return df


def normalize_labels(df: pd.DataFrame, col: str = COL_LABEL) -> pd.DataFrame:
    """
    Standardize the Label column and add a binary is_attack column.
    """
    if col not in df.columns:
        logger.warning("Label column '%s' not found", col)
        return df

    df = df.copy()
    df[col] = df[col].astype(str).str.strip()
    df["is_attack"] = df[col] != LABEL_BENIGN
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full cleaning pipeline: parse timestamps, drop invalid, normalize labels."""
    df = parse_timestamps(df)
    df = drop_invalid_rows(df)
    df = normalize_labels(df)
    return df
