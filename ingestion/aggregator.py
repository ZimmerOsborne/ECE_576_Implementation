"""
Time-bin aggregation for Barford signal analysis.

Converts individual CIC-IDS2017 flow records into fixed-width time-binned
signal series (packets, bytes, flows) suitable for wavelet decomposition.
"""

import logging
from typing import Dict

import pandas as pd

from config import (
    COL_BWD_BYTES,
    COL_BWD_PACKETS,
    COL_FWD_BYTES,
    COL_FWD_PACKETS,
    COL_LABEL,
    COL_TIMESTAMP,
    DEFAULT_TIME_BIN,
    LABEL_BENIGN,
)

logger = logging.getLogger(__name__)


def aggregate_to_time_bins(
    df: pd.DataFrame,
    time_bin: str = DEFAULT_TIME_BIN,
    timestamp_col: str = COL_TIMESTAMP,
) -> pd.DataFrame:
    """
    Aggregate flow records into fixed-width time bins.

    Each flow is assigned to a bin based on its timestamp. Per-bin statistics
    are computed for packet counts, byte counts, flow counts, and attack labels.
    """
    df = df.copy()
    df = df.set_index(timestamp_col)
    df.index = pd.DatetimeIndex(df.index)

    grouper = pd.Grouper(freq=time_bin)

    # Aggregate traffic volume columns
    agg_cols = {}
    for col in [COL_FWD_PACKETS, COL_BWD_PACKETS, COL_FWD_BYTES, COL_BWD_BYTES]:
        if col in df.columns:
            agg_cols[col] = "sum"

    result = df.groupby(grouper).agg(agg_cols)

    # Rename to clean names
    rename = {
        COL_FWD_PACKETS: "fwd_packets",
        COL_BWD_PACKETS: "bwd_packets",
        COL_FWD_BYTES: "fwd_bytes",
        COL_BWD_BYTES: "bwd_bytes",
    }
    result = result.rename(columns={k: v for k, v in rename.items() if k in result.columns})

    # Derived totals
    result["total_packets"] = result.get("fwd_packets", 0) + result.get("bwd_packets", 0)
    result["total_bytes"] = result.get("fwd_bytes", 0) + result.get("bwd_bytes", 0)

    # Flow count per bin
    result["flow_count"] = df.groupby(grouper).size()

    # Attack labeling
    if "is_attack" in df.columns:
        attack_counts = df.groupby(grouper)["is_attack"].sum()
        result["attack_flow_count"] = attack_counts
        result["benign_flow_count"] = result["flow_count"] - attack_counts
        result["attack_ratio"] = attack_counts / result["flow_count"].replace(0, 1)
        result["is_attack_present"] = attack_counts > 0
    else:
        result["attack_flow_count"] = 0
        result["benign_flow_count"] = result["flow_count"]
        result["attack_ratio"] = 0.0
        result["is_attack_present"] = False

    # Dominant attack label per bin
    if COL_LABEL in df.columns:
        def _dominant_label(group):
            attacks = group[group != LABEL_BENIGN]
            if attacks.empty:
                return LABEL_BENIGN
            return attacks.mode().iloc[0]

        result["dominant_label"] = df.groupby(grouper)[COL_LABEL].apply(_dominant_label)
    else:
        result["dominant_label"] = LABEL_BENIGN

    # Fill gaps for signal continuity (wavelet transform needs gapless data)
    numeric_cols = result.select_dtypes(include="number").columns
    result[numeric_cols] = result[numeric_cols].fillna(0)
    result["dominant_label"] = result["dominant_label"].fillna(LABEL_BENIGN)
    result["is_attack_present"] = result["is_attack_present"].fillna(False)

    result.index.name = "timestamp"
    logger.info(
        "Aggregated into %d time bins (%s) spanning %s to %s",
        len(result), time_bin, result.index.min(), result.index.max(),
    )
    return result


def extract_signal_series(binned_df: pd.DataFrame, metric: str = "total_packets") -> pd.Series:
    """Extract a single time-series signal from aggregated data."""
    if metric not in binned_df.columns:
        raise ValueError(f"Metric '{metric}' not in binned data. Available: {list(binned_df.columns)}")
    return binned_df[metric].copy()


def extract_all_signals(binned_df: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Extract the three primary signals:
    - packets: total packet count per bin
    - bytes: total byte count per bin
    - flows: flow count per bin
    """
    return {
        "packets": extract_signal_series(binned_df, "total_packets"),
        "bytes": extract_signal_series(binned_df, "total_bytes"),
        "flows": extract_signal_series(binned_df, "flow_count"),
    }
