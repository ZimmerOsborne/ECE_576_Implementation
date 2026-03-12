"""
Deviation score computation for anomaly exposure.

Implements the deviation score method from Barford et al. (2002) that
quantifies how much a signal deviates from its expected behavior within
a local time window.

The deviation score uses two windows:
- M-window: medium-term window for local mean estimation
- H-window: short-term window for current behavior estimation

A high deviation score indicates anomalous behavior in the current
time window relative to the recent past.
"""

import numpy as np
import pandas as pd


def deviation_score(
    signal: pd.Series,
    m_window: int = 50,
    h_window: int = 5,
    m_weight: float = 1.0,
    h_weight: float = 1.0,
) -> pd.Series:

    return
