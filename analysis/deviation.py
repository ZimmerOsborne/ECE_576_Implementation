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

from config import DEFAULT_M_WINDOW, DEFAULT_H_WINDOW , DEFAULT_M_WEIGHT, DEFAULT_H_WEIGHT, HIGH_CONFIDENCE_THRESHOLD, LOW_CONFIDENCE_THRESHOLD

class DeviationScorer:
    """Computes deviation scores for anomaly detection."""

    def __init__(self, m_window = DEFAULT_M_WINDOW,
        h_window = DEFAULT_H_WINDOW,
        m_weight = DEFAULT_M_WEIGHT,
        h_weight = DEFAULT_H_WEIGHT,
        high_threshold = HIGH_CONFIDENCE_THRESHOLD,
        low_threshold = LOW_CONFIDENCE_THRESHOLD
    ):
        """Set to defaults in config"""
        self.m_window = m_window
        self.h_window = h_window
        self.m_weight = m_weight
        self.h_weight = h_weight
        self.high_threshold = high_threshold
        self.low_threshold = low_threshold


    @staticmethod
    def _normalize(signal: np.ndarray) -> np.ndarray:
        """Normalize signal to unit variance."""
        std = np.std(signal)
        if std < 1e-10:
            return np.zeros_like(signal)
        return signal / std

    @staticmethod
    def _local_variance(signal: np.ndarray, window: int) -> np.ndarray:
        """
        Compute local variance using a centered sliding window.
        For zero-mean signals, variance = mean of squared values.
        """
        squared = signal ** 2
        n = len(signal)
        result = np.zeros(n)
        half_w = window // 2

        cumsum = np.cumsum(np.insert(squared, 0, 0))
        for i in range(n):
            start = max(0, i - half_w)
            end = min(n, i + half_w + 1)
            count = end - start
            result[i] = (cumsum[end] - cumsum[start]) / count

        return result

    def deviation_score(self, h_signal: pd.Series, m_signal: pd.Series) -> dict:
        """Compute deviation score from H and M band signals."""
        index = h_signal.index if hasattr(h_signal, 'index') else None

        # Extract raw arrays so _normalize receives np.ndarray
        h_arr = np.asarray(h_signal, dtype=float)
        m_arr = np.asarray(m_signal, dtype=float)

        # Normalize to unit variance
        h_norm = self._normalize(h_arr)
        m_norm = self._normalize(m_arr)

        # Compute local variance
        v_h = self._local_variance(h_norm, self.h_window)
        v_m = self._local_variance(m_norm, self.m_window)

        # Weighted combination
        score = self.h_weight * v_h + self.m_weight * v_m

        # Classify each bin element-wise
        confidence = np.where(
            score >= self.high_threshold, 'high',
            np.where(score >= self.low_threshold, 'low', 'none')
        )

        # Wrap outputs in pd.Series to preserve the time index for alignment
        def _to_series(arr):
            return pd.Series(arr, index=index)

        return {
            'score': _to_series(score),
            'h_normalized': _to_series(h_norm),
            'm_normalized': _to_series(m_norm),
            'v_h': _to_series(v_h),
            'v_m': _to_series(v_m),
            'confidence': _to_series(confidence),
        }
