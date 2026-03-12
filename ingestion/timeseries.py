"""
TrafficSignals container and DataPipeline orchestrator.

This is the primary API for the ingestion framework. Downstream signal
analysis code should import DataPipeline and TrafficSignals from here.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from config import (
    SIGNAL_COLUMNS,
    IngestionConfig,
)
from ingestion.aggregator import aggregate_to_time_bins, extract_all_signals
from ingestion.cleaner import clean_dataframe
from ingestion.loader import discover_csv_files, load_csv_full

logger = logging.getLogger(__name__)


@dataclass
class TrafficSignals:
    """
    Container for time-binned traffic signals and metadata.

    This is the primary output of the ingestion pipeline, consumed by
    downstream signal analysis (wavelet decomposition, deviation scoring).
    """
    binned_data: pd.DataFrame
    packets: pd.Series
    bytes: pd.Series
    flows: pd.Series
    time_bin: str
    source_files: List[str]
    date_range: tuple
    total_flows_ingested: int
    attack_bins: pd.Series

    def get_signal(self, metric: str) -> pd.Series:
        signals = {"packets": self.packets, "bytes": self.bytes, "flows": self.flows}
        if metric not in signals:
            raise ValueError(f"Unknown metric '{metric}'. Choose from: {list(signals)}")
        return signals[metric]

    def get_labels(self) -> pd.Series:
        return self.binned_data["dominant_label"]

    def get_attack_mask(self) -> pd.Series:
        return self.attack_bins

    def summary(self) -> str:
        """Return a human-readable summary of the ingested data."""
        attack_count = self.attack_bins.sum()
        total_bins = len(self.attack_bins)
        lines = [
            f"TrafficSignals Summary",
            f"  Time bin:       {self.time_bin}",
            f"  Date range:     {self.date_range[0]} to {self.date_range[1]}",
            f"  Total bins:     {total_bins}",
            f"  Total flows:    {self.total_flows_ingested:,}",
            f"  Attack bins:    {attack_count} / {total_bins} ({100*attack_count/max(total_bins,1):.1f}%)",
            f"  Source files:   {len(self.source_files)}",
        ]
        return "\n".join(lines)


class DataPipeline:
    """
    Orchestrates: CSV loading -> cleaning -> time-bin aggregation -> signal extraction.

    Usage
    -----
        pipeline = DataPipeline()
        signals = pipeline.run()

        # Signals ready for wavelet analysis:
        packet_signal = signals.packets
        byte_signal = signals.bytes
    """

    def __init__(self, config: Optional[IngestionConfig] = None):
        self.config = config or IngestionConfig()

    def run(self) -> TrafficSignals:
        """Execute the full ingestion pipeline across all available CSV files."""
        files = discover_csv_files(self.config.data_dir)
        if not files:
            raise FileNotFoundError(f"No CSV files found in {self.config.data_dir}")

        usecols = self.config.columns or SIGNAL_COLUMNS
        frames = []
        for f in files:
            logger.info("Loading %s...", f.name)
            df = load_csv_full(f, usecols=usecols, chunk_size=self.config.chunk_size)
            df = clean_dataframe(df)
            frames.append(df)

        combined = pd.concat(frames, ignore_index=True)
        total_flows = len(combined)
        logger.info("Total cleaned flows: %d", total_flows)

        binned = aggregate_to_time_bins(combined, self.config.time_bin)
        signals = extract_all_signals(binned)

        return TrafficSignals(
            binned_data=binned,
            packets=signals["packets"],
            bytes=signals["bytes"],
            flows=signals["flows"],
            time_bin=self.config.time_bin,
            source_files=[f.name for f in files],
            date_range=(binned.index.min(), binned.index.max()),
            total_flows_ingested=total_flows,
            attack_bins=binned["is_attack_present"],
        )