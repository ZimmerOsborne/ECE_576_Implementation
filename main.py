"""
Data ingestion pipeline

Usage:
    python main.py                          # Process all available CSV files
    python main.py --time-bin 1min          # Use 1-minute bins instead of 5min
    python main.py --data-dir /path/to/csvs # Custom data directory
"""

import argparse
import logging
import sys
from pathlib import Path

from config import DEFAULT_TIME_BIN, SUPPORTED_TIME_BINS, IngestionConfig
from ingestion.timeseries import DataPipeline

from analysis.wavelet import WaveletPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

def main():
    parser = argparse.ArgumentParser(description="CIC-IDS2017 Traffic Data Ingestion Pipeline")
    parser.add_argument("--data-dir", type=Path, default=None, help="Path to CSV files directory")
    parser.add_argument("--time-bin", default=DEFAULT_TIME_BIN, choices=SUPPORTED_TIME_BINS,
                        help=f"Time bin width (default: {DEFAULT_TIME_BIN})")
    args = parser.parse_args()

    config = IngestionConfig(time_bin=args.time_bin)
    if args.data_dir:
        config.data_dir = args.data_dir

    pipeline = DataPipeline(config=config)

    # Ingested data for wavelet analysis
    signals = pipeline.run()

    print(f"\n{signals.summary()}\n")

    # Show attack type distribution
    labels = signals.get_labels()
    label_counts = labels.value_counts()
    print("Label distribution across time bins:")
    for label, count in label_counts.items():
        print(f"  {label}: {count}")

    # TODO: Run analysis 
    wavelet = WaveletPipeline()

    packet_signal = signals.packets
    byte_signal = signals.bytes

    packet_analysis = wavelet.wavelet_decompose(packet_signal)

    # TODO: Graph performance




if __name__ == "__main__":
    main()
