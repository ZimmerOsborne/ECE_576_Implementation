"""
CSV loading for CIC-IDS2017 dataset.

Handles the dataset's quirks: leading whitespace in column names,
infinity values in rate columns, and multi-GB file sizes via chunked reading.
"""

import logging
from pathlib import Path
from typing import Iterator, List, Optional

import numpy as np
import pandas as pd

from config import CSV_FILES, DEFAULT_CHUNK_SIZE, RAW_DATA_DIR

logger = logging.getLogger(__name__)


def discover_csv_files(data_dir: Path = RAW_DATA_DIR) -> List[Path]:
    """Find available CSV files. Warns about missing ones."""
    found = []
    for filename in CSV_FILES:
        path = data_dir / filename
        if path.exists():
            found.append(path)
        else:
            logger.warning("Expected CSV not found: %s", path)
    if not found:
        logger.error("No CSV files found in %s", data_dir)
    return found


def load_csv_chunked(
    filepath: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    usecols: Optional[List[str]] = None,
) -> Iterator[pd.DataFrame]:
    """
    Generator yielding cleaned DataFrame chunks from a CSV.

    Each chunk has column names stripped of whitespace and infinity values
    replaced with NaN. If usecols is provided, columns are filtered after
    stripping (so pass the stripped names, e.g. "Timestamp" not " Timestamp").
    """
    reader = pd.read_csv(
        filepath, chunksize=chunk_size, low_memory=False, encoding="latin1"
    )
    for chunk in reader:
        chunk.columns = chunk.columns.str.strip()
        chunk.replace([np.inf, -np.inf], np.nan, inplace=True)
        if usecols:
            available = [c for c in usecols if c in chunk.columns]
            chunk = chunk[available]
        yield chunk


def load_csv_full(
    filepath: Path,
    usecols: Optional[List[str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> pd.DataFrame:
    """Load an entire CSV file into a single DataFrame."""
    chunks = list(load_csv_chunked(filepath, chunk_size=chunk_size, usecols=usecols))
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(chunks, ignore_index=True)
    logger.info("Loaded %d rows from %s", len(df), filepath.name)
    return df
