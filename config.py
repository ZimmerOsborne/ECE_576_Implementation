"""
Central configuration for the ECE 576 traffic anomaly detection project.

Defines paths, CIC-IDS2017 column mappings, and ingestion defaults.
"""

from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional

PROJECT_ROOT = Path(__file__).parent
RAW_DATA_DIR = PROJECT_ROOT / "TrafficData"

CSV_FILES = [
    "Monday-WorkingHours.pcap_ISCX.csv",
    "Tuesday-WorkingHours.pcap_ISCX.csv",
    "Wednesday-workingHours.pcap_ISCX.csv",
    "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
    "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv",
    "Friday-WorkingHours-Morning.pcap_ISCX.csv",
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
    "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv",
]

# --- Column name constants ---
COL_FLOW_ID = "Flow ID"
COL_SRC_IP = "Source IP"
COL_SRC_PORT = "Source Port"
COL_DST_IP = "Destination IP"
COL_DST_PORT = "Destination Port"
COL_PROTOCOL = "Protocol"
COL_TIMESTAMP = "Timestamp"
COL_FLOW_DURATION = "Flow Duration"
COL_FWD_PACKETS = "Total Fwd Packets"
COL_BWD_PACKETS = "Total Backward Packets"
COL_FWD_BYTES = "Total Length of Fwd Packets"
COL_BWD_BYTES = "Total Length of Bwd Packets"
COL_FLOW_BYTES_PER_SEC = "Flow Bytes/s"
COL_FLOW_PACKETS_PER_SEC = "Flow Packets/s"
COL_LABEL = "Label"

# --- Columns needed for signal construction ---
SIGNAL_COLUMNS = [
    COL_TIMESTAMP,
    COL_FWD_PACKETS,
    COL_BWD_PACKETS,
    COL_FWD_BYTES,
    COL_BWD_BYTES,
    COL_PROTOCOL,
    COL_LABEL,
]

LABEL_BENIGN = "BENIGN"

# --- Aggregation defaults ---
DEFAULT_TIME_BIN = "5min" 
SUPPORTED_TIME_BINS = ["1min", "5min", "10min", "15min", "30min", "1h"]

# --- Loading defaults ---
DEFAULT_CHUNK_SIZE = 100_000

# --- Deviation defaults ---
DEFAULT_M_WINDOW = 50
DEFAULT_H_WINDOW = 5
DEFAULT_M_WEIGHT = 1
DEFAULT_H_WEIGHT = 1
HIGH_CONFIDENCE_THRESHOLD = 2
LOW_CONFIDENCE_THRESHOLD = 1.25


@dataclass
class IngestionConfig:
    """Configuration for a single ingestion run."""
    data_dir: Path = RAW_DATA_DIR
    csv_files: List[str] = field(default_factory=lambda: CSV_FILES.copy())
    time_bin: str = DEFAULT_TIME_BIN
    chunk_size: int = DEFAULT_CHUNK_SIZE
    columns: Optional[List[str]] = None
