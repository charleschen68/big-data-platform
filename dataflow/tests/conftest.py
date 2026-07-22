import sys
from pathlib import Path

DATAFLOW_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DATAFLOW_ROOT))
