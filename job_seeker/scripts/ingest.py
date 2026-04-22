import asyncio
import sys
from pathlib import Path

# Đảm bảo import được src/
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingest.pipeline import run_pipeline

if __name__ == "__main__":
    asyncio.run(run_pipeline())