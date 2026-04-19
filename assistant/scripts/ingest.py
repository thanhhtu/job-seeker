#!/usr/bin/env python
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# agent package is at agent/src/agent
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

load_dotenv(ROOT / ".env")

from agent.ingest import ingest_jobs


def main() -> None:
    result = asyncio.run(ingest_jobs())
    print(
        f"Total: {result.total}, Inserted: {result.inserted}, Failed: {result.failed}"
    )
    for err in result.errors:
        print(f"  ERROR: {err}")


if __name__ == "__main__":
    main()
