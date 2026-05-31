import json
from pathlib import Path


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> list | dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [] if path.suffix == ".json" else {}
