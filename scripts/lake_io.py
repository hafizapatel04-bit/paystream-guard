import json
from pathlib import Path

import pandas as pd


def read_committed(dataset_dir):
    """Read only files in the Spark sink commit log, ignoring orphans from crashed batches."""
    root = Path(dataset_dir)
    meta = root / "_spark_metadata"
    if not meta.exists():
        return pd.read_parquet(root)

    marker = root.as_posix().strip("/") + "/"
    rels = set()
    for log in meta.iterdir():
        if not log.is_file() or log.name.startswith(".") or log.name.endswith(".crc"):
            continue
        for line in log.read_text().splitlines():
            line = line.strip()
            if line.startswith("{"):
                entry = json.loads(line)
                if entry.get("action", "add") == "add":
                    rels.add(entry["path"].split(marker, 1)[1])
    return pd.concat([pd.read_parquet(root / r) for r in sorted(rels)], ignore_index=True)
