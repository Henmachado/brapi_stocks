import constants
import json
import os

from typing import Any


def save_json_data(data: Any, file_name: str, storage_dir: str = constants.RAW_LAYER) -> None:
    os.makedirs(storage_dir, exist_ok=True)
    file_path = os.path.join(storage_dir, f"{file_name}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)