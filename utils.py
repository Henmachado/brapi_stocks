import constants
import json
import logging
import os

from typing import Any


logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def save_json_data(data: Any, file_name: str, storage_dir: str = constants.RAW_LAYER) -> None:
    os.makedirs(storage_dir, exist_ok=True)
    file_path = os.path.join(storage_dir, f"{file_name}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        logger.info(f"\n Saving {file_name} on {storage_dir} \n")
        json.dump(data, f, ensure_ascii=False, indent=4)