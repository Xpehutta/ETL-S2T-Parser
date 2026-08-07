import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


COLUMN_MAPPING_PATH = Path(__file__).with_name("column_mapping.json")


def normalize_column_alias(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("ё", "е").replace("Ё", "е").lower()
    text = re.sub(r"[^0-9a-zа-я]+", " ", text, flags=re.IGNORECASE)
    return " ".join(text.split())


@lru_cache(maxsize=8)
def load_column_mapping(path: Optional[str] = None) -> Dict[str, Any]:
    mapping_path = Path(path) if path else COLUMN_MAPPING_PATH
    if not mapping_path.exists():
        return {}
    with mapping_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def clear_column_mapping_cache() -> None:
    load_column_mapping.cache_clear()


def get_sheet_column_mapping(sheet_group: str, path: Optional[str] = None) -> Dict[str, Any]:
    mapping = load_column_mapping(path).get(sheet_group, {})
    return mapping if isinstance(mapping, dict) else {}


def get_field_aliases(sheet_group: str, field_name: str, path: Optional[str] = None) -> List[str]:
    aliases = get_sheet_column_mapping(sheet_group, path).get(field_name, [])
    if isinstance(aliases, str):
        return [aliases]
    if not isinstance(aliases, list):
        return []
    return [str(alias) for alias in aliases if alias is not None and str(alias).strip()]


def add_field_aliases(sheet_group: str, field_name: str, aliases: Iterable[Any], path: Optional[str] = None) -> List[str]:
    """Append aliases to column_mapping.json, deduplicating by normalized text."""
    mapping_path = Path(path) if path else COLUMN_MAPPING_PATH
    if mapping_path.exists():
        with mapping_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            data = {}
    else:
        data = {}

    group = data.setdefault(sheet_group, {})
    if not isinstance(group, dict):
        group = {}
        data[sheet_group] = group

    current = group.get(field_name, [])
    if isinstance(current, str):
        current = [current]
    elif not isinstance(current, list):
        current = []

    existing_normalized = {
        normalize_column_alias(alias)
        for alias in current
        if normalize_column_alias(alias)
    }
    added: List[str] = []
    for alias in aliases:
        text = "" if alias is None else str(alias).strip()
        normalized = normalize_column_alias(text)
        if not text or not normalized or normalized in existing_normalized:
            continue
        current.append(text)
        existing_normalized.add(normalized)
        added.append(text)

    if added:
        group[field_name] = current
        with mapping_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        clear_column_mapping_cache()
    return added
