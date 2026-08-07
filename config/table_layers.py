"""Deterministic ETL-layer resolution from the source sheet role."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Tuple

from config.sheet_groups import group_for_sheet


TABLE_LAYERS_PATH = Path(__file__).with_name("table_layers.json")
LayerRule = Tuple[str, Optional[str], Optional[str]]


@lru_cache(maxsize=8)
def load_table_layer_rules(path: Optional[str] = None) -> Tuple[LayerRule, ...]:
    """Load and validate layer transitions configured for sheet groups."""
    config_path = Path(path) if path else TABLE_LAYERS_PATH
    with config_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    raw_rules = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(raw_rules, list):
        raise ValueError("table_layers.json must contain a rules list")

    rules = []
    seen_groups = set()
    for index, raw_rule in enumerate(raw_rules):
        if not isinstance(raw_rule, dict):
            raise ValueError(f"table layer rule {index} must be an object")
        sheet_group = str(raw_rule.get("sheet_group") or "").strip()
        source_layer = str(raw_rule.get("source_layer") or "").strip() or None
        target_layer = str(raw_rule.get("target_layer") or "").strip() or None
        if not sheet_group or not (source_layer or target_layer):
            raise ValueError(
                f"table layer rule {index} requires sheet_group and at least one layer"
            )
        normalized_group = sheet_group.casefold()
        if normalized_group in seen_groups:
            raise ValueError(f"duplicate table layer sheet group: {sheet_group}")
        seen_groups.add(normalized_group)
        rules.append((sheet_group, source_layer, target_layer))
    return tuple(rules)


def clear_table_layer_rules_cache() -> None:
    load_table_layer_rules.cache_clear()


def resolve_sheet_layers(
    sheet_name: str,
    *,
    sheet_group: Optional[str] = None,
    path: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Return the source/target layers assigned to a sheet's semantic group."""
    resolved_group = str(sheet_group or "").strip() or group_for_sheet(sheet_name)
    if not resolved_group:
        return {"source_layer": None, "target_layer": None}

    matches = [
        (source_layer, target_layer)
        for configured_group, source_layer, target_layer in load_table_layer_rules(path)
        if configured_group.casefold() == resolved_group.casefold()
    ]
    if len(matches) != 1:
        return {"source_layer": None, "target_layer": None}
    source_layer, target_layer = matches[0]
    return {"source_layer": source_layer, "target_layer": target_layer}


__all__ = [
    "clear_table_layer_rules_cache",
    "load_table_layer_rules",
    "resolve_sheet_layers",
]
