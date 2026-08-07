"""CatBoost classifier for selecting the Excel header row."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier


DEFAULT_MODEL_PATH = (
    Path(__file__).resolve().parents[1]
    / "models"
    / "catboost_header_model.cbm"
)


def _model_path() -> Path:
    configured = os.getenv("CATBOOST_HEADER_MODEL_PATH", "").strip()
    if not configured:
        return DEFAULT_MODEL_PATH
    path = Path(configured)
    return path if path.is_absolute() else DEFAULT_MODEL_PATH.parents[1] / path


def _is_pad(value: Any) -> bool:
    return pd.isna(value) or str(value).strip().lower().startswith("untitled")


def _longest_run(mask: np.ndarray) -> int:
    best = current = 0
    for value in mask:
        current = current + 1 if value else 0
        best = max(best, current)
    return best


def _pad_gaps(mask: np.ndarray) -> int:
    filled = ~mask
    if not filled.any():
        return 0

    left = np.flatnonzero(filled)[0]
    right = np.flatnonzero(filled)[-1] + 1
    inner = mask[left:right]
    return sum(
        value and (index == 0 or not inner[index - 1])
        for index, value in enumerate(inner)
    )


def make_header_features(table: pd.DataFrame) -> np.ndarray:
    """Build the 22 row features used to train the supplied model."""
    width = max(table.shape[1], 1)
    denominator = max(width - 1, 1)
    features = []

    for row in table.itertuples(index=False, name=None):
        pad_mask = np.array([_is_pad(value) for value in row])
        values = [
            str(value).strip()
            for value, pad in zip(row, pad_mask)
            if not pad
        ]
        count = len(values)
        lengths = [len(value) for value in values]
        words = [len(value.split()) for value in values]
        pad_positions = np.flatnonzero(pad_mask) / denominator
        has_digit = [any(char.isdigit() for char in value) for value in values]
        only_alpha = [
            bool(value)
            and all(char.isalpha() or char.isspace() for char in value)
            for value in values
        ]
        mixed = [
            any(char.isdigit() for char in value)
            and any(char.isalpha() for char in value)
            for value in values
        ]

        repeated = 0.0
        if count:
            counts = pd.Series(values).value_counts()
            repeated = sum(counts[value] > 1 for value in values) / count

        features.append(
            [
                pad_mask.sum(),
                pad_mask.mean(),
                count,
                count / width,
                _pad_gaps(pad_mask),
                _longest_run(~pad_mask),
                _longest_run(~pad_mask) / width,
                np.mean(pad_positions) if len(pad_positions) else -1,
                np.std(pad_positions) if len(pad_positions) else 0,
                np.min(pad_positions) if len(pad_positions) else -1,
                np.max(pad_positions) if len(pad_positions) else -1,
                np.mean(lengths) if lengths else 0,
                max(lengths, default=0),
                np.mean(words) if words else 0,
                len(set(values)),
                len(set(values)) / count if count else 0,
                np.mean(has_digit) if count else 0,
                np.mean(only_alpha) if count else 0,
                np.mean(mixed) if count else 0,
                np.mean(["\n" in value for value in values]) if count else 0,
                np.mean([value.endswith(":") for value in values]) if count else 0,
                repeated,
            ]
        )

    return np.asarray(features, dtype=float)


@lru_cache(maxsize=1)
def _load_model() -> CatBoostClassifier:
    model = CatBoostClassifier()
    model.load_model(str(_model_path()))
    return model


def predict_header_row(
    table: pd.DataFrame | Sequence[Sequence[Any]],
) -> int:
    """Return the zero-based row with the highest header probability."""
    frame = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table)
    probabilities = _load_model().predict_proba(make_header_features(frame))[:, 1]
    return int(probabilities.argmax())


__all__ = ["make_header_features", "predict_header_row"]
