from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from agents.header_classifier import make_header_features, predict_header_row


def test_make_header_features_matches_supplied_model_shape():
    table = pd.DataFrame(
        [
            ["Отчёт", None, None],
            ["Название", "Код", "Описание"],
            ["Клиент", "A-1", "Тестовая строка"],
        ]
    )

    features = make_header_features(table)

    assert features.shape == (3, 22)
    assert features.dtype == float
    assert features[0, 0] == 2
    assert features[1, 0] == 0


def test_predict_header_row_uses_highest_positive_probability():
    model = MagicMock()
    model.predict_proba.return_value = np.array(
        [[0.9, 0.1], [0.2, 0.8], [0.7, 0.3]]
    )

    with patch("agents.header_classifier._load_model", return_value=model):
        result = predict_header_row([["Title"], ["Column"], ["value"]])

    assert result == 1
    assert model.predict_proba.call_args.args[0].shape == (3, 22)
