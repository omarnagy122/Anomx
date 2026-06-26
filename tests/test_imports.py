from __future__ import annotations


def test_import_prediction_modules():
    import prediction.features as features

    assert hasattr(features, "clean_raw_dataframe")
    assert hasattr(features, "build_features")
    assert hasattr(features, "score_risk")
