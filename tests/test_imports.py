from __future__ import annotations


def test_import_prediction_modules():
    import prediction.features as features

    assert hasattr(features, "clean_raw_dataframe")
    assert hasattr(features, "build_features")
    assert hasattr(features, "score_risk")


def test_import_trained_model_module():
    import prediction.trained_model as trained_model

    assert hasattr(trained_model, "run_trained_model_prediction")
