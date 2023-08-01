from fastapi.testclient import TestClient
from app.api.routers import predictions_router
from app.services.db_service import db
from unittest.mock import patch

client = TestClient(predictions_router)

@patch.object(db, 'get_live_predictions')
def test_get_live_predictions(mock_get_live_predictions):
    mock_get_live_predictions.return_value = ['prediction1', 'prediction2']

    response = client.get("/live")
    assert response.status_code == 200
    assert response.json() == ['prediction1', 'prediction2']

    mock_get_live_predictions.side_effect = Exception('DB error')
    response = client.get("/live")
    assert response.status_code == 500

@patch.object(db, 'get_match_prediction')
def test_get_match_predictions(mock_get_match_prediction):
    mock_get_match_prediction.return_value = {'id': 1, 'prediction': 'test prediction'}

    response = client.get("/match/1")
    assert response.status_code == 200
    assert response.json() == {'id': 1, 'prediction': 'test prediction'}

    mock_get_match_prediction.return_value = None
    response = client.get("/match/1")
    assert response.status_code == 404