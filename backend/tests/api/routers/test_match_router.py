from fastapi.testclient import TestClient
from app.api.routers import match_router
from app.services.db_service import db
from unittest.mock import patch

client = TestClient(match_router)

@patch.object(db, 'get_match_info')
def test_get_match_info(mock_get_match_info):
    mock_get_match_info.return_value = {'id': 1, 'info': 'test info'}

    response = client.get("/1")
    assert response.status_code == 200
    assert response.json() == {'id': 1, 'info': 'test info'}

    mock_get_match_info.return_value = None
    response = client.get("/1")
    assert response.status_code == 404

    mock_get_match_info.side_effect = Exception('DB error')
    response = client.get("/1")
    assert response.status_code == 500