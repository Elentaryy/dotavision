from fastapi.testclient import TestClient
from app.api.routers import stats_router
from app.services.db_service import db
from unittest.mock import patch

client = TestClient(stats_router)

@patch.object(db, 'get_tournaments_stats')
def test_get_tournaments_stats(mock_get_tournaments_stats):
    mock_get_tournaments_stats.return_value = {'id': 1, 'stats': 'test stats'}

    response = client.get("/tournaments")
    assert response.status_code == 200
    assert response.json() == {'id': 1, 'stats': 'test stats'}

    mock_get_tournaments_stats.return_value = None
    response = client.get("/tournaments")
    assert response.status_code == 404

    mock_get_tournaments_stats.side_effect = Exception('DB error')
    response = client.get("/tournaments")
    assert response.status_code == 500

@patch.object(db, 'get_recent_stats')
def test_get_recent_stats(mock_get_recent_stats):
    mock_get_recent_stats.return_value = {'id': 1, 'stats': 'test stats'}

    response = client.get("/recent")
    assert response.status_code == 200
    assert response.json() == {'id': 1, 'stats': 'test stats'}

    mock_get_recent_stats.return_value = None
    response = client.get("/recent")
    assert response.status_code == 500

    mock_get_recent_stats.side_effect = Exception('DB error')
    response = client.get("/recent")
    assert response.status_code == 500