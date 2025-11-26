# server/tests/test_api.py

"""
Tests for the FastAPI search endpoint.
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from server.main import app
from server.search.semantic import SearchResult


client = TestClient(app)


class TestRootEndpoint:
    """Tests for the root endpoint."""

    def test_root_returns_ok(self):
        """Root endpoint should return OK status."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "LitScout API" in data["message"]


class TestHealthEndpoint:
    """Tests for the health check endpoint."""

    def test_health_returns_healthy(self):
        """Health endpoint should return healthy status."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"


class TestSearchEndpoint:
    """Tests for the search endpoint."""

    @patch("server.main.semantic_search")
    def test_search_with_valid_query(self, mock_search):
        """Test search with a valid query."""
        mock_search.return_value = [
            SearchResult(
                paper_id=1,
                title="Test Paper",
                abstract="Test abstract",
                year=2023,
                doi="10.1234/test",
                score=0.95,
            )
        ]

        response = client.get("/search?q=machine+learning")
        assert response.status_code == 200

        data = response.json()
        assert data["query"] == "machine learning"
        assert data["total_results"] == 1
        assert len(data["results"]) == 1
        assert data["results"][0]["title"] == "Test Paper"
        assert data["results"][0]["score"] == 0.95

    @patch("server.main.semantic_search")
    def test_search_with_no_results(self, mock_search):
        """Test search that returns no results."""
        mock_search.return_value = []

        response = client.get("/search?q=nonexistent+topic")
        assert response.status_code == 200

        data = response.json()
        assert data["total_results"] == 0
        assert data["results"] == []

    def test_search_without_query(self):
        """Test search without query parameter returns error."""
        response = client.get("/search")
        assert response.status_code == 422  # Validation error

    @patch("server.main.semantic_search")
    def test_search_with_top_k_parameter(self, mock_search):
        """Test search with custom top_k parameter."""
        mock_search.return_value = []

        response = client.get("/search?q=test&top_k=5")
        assert response.status_code == 200

        mock_search.assert_called_once()
        call_args = mock_search.call_args
        assert call_args.kwargs["top_k"] == 5

    @patch("server.main.semantic_search")
    def test_search_with_min_score_parameter(self, mock_search):
        """Test search with custom min_score parameter."""
        mock_search.return_value = []

        response = client.get("/search?q=test&min_score=0.5")
        assert response.status_code == 200

        mock_search.assert_called_once()
        call_args = mock_search.call_args
        assert call_args.kwargs["min_score"] == 0.5

    @patch("server.main.semantic_search")
    def test_search_with_model_parameter(self, mock_search):
        """Test search with custom model parameter."""
        mock_search.return_value = []

        response = client.get("/search?q=test&model=custom-model")
        assert response.status_code == 200

        mock_search.assert_called_once()
        call_args = mock_search.call_args
        assert call_args.kwargs["model_name"] == "custom-model"

    def test_search_top_k_validation(self):
        """Test that top_k parameter is validated."""
        # top_k must be >= 1
        response = client.get("/search?q=test&top_k=0")
        assert response.status_code == 422

        # top_k must be <= 100
        response = client.get("/search?q=test&top_k=101")
        assert response.status_code == 422

    def test_search_min_score_validation(self):
        """Test that min_score parameter is validated."""
        # min_score must be >= 0.0
        response = client.get("/search?q=test&min_score=-0.1")
        assert response.status_code == 422

        # min_score must be <= 1.0
        response = client.get("/search?q=test&min_score=1.1")
        assert response.status_code == 422


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
