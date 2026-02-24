"""
Tests for CatalogClient - REST Catalog HTTP client.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestCatalogClient:
    """Tests for the CatalogClient class."""

    @pytest.fixture
    def mock_session(self):
        """Mock requests.Session."""
        with patch("catalog_client.requests.Session") as mock:
            session_instance = MagicMock()
            mock.return_value = session_instance
            yield session_instance

    def test_init_without_auth_token(self, mock_session):
        """CatalogClient without auth token should not set Authorization header."""
        from catalog_client import CatalogClient

        client = CatalogClient("http://localhost:5001")

        assert client.base_url == "http://localhost:5001"
        assert client.auth_token is None
        # Headers should not include Authorization
        assert "Authorization" not in mock_session.headers

    def test_init_with_auth_token(self, mock_session):
        """CatalogClient with auth token should set Bearer Authorization header."""
        from catalog_client import CatalogClient

        client = CatalogClient("http://localhost:5001", auth_token="test-token")

        assert client.base_url == "http://localhost:5001"
        assert client.auth_token == "test-token"
        mock_session.headers.__setitem__.assert_called_with(
            "Authorization", "Bearer test-token"
        )

    def test_init_strips_trailing_slash(self, mock_session):
        """CatalogClient should strip trailing slash from base_url."""
        from catalog_client import CatalogClient

        client = CatalogClient("http://localhost:5001/")

        assert client.base_url == "http://localhost:5001"

    def test_get_request(self, mock_session):
        """GET request should construct correct URL and return response."""
        from catalog_client import CatalogClient

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.get.return_value = mock_response

        client = CatalogClient("http://localhost:5001")
        response = client.get("/health")

        mock_session.get.assert_called_once_with(
            "http://localhost:5001/health", timeout=30
        )
        assert response == mock_response

    def test_get_request_custom_timeout(self, mock_session):
        """GET request should use custom timeout when provided."""
        from catalog_client import CatalogClient

        mock_session.get.return_value = MagicMock()

        client = CatalogClient("http://localhost:5001")
        client.get("/health", timeout=5)

        mock_session.get.assert_called_once_with(
            "http://localhost:5001/health", timeout=5
        )

    def test_post_request(self, mock_session):
        """POST request should send JSON payload correctly."""
        from catalog_client import CatalogClient

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response

        client = CatalogClient("http://localhost:5001")
        payload = {"key": "value"}
        response = client.post("/tables", json=payload)

        mock_session.post.assert_called_once_with(
            "http://localhost:5001/tables", json=payload, timeout=30
        )
        assert response == mock_response

    def test_post_request_no_payload(self, mock_session):
        """POST request without payload should work correctly."""
        from catalog_client import CatalogClient

        mock_session.post.return_value = MagicMock()

        client = CatalogClient("http://localhost:5001")
        client.post("/endpoint")

        mock_session.post.assert_called_once_with(
            "http://localhost:5001/endpoint", json=None, timeout=30
        )

    def test_put_request(self, mock_session):
        """PUT request should send JSON payload correctly."""
        from catalog_client import CatalogClient

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_session.put.return_value = mock_response

        client = CatalogClient("http://localhost:5001")
        payload = {"location": "s3://bucket/path", "partition_spec": []}
        response = client.put("/namespaces/default/tables/test", json=payload)

        mock_session.put.assert_called_once_with(
            "http://localhost:5001/namespaces/default/tables/test",
            json=payload,
            timeout=30,
        )
        assert response == mock_response

    def test_put_request_custom_timeout(self, mock_session):
        """PUT request should use custom timeout when provided."""
        from catalog_client import CatalogClient

        mock_session.put.return_value = MagicMock()

        client = CatalogClient("http://localhost:5001")
        client.put("/endpoint", json={}, timeout=60)

        mock_session.put.assert_called_once_with(
            "http://localhost:5001/endpoint", json={}, timeout=60
        )

    def test_str_representation(self, mock_session):
        """__str__ should return the base_url."""
        from catalog_client import CatalogClient

        client = CatalogClient("http://localhost:5001")

        assert str(client) == "http://localhost:5001"

    def test_str_representation_with_trailing_slash_stripped(self, mock_session):
        """__str__ should return base_url with trailing slash already stripped."""
        from catalog_client import CatalogClient

        client = CatalogClient("http://localhost:5001/")

        assert str(client) == "http://localhost:5001"
