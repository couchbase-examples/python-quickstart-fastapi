"""Offline tests for the hotel search seam (no Couchbase cluster needed).

They check that search failures reach the API as errors instead of being
masked as empty results.
"""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.db import CouchbaseClient, get_db
from app.main import app


class SearchIndexMissing(Exception):
    pass


class _LazyFailingResult:
    """Mimics the SDK: the search request only executes (and fails) in rows()."""

    def rows(self):
        raise SearchIndexMissing("index hotel_search not found")


class _FakeScope:
    def __init__(self, result):
        self._result = result

    def search(self, *args, **kwargs):
        return self._result


class _RowsResult:
    def __init__(self, fields_list):
        self._rows = [SimpleNamespace(fields=f) for f in fields_list]

    def rows(self):
        return iter(self._rows)


def _client_with_scope(scope) -> CouchbaseClient:
    client = CouchbaseClient.__new__(CouchbaseClient)  # skip connect()
    client.index_name = "hotel_search"
    client.scope = scope
    return client


class TestCouchbaseClientSearch:
    def test_search_by_name_returns_names(self):
        client = _client_with_scope(
            _FakeScope(_RowsResult([{"name": "Seal View"}, {}]))
        )
        assert client.search_by_name("Seal") == ["Seal View", ""]

    def test_search_by_name_propagates_lazy_search_error(self):
        client = _client_with_scope(_FakeScope(_LazyFailingResult()))
        with pytest.raises(SearchIndexMissing):
            client.search_by_name("Seal")

    def test_filter_propagates_lazy_search_error(self):
        client = _client_with_scope(_FakeScope(_LazyFailingResult()))
        with pytest.raises(SearchIndexMissing):
            client.filter({"city": "Santa Margarita"}, limit=10, offset=0)

    def test_filter_without_fields_returns_empty_without_searching(self):
        client = _client_with_scope(None)  # scope.search would fail if called
        assert client.filter({}, limit=10, offset=0) == []


@pytest.fixture
def failing_search_api():
    failing = _client_with_scope(_FakeScope(_LazyFailingResult()))
    app.dependency_overrides[get_db] = lambda: failing
    yield TestClient(app)
    app.dependency_overrides.pop(get_db, None)


class TestHotelSearchErrorsReachApi:
    def test_autocomplete_returns_500_when_search_fails(self, failing_search_api):
        response = failing_search_api.get(
            "/api/v1/hotel/autocomplete", params={"name": "Seal"}
        )
        assert response.status_code == 500
        assert "hotel_search" in response.json()["detail"]

    def test_filter_returns_500_when_search_fails(self, failing_search_api):
        response = failing_search_api.post(
            "/api/v1/hotel/filter", json={"city": "Santa Margarita"}
        )
        assert response.status_code == 500
        assert "hotel_search" in response.json()["detail"]

    def test_filter_without_body_returns_empty_list(self, failing_search_api):
        response = failing_search_api.post("/api/v1/hotel/filter")
        assert response.status_code == 200
        assert response.json() == []
