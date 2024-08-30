import pytest
from couchbase.exceptions import DocumentNotFoundException

BASE = "http://127.0.0.1:8000"
BASE_URI = f"{BASE}/api/v1"


@pytest.fixture(scope="session")
def couchbase_client():
    from app.db import get_db

    couchbase_client = get_db()
    return couchbase_client


@pytest.fixture(scope="module")
def airport_api():
    return f"{BASE_URI}/airport"


@pytest.fixture(scope="module")
def airport_collection():
    return "airport"


@pytest.fixture(scope="module")
def airline_api():
    return f"{BASE_URI}/airline"


@pytest.fixture(scope="module")
def airline_collection():
    return "airline"


@pytest.fixture(scope="module")
def route_api():
    return f"{BASE_URI}/route"


@pytest.fixture(scope="module")
def route_collection():
    return "route"


@pytest.fixture(scope="module")
def hotel_api():
    return f"{BASE_URI}/hotel"


class Helpers:
    @staticmethod
    def delete_existing_document(couchbase_client, collection, key):
        try:
            couchbase_client.delete_document(collection, key)
        except DocumentNotFoundException:
            pass


@pytest.fixture(autouse=True, scope="session")
def helpers():
    return Helpers
