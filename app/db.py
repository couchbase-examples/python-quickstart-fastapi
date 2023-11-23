from __future__ import annotations
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from datetime import timedelta
from dotenv import load_dotenv
import os
from functools import cache


class CouchbaseClient(object):
    """Class to handle interactions with Couchbase cluster"""

    def __init__(self, conn_str: str, username: str, password: str) -> CouchbaseClient:
        self.cluster = None
        self.bucket = None
        self.scope = None
        self.conn_str = conn_str
        self.username = username
        self.password = password
        self.bucket_name = "travel-sample"
        self.scope_name = "inventory"
        self.connect()

    def connect(self) -> None:
        """Connect to the Couchbase cluster"""
        # If the connection is not established, establish it now
        if not self.cluster:
            print("connecting to db")
            try:
                # authentication for Couchbase cluster
                auth = PasswordAuthenticator(self.username, self.password)

                cluster_opts = ClusterOptions(auth)
                # wan_development is used to avoid latency issues while connecting to Couchbase over the internet
                cluster_opts.apply_profile("wan_development")

                # connect to the cluster
                self.cluster = Cluster(self.conn_str, cluster_opts)

                # wait until the cluster is ready for use
                self.cluster.wait_until_ready(timedelta(seconds=5))

                # get a reference to our bucket
                self.bucket = self.cluster.bucket(self.bucket_name)
            except CouchbaseException as error:
                print(f"Could not connect to cluster. \nError: {error}")
                print(
                    "WARNING: Ensure that you have the travel-sample bucket loaded in the cluster."
                )

            if not self.check_scope_exists():
                print(
                    "WARNING: Inventory scope does not exist in the bucket. \nEnsure that you have the inventory scope in your travel-sample bucket."
                )

            # get a reference to our scope
            self.scope = self.bucket.scope(self.scope_name)

    def check_scope_exists(self) -> bool:
        """Check if the scope exists in the bucket"""
        try:
            scopes_in_bucket = [
                scope.name for scope in self.bucket.collections().get_all_scopes()
            ]
            return self.scope_name in scopes_in_bucket
        except Exception:
            print(
                "Error fetching scopes in cluster. \nEnsure that travel-sample bucket exists."
            )

    def close(self) -> None:
        """Close the connection to the Couchbase cluster"""
        if self.cluster:
            try:
                self.cluster.close()
            except Exception as e:
                print(f"Error closing cluster. \nError: {e}")

    def get_document(self, collection_name: str, key: str):
        """Get document by key using KV operation"""
        return self.scope.collection(collection_name).get(key)

    def insert_document(self, collection_name: str, key: str, doc: dict):
        """Insert document using KV operation"""
        return self.scope.collection(collection_name).insert(key, doc)

    def delete_document(self, collection_name: str, key: str):
        """Delete document using KV operation"""
        return self.scope.collection(collection_name).remove(key)

    def upsert_document(self, collection_name: str, key: str, doc: dict):
        """Upsert document using KV operation"""
        return self.scope.collection(collection_name).upsert(key, doc)

    def query(self, sql_query, *options, **kwargs):
        """Query Couchbase using SQL++"""
        # options are used for positional parameters
        # kwargs are used for named parameters
        return self.scope.query(sql_query, *options, **kwargs)


@cache
def get_db() -> CouchbaseClient:
    """Get Couchbase client"""
    load_dotenv()
    conn_str = os.getenv("DB_CONN_STR")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    if conn_str is None:
        print("WARNING: DB_CONN_STR environment variable not set")
    if username is None:
        print("WARNING: DB_USERNAME environment variable not set")
    if password is None:
        print("WARNING: DB_PASSWORD environment variable not set")
    return CouchbaseClient(conn_str, username, password)
