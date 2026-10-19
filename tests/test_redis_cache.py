from datetime import date, datetime, timezone
from decimal import Decimal

from app.infrastructure import redis_cache


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.pipeline_keys = []

    def ping(self):
        return True

    def mget(self, keys):
        return [self.values.get(key) for key in keys]

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None):
        self.values[key] = value
        return True

    def delete(self, key):
        self.values.pop(key, None)
        return 1

    def pipeline(self, transaction=True):
        self.pipeline_keys = []
        return self

    def incr(self, key):
        self.pipeline_keys.append(key)

    def execute(self):
        for key in self.pipeline_keys:
            self.values[key] = str(int(self.values.get(key) or 0) + 1)
        return [self.values[key] for key in self.pipeline_keys]


def test_round_trip_preserves_common_database_value_types():
    payload = {
        "created_at": datetime(2026, 10, 19, 12, 30, tzinfo=timezone.utc),
        "birthday": date(1980, 4, 7),
        "exact": Decimal("1.2300"),
        "raw": b"legacy-bytes",
    }

    assert redis_cache._decode(redis_cache._encode(payload)) == payload


def test_cache_type_tags_do_not_collide_with_user_json():
    payload = {
        "custom_metadata": {
            "__type__": "date",
            "value": "this is a user string, not a cache date",
            "$mybibliotheca_cache_value_v1": ["datetime", "not-a-timestamp"],
        },
        "nested": ["$mybibliotheca_cache_value_v1", "bytes", "YWJj"],
        "tuple": ("preserve", Decimal("2.500")),
    }

    assert redis_cache._decode(redis_cache._encode(payload)) == payload


def test_cache_keys_are_isolated_between_postgres_databases(monkeypatch):
    monkeypatch.setenv("CACHE_REDIS_PREFIX", "shared-instance")
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_HOST", "postgres.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_USER", "bibliotheca")

    monkeypatch.setenv("POSTGRES_DB", "library_a")
    library_a_tag = redis_cache._tag_generation_keys(("user:7",))
    library_a_data = redis_cache._versioned_key("user:7:books", ("user:7",), (None,))

    monkeypatch.setenv("POSTGRES_DB", "library_b")
    library_b_tag = redis_cache._tag_generation_keys(("user:7",))
    library_b_data = redis_cache._versioned_key("user:7:books", ("user:7",), (None,))

    assert library_a_tag != library_b_tag
    assert library_a_data != library_b_data


def test_tag_invalidation_moves_reads_to_a_new_cache_generation(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: fake)

    assert redis_cache.get_json("user:7:books", tags=("user:7",)) == (False, None)
    assert redis_cache.set_json("user:7:books", [{"id": "book-1"}], tags=("user:7",))
    assert redis_cache.get_json("user:7:books", tags=("user:7",)) == (True, [{"id": "book-1"}])

    assert redis_cache.invalidate_tags("user:7")
    assert redis_cache.get_json("user:7:books", tags=("user:7",)) == (False, None)


def test_stale_read_snapshot_is_not_cached_after_a_concurrent_write(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: fake)

    snapshot = redis_cache.snapshot_tag_versions(("books",))
    assert snapshot is not None
    assert redis_cache.invalidate_tags("books")
    assert not redis_cache.set_json("all-books", ["stale"], tags=("books",), snapshot=snapshot)


def test_kuzu_query_cache_hits_and_mutations_invalidate(tmp_path, monkeypatch):
    from app.utils import safe_kuzu_manager

    fake = FakeRedis()
    monkeypatch.setattr(redis_cache, "get_redis_client", lambda: fake)
    monkeypatch.setenv("CACHE_REDIS_URL", "redis://fake/1")
    monkeypatch.setenv("KUZU_REDIS_CACHE_ENABLED", "true")
    monkeypatch.setenv("KUZU_REDIS_CACHE_TTL_SECONDS", "10")
    monkeypatch.setattr(safe_kuzu_manager, "_KUZU_CACHE_BYPASS_UNTIL", 0.0)

    manager = safe_kuzu_manager.SafeKuzuManager(str(tmp_path / "cache-test.db"))
    with manager.get_connection(operation="cache-test-create") as connection:
        connection.execute("CREATE NODE TABLE CacheProbe(id STRING, PRIMARY KEY(id))")
        connection.execute("CREATE (n:CacheProbe {id: 'first'})")

    query = "MATCH (n:CacheProbe) RETURN COUNT(n) AS count"
    first = manager.execute_query(query, operation="cache-test-read")
    assert first.get_next()[0] == 1
    connection_count_after_miss = manager._total_connections_created

    second = manager.execute_query(query, operation="cache-test-read")
    assert second.get_next()[0] == 1
    assert manager._total_connections_created == connection_count_after_miss

    with manager.get_connection(operation="cache-test-write") as connection:
        connection.execute("CREATE (n:CacheProbe {id: 'second'})")
    third = manager.execute_query(query, operation="cache-test-read")
    assert third.get_next()[0] == 2
