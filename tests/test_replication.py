from conftest import dbclass
from jql.client import Client
from jql.types import get_fact, get_value
from jql.store.sqlite import SqliteStore


def test_creation_replicates(db: dbclass, replication_enabled: None) -> None:
    assert db.store.replicate

    db.q("CREATE do dishes #chores")

    res = db.q("CHANGESETS")
    cs_uuid = get_fact(res[0], '_tx', 'uuid').value

    # Unset applied/replication flags to match ingested changeset
    changeset = db.store._load_changeset(cs_uuid)
    changeset.applied = False
    changeset.replicated = False

    replicated = db.store.replicator.ingest_changesets(db.store.uuid, since=0)
    assert replicated == [changeset]

    db.q("CREATE mow lawns #todo #chores")

    res2 = db.q("CHANGESETS")
    cs2_uuid = get_fact(res2[0], '_tx', 'uuid').value

    # Unset applied/replication flags to match ingested changeset
    changeset2 = db.store._load_changeset(cs2_uuid)
    changeset2.applied = False
    changeset2.replicated = False

    replicated2 = db.store.replicator.ingest_changesets(db.store.uuid, since=0)
    assert replicated2 == [changeset, changeset2]


def test_basic_ingestion(db: dbclass, replication_enabled: None) -> None:
    db.q("CREATE do dishes #chores")
    db.q("CREATE mow lawns #todo #chores")

    c_res = db.q("CHANGESETS")
    res = db.q("#chores")

    dest = Client(store=SqliteStore(), client="pytest:testuser")

    # Enable ingestion
    dest.read(f"CREATE {db.store.uuid} #_ingest")
    dest.store.ingest_replication()

    c_res2 = dest.read("CHANGESETS")
    res2 = dest.read("#chores")

    # Ignore the _ingest item
    c_res2 = [r for r in c_res2 if get_value(r, '_tx', 'origin') == db.store.uuid]

    db.compare_results(c_res, c_res2)
    db.compare_results(res, res2)


def test_basic_ingestion_with_updates(db: dbclass, replication_enabled: None) -> None:
    db.q("CREATE do dishes #chores")
    db.q("CREATE mow lawns #todo #chores")
    ref = db.last_ref
    db.q(f"@{ref} UNSET #todo")
    db.q(f"@{ref} SET #newtag")

    c_res = db.q("CHANGESETS")
    res = db.q("#chores")

    dest = Client(store=SqliteStore(), client="pytest:testuser")
    # Enable ingestion
    dest.read(f"CREATE {db.store.uuid} #_ingest")
    dest.store.ingest_replication()

    c_res2 = dest.read("CHANGESETS")
    res2 = dest.read("#chores")

    # Ignore the _ingest item
    c_res2 = [r for r in c_res2 if get_value(r, '_tx', 'origin') == db.store.uuid]

    db.compare_results(c_res, c_res2)
    db.compare_results(res, res2)
