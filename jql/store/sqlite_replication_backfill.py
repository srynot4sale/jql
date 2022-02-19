import jql.client

import datetime
import json
import logging
import sqlite3
import sys
import time
from typing import Any, Dict, List

from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute)

from jql.changeset import ChangeSet, Change
from jql.types import Ref, fact_from_dict
from jql.store.sqlite import SqliteStore


DEFAULT_SLEEP = 1


class ReplicatedChangesets(Model):
    class Meta:
        table_name = 'Changesets'
        region = 'ap-southeast-2'

    db_uuid = UnicodeAttribute(hash_key=True)
    changeset_rowid = NumberAttribute(range_key=True)
    received = UTCDateTimeAttribute()
    content = UnicodeAttribute()


def backfill_replication(store: SqliteStore, sleep_secs) -> None:
    cur = store._conn.cursor()
    res = cur.execute('SELECT rowid, client, created, query, changes, origin, origin_rowid, uuid FROM changesets WHERE origin = ? ORDER BY rowid', (store.uuid,))
    for cs in res:
        changeset = ChangeSet(
            uuid=cs[7],
            client=cs[1],
            created=cs[2],
            query=cs[3],
            origin=cs[5],
            origin_rowid=cs[0],
            changes=[]
        )

        for c in json.loads(cs[4]):
            ref = Ref(c['ref']) if c['ref'] else None
            uid = c['uid'] if c['uid'] else None
            revoke = c['revoke']
            facts = {fact_from_dict(fact) for fact in c['facts']}

            changeset.changes.append(Change(
                ref=ref,
                uid=uid,
                facts=facts,
                revoke=revoke
            ))

        # Check if already exists
        try:
            time.sleep(sleep_secs)
            ReplicatedChangesets.get(changeset.origin, changeset.origin_rowid)
            print(f'{changeset.origin_rowid} already exists, skipping')
            continue

        except ReplicatedChangesets.DoesNotExist:
            time.sleep(sleep_secs)
            print(changeset)
            replicate = {
                'uuid': changeset.uuid,
                'client': changeset.client,
                'created': str(changeset.created),
                'query': changeset.query,
                'changes': changeset.changes_as_dict()
            }
            rc = ReplicatedChangesets(changeset.origin, changeset.origin_rowid, received=datetime.datetime.utcnow(), content=json.dumps(replicate))
            rc.save()


if __name__ == '__main__':
    store_path = sys.argv[1]
    sleep_secs = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_SLEEP
    store = SqliteStore(location=store_path)
    backfill_replication(store, sleep_secs)
