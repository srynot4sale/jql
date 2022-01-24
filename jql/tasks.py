import datetime
import json
import os
import structlog
from typing import Dict
from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute)
from huey import crontab  # type: ignore
from huey.contrib.mini import MiniHuey  # type: ignore

from jql.changeset import ChangeSet
from jql.client import Client


queue = MiniHuey()
queue.start()

# Make this threadsafe/better
LAST_INGESTED: Dict[str, int] = {}


class ReplicatedChangesets(Model):
    class Meta:
        table_name = 'Changesets'
        region = 'ap-southeast-2'

    db_uuid = UnicodeAttribute(hash_key=True)
    changeset_rowid = NumberAttribute(range_key=True)
    received = UTCDateTimeAttribute()
    content = UnicodeAttribute()


@queue.task()  # type: ignore
def replicate_changeset(dbid: str, rowid: int, changeset: ChangeSet) -> None:
    task_log = structlog.get_logger()
    task_log = task_log.bind(task='replicate_changeset')
    try:
        # Ship to dynamodb
        if not ReplicatedChangesets.exists():
            ReplicatedChangesets.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

        replicate = {
            'uuid': changeset.uuid,
            'client': changeset.client,
            'created': str(changeset.created),
            'query': changeset.query,
            'changes': changeset.changes_as_dict()
        }
        rc = ReplicatedChangesets(dbid, rowid, received=datetime.datetime.utcnow(), content=json.dumps(replicate))
        rc.save()

        task_log.warning('Changeset replicated!')
    except BaseException as e:
        task_log.exception(e)


@queue.task(crontab(minute='*'))  # type: ignore
def ingest_replication() -> None:
    ingest = os.getenv('INGEST', '')
    if not ingest:
        return

    task_log = structlog.get_logger()
    task_log = task_log.bind(task='ingest_replication')
    try:
        if not ReplicatedChangesets.exists():
            task_log.error('No replication table to pull from')
            return

        sources = ingest.split(',')

        # Pull from dynamodb
        for source in sources:
            last = LAST_INGESTED.get(source, 0)
            for item in ReplicatedChangesets.query(source, ReplicatedChangesets.changeset_rowid > last):
                content = json.loads(item.content)
                changeset = ChangeSet(
                    uuid=content.uuid,
                    client=content.client,
                    created=datetime.datetime.fromisoformat(content.created),
                    query=content.query,
                    changes=ChangeSet.changes_from_dict(content.changes)
                )

                for store in Client.get_stores():
                    store.record_changeset(changeset)

                LAST_INGESTED[source] = last

        task_log.warning('Changesets ingested!')
    except BaseException as e:
        task_log.exception(e)
