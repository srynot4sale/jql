import datetime
import json
import os
import structlog
import queue
from threading import RLock
from typing import Any, Dict, TYPE_CHECKING
from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute)
from huey import crontab  # type: ignore
from huey.contrib.mini import MiniHuey  # type: ignore

from jql.changeset import ChangeSet
from jql.client import Client
from jql.types import get_content, Tag


taskqueue = MiniHuey()
taskqueue.start()

if TYPE_CHECKING:
    TO_REPLICATE: queue.Queue[Any] = queue.Queue()
else:
    TO_REPLICATE = queue.Queue()

INGEST_LOCK = RLock()
LAST_INGESTED: Dict[str, Dict[str, int]] = {}


class ReplicatedChangesets(Model):
    class Meta:
        table_name = 'Changesets'
        region = 'ap-southeast-2'

    db_uuid = UnicodeAttribute(hash_key=True)
    changeset_rowid = NumberAttribute(range_key=True)
    received = UTCDateTimeAttribute()
    content = UnicodeAttribute()


@taskqueue.task()  # type: ignore
def replicate_changeset(changeset: ChangeSet) -> None:
    global TO_REPLICATE

    task_log = structlog.get_logger()
    task_log = task_log.bind(task='replicate_changeset')
    TO_REPLICATE.put(changeset)

    try:
        # Ship to dynamodb
        if not ReplicatedChangesets.exists():
            ReplicatedChangesets.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

        while not TO_REPLICATE.empty():
            cs = TO_REPLICATE.get()

            replicate = {
                'uuid': cs.uuid,
                'client': cs.client,
                'created': str(cs.created),
                'query': cs.query,
                'changes': cs.changes_as_dict()
            }
            rc = ReplicatedChangesets(cs.origin, cs.origin_rowid, received=datetime.datetime.utcnow(), content=json.dumps(replicate))
            rc.save()
            TO_REPLICATE.task_done()
            task_log.warning(f'Replicated changeset {cs.origin_rowid} - {repr(replicate)}')

    except BaseException as e:
        task_log.exception(e)


@taskqueue.task(crontab(minute='*'))  # type: ignore
def ingest_replication() -> None:
    global INGEST_LOCK
    global LAST_INGESTED

    ingest = os.getenv('INGEST', '')
    if not ingest:
        return

    task_log = structlog.get_logger()
    task_log = task_log.bind(task='ingest_replication')

    try:
        if not INGEST_LOCK.acquire(blocking=False):
            task_log.error("Cannot acquire INGEST_LOCK")
            return

        if not ReplicatedChangesets.exists():
            task_log.error('No replication table to pull from')
            return

        # Pull from dynamodb
        for store in Client.get_stores():
            stuuid = store.uuid
            sources = store.get_items([Tag('_ingest')])

            if not sources:
                continue

            if stuuid not in LAST_INGESTED:
                LAST_INGESTED[stuuid] = {}

            task_log.warning(f'Ingest replication for store {stuuid}')
            for source in sources:
                sourceid = get_content(source).value
                if sourceid not in LAST_INGESTED[stuuid]:
                    LAST_INGESTED[stuuid][sourceid] = store.get_last_ingested_changeset(sourceid)

                last = LAST_INGESTED[stuuid][sourceid]
                task_log.warning(f'Ingesting from {sourceid} since {last}')
                for item in ReplicatedChangesets.query(sourceid, ReplicatedChangesets.changeset_rowid > last):
                    content = json.loads(item.content)
                    changeset = ChangeSet(
                        uuid=content['uuid'],
                        origin=item.db_uuid,
                        origin_rowid=int(item.changeset_rowid),
                        client=content['client'],
                        created=datetime.datetime.fromisoformat(content['created']),
                        query=content['query'],
                        changes=ChangeSet.changes_from_dict(content['changes'])
                    )

                    cid = store.record_changeset(changeset)
                    store.apply_changeset(cid)
                    task_log.warning(f'Ingested changeset {item.changeset_rowid} - {repr(content)}')
                    LAST_INGESTED[stuuid][sourceid] = int(item.changeset_rowid)

    except BaseException as e:
        task_log.exception(e)
    finally:
        INGEST_LOCK.release()
