import datetime
import json
import os
import structlog
import queue
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
    TO_REPLICATE: Dict[str, queue.Queue[Any]] = {}
else:
    TO_REPLICATE = {}

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
def replicate_changeset(dbcode: str, rowid: int, changeset: ChangeSet) -> None:
    global TO_REPLICATE

    task_log = structlog.get_logger()
    task_log = task_log.bind(task='replicate_changeset')
    if dbcode not in TO_REPLICATE:
        TO_REPLICATE[dbcode] = queue.Queue()
    TO_REPLICATE[dbcode].put((rowid, changeset))

    try:
        # Ship to dynamodb
        if not ReplicatedChangesets.exists():
            ReplicatedChangesets.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

        while not TO_REPLICATE[dbcode].empty():
            csrowid, cs = TO_REPLICATE[dbcode].get()

            replicate = {
                'uuid': cs.uuid,
                'client': cs.client,
                'created': str(cs.created),
                'query': cs.query,
                'changes': cs.changes_as_dict()
            }
            rc = ReplicatedChangesets(dbcode, csrowid, received=datetime.datetime.utcnow(), content=json.dumps(replicate))
            rc.save()
            TO_REPLICATE[dbcode].task_done()
            task_log.info(f'Replicated changeset {csrowid} - {repr(replicate)}')

    except BaseException as e:
        task_log.exception(e)
