import datetime
import json
import os
import structlog
from typing import List, TYPE_CHECKING
from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute)

from jql.changeset import ChangeSet

if TYPE_CHECKING:
    from jql.store import Store


class ReplicatedChangesets(Model):
    class Meta:
        table_name = 'Changesets'
        region = 'ap-southeast-2'
        host = 'http://dynamodb:8000' if os.getenv("DEBUG") else None

    db_uuid = UnicodeAttribute(hash_key=True)
    changeset_rowid = NumberAttribute(range_key=True)
    received = UTCDateTimeAttribute()
    content = UnicodeAttribute()


class Replicator:
    def __init__(self, store: 'Store') -> None:
        self._store = store
        self._log = structlog.get_logger('Replicator')
        self._setup = False

    def setup(self) -> None:
        if self._setup:
            return

        # Create table if it does not already exist
        if not ReplicatedChangesets.exists():
            ReplicatedChangesets.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

        self._setup = True

    def replicate_changeset(self, changeset: ChangeSet) -> bool:
        self.setup()
        task_log = self._log.bind(task='replicate_changeset', changeset=changeset)
        try:
            # Ship to dynamodb
            replicate = {
                'uuid': changeset.uuid,
                'client': changeset.client,
                'created': str(changeset.created),
                'query': changeset.query,
                'changes': changeset.changes_as_dict()
            }
            rc = ReplicatedChangesets(
                changeset.origin,
                changeset.origin_rowid,
                received=datetime.datetime.utcnow(),
                content=json.dumps(replicate)
            )
            rc.save()
            task_log.info('Replicated changeset successfully')
            return True
        except BaseException as e:
            task_log.exception(e)
            return False

    def ingest_changesets(self, store_uuid: str, since: int) -> List[ChangeSet]:
        self.setup()
        task_log = self._log.bind(task='ingest_replication', store_uuid=store_uuid)
        task_log.info(f'Ingesting changesets since {since}')
        changesets = []
        try:
            for item in ReplicatedChangesets.query(store_uuid, ReplicatedChangesets.changeset_rowid > since):
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

                changesets.append(changeset)
                task_log.info(f'Loaded changeset {item.changeset_rowid} - {repr(content)}')
        except BaseException as e:
            task_log.exception(e)

        return changesets
