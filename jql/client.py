import logging
import structlog
from structlog.stdlib import LoggerFactory
import sys
from typing import List

from jql.db import Store
from jql.types import Item
from jql.transaction import Transaction


class Client:
    ref: str
    name: str
    user: str
    store: Store

    def __init__(self, store: Store, client: str, log_level: int = logging.INFO):
        self.ref = client
        self.name, self.user = client.split(':')
        self.store = store

        logging.basicConfig(
            stream=sys.stdout,
            level=log_level,
        )

        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.dev.set_exc_info,
                structlog.dev.ConsoleRenderer()
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
            context_class=dict,
            logger_factory=LoggerFactory(),
            cache_logger_on_first_use=False
        )

    def new_transaction(self) -> Transaction:
        return Transaction(self, self.store)

    def read(self, query: str) -> List[Item]:
        return self.new_transaction().q(query)
