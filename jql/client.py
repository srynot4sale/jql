from typing import List

from jql.db import Store
from jql.types import Item
from jql.transaction import Transaction


class Client:
    name: str
    user: str
    store: Store

    def __init__(self, store: Store, client: str):
        self.name, self.user = client.split(':')
        self.store = store

    def new_transaction(self) -> Transaction:
        return Transaction(self.store)

    def read(self, query: str) -> List[Item]:
        return self.new_transaction().q(query)
