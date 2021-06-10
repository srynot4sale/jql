from jql.db import Store


class Client:
    name: str
    user: str
    store: Store

    def __init__(self, store: Store, client: str):
        self.name, self.user = client.split(':')
        self.store = store
