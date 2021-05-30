from client import Client


class User:
    def __init__(self, name, store):
        self.name = name
        self.store = store

    def get_client(self, client, tx="HEAD"):
        return Client(self, store=self.store, name=client, tx=tx)
