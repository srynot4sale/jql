from jql.transaction import Transaction


class Client:
    def __init__(self, user, store, name, tx):
        self.user = user
        self.store = store
        self.name = name
        self.tx = tx

    def new_transaction(self, query):
        return Transaction(user=self.user, client=self, query=query)
