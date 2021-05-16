from transaction import Transaction


class Client:
    def __init__(self, user, session, name, tx):
        self.user = user
        self.session = session
        self.name = name
        self.tx = tx

    def new_transaction(self):
        return Transaction(user=self.user, client=self)
