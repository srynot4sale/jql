from neo4j import GraphDatabase

from client import Client


class User:
    def __init__(self, name, dsn="bolt://localhost:7687"):
        self.name = name
        self.dsn = dsn
        self.driver = GraphDatabase.driver(dsn, auth=("neo4j", "test"))

    def get_client(self, client, tx="HEAD"):
        session = self.driver.session()
        return Client(self, session=session, name=client, tx=tx)

