from neo4j import GraphDatabase

from client import Client


class User:
    def __init__(self, name):
        self.name = name
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test"))

    def get_client(self, client, tx="HEAD"):
        session = self.driver.session()
        return Client(self, session=session, name=client, tx=tx)

