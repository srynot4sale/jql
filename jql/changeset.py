from dataclasses import dataclass


from jql.parser import Tag
from jql.item import Item


@dataclass()
class Change:
    pass


@dataclass()
class CreateItem(Change):
    id: str
    facts: set


@dataclass()
class AddFact(Change):
    item: Item
    new_fact: Tag
