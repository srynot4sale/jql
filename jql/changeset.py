from dataclasses import dataclass


from jql.types import Item, Prop


@dataclass()
class Change:
    pass


@dataclass()
class CreateItem(Change):
    item: Item

    def __repr__(self) -> str:
        return f"CreateItem({self.item.facts})"


@dataclass()
class AddFact(Change):
    item: Item
    new_fact: Prop
