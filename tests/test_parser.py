import pytest

from jql.parser import jql_parser, JqlTransformer


examples = [
    "CREATE hi",
    "CREATE go to supermarket #todo #todo/completed",
    "CREATE do dishes #todo #chores",
    "CREATE book appointment #todo #todo/remind_at=20210412",
    "@d2a SET #todo/completed",
    "@3dd SET book appointment at physio",
    "@544 SET #book #todo",
    "@aaa",
    "@f4a HISTORY",
    "#todo/completed",
    "do dishes",
    "find #todo",
    "#todo/remind_at=444"
]


@pytest.mark.parametrize("test_query", examples)
def test_parser(test_query: str) -> None:
    tree = jql_parser.parse(test_query)
    # print(tree)
    JqlTransformer().transform(tree)
