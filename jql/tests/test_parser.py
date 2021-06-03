import pytest

from jql.parser import jql_parser, JqlTransformer


examples = [
    "CREATE hi",
    "CREATE go to supermarket #todo #todo/completed",
    "CREATE do dishes #todo #chores",
    "CREATE book appointment #todo #todo/remind_at=20210412",
    "SET @2 #todo/completed",
    "SET @3 book appointment at physio",
    "SET @5 #book #todo",
    "GET @3",
    "HISTORY @3",
    "LIST #todo/completed",
    "LIST do dishes",
    "LIST find #todo",
    "LIST #todo/remind_at=444"
]


@pytest.mark.parametrize("test_query", examples)
def test_parser(test_query):
    tree = jql_parser.parse(test_query)
    # print(tree)
    JqlTransformer().transform(tree)
