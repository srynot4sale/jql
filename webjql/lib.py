import logging
import os.path
from typing import Dict, List, Set
from flask import g


from jql.client import Client
from jql.sqlite import SqliteStore
from jql.types import get_tags, get_value, single, Tag


ROOTDIR = os.path.join(os.getcwd(), 'dbs')
DATABASES = set()


def update_databases() -> Set[str]:
    return set([d[:-4] for d in os.listdir(ROOTDIR) if d.endswith('.jdb')])


def create_database(database: str) -> None:
    # Confirm database str is a-z only
    if not database.isalpha():
        raise Exception(f'{database} is invalid')

    SqliteStore(location=db_path(database))


def url_to_query(url: str) -> str:
    query = url
    query = query.replace('~', '#')
    return query


def query_to_url(query: str) -> str:
    url = query
    url = url.replace('#', '~')
    return url


def db_path(database: str) -> str:
    return os.path.join(ROOTDIR, f'{database}.jdb')


def get_client() -> Client:
    global DATABASES
    database = g.database

    # If not in DATABASES, rescan
    if database not in DATABASES:
        DATABASES = update_databases()

        if database not in DATABASES:
            raise Exception(f'{database} does not exist!')

    if not hasattr(g, '_client'):
        print(f'Loading store {database}')
        store = SqliteStore(location=db_path(database))
        g._client = Client(store=store, client="web:user", log_level=logging.ERROR)

    return g._client


def get_toc():  # type: ignore
    if not hasattr(g, 'database'):
        return {}

    tx = get_client().new_transaction()
    all_tags = tx.q("HINTS")
    full_count = None
    primary_tag = None

    # Find the full count by looking for #db tag
    for t in all_tags:
        if not get_tags(t):
            full_count = get_value(t, "db", "count")
            break

    # Look for any tags that everything has
    tags = []
    for t in all_tags:
        if not get_tags(t):
            continue

        if full_count and get_value(t, "db", "count") == full_count:
            if primary_tag:
                raise Exception("Multiple primary tags found")
            primary_tag = single(get_tags(t))
        else:
            tags.append((single(get_tags(t)), get_value(t, "db", "count")))

    if not primary_tag:
        primary_tag = Tag('db')

    return dict(tags=tags, primary_tag=primary_tag, full_count=full_count)
