from flask import Flask, request, render_template, g
from itertools import filterfalse
import logging
from typing import Any, Dict
import urllib


from jql.client import Client
from jql.sqlite import SqliteStore
from jql.types import is_flag, is_tag, is_primary_ref, get_props, get_tags, get_flags, get_value, has_ref, has_sys_tag, Ref, single, Tag, Flag, Fact, tag_eq


app = Flask(__name__)

DATABASE = 'web.jdb'


@app.context_processor
def jql_utilities():  # type: ignore
    return dict(get_tags=get_tags, get_flags=get_flags, get_props=get_props, has_ref=has_ref, is_primary_ref=is_primary_ref, is_tag=is_tag)


@app.context_processor
def html_utilities() -> Dict[str, Any]:
    def make_link(fact: Fact) -> str:
        if is_tag(fact):
            link = f'/tag/{fact.tag}'
        elif is_primary_ref(fact):
            link = f'/ref/{fact.value}'
        elif is_flag(fact):
            link = f'/flag/{fact.tag}/{fact.prop}'
        else:
            link = f'/q?{urllib.parse.urlencode(str(fact))}'
        return f'<a href="{link}">{fact}</a>'

    return dict(make_link=make_link)


def get_toc():
    tx = get_client().new_transaction()  # type: ignore
    all_tags = tx.q("HINTS")
    full_count = None
    primary_tag = None

    # Find the full count by looking for #db tag
    for t in all_tags:
        if not get_tags(t):
            full_count = get_value(t, "db", "count")
            break

    if not full_count:
        raise Exception("No items found")

    # Look for any tags that everything has
    tags = []
    for t in all_tags:
        if not get_tags(t):
            continue

        if get_value(t, "db", "count") == full_count:
            if primary_tag:
                raise Exception("Multiple primary tags found")
            primary_tag = single(get_tags(t))
        else:
            tags.append((single(get_tags(t)), get_value(t, "db", "count")))

    return dict(tags=tags, primary_tag=primary_tag, full_count=full_count)


@app.context_processor
def app_utilities():  # type: ignore
    return get_toc()


def get_client():  # type: ignore
    client = getattr(g, '_client', None)
    if client is None:
        store = SqliteStore(location=DATABASE)
        client = Client(store=store, client="web:user", log_level=logging.ERROR)
    return client


@app.teardown_appcontext
def close_connection(exception):  # type: ignore
    client = getattr(g, '_client', None)
    if client is not None:
        del client


@app.route("/")
def index():  # type: ignore
    primary_tag = get_toc()['primary_tag']

    tx = get_client().new_transaction()  # type: ignore

    # Get non db/count fact
    props = [(single(filterfalse(has_sys_tag, get_flags(t))), get_value(t, "db", "count")) for t in tx.q(f"HINTS #{primary_tag.tag}/") if get_flags(t)]

    tx = get_client().new_transaction()
    items = tx.q(str(primary_tag))

    return render_template('tag.html', title='JQL', context=[primary_tag], items=items, props=props)


@app.route("/results/")
def results():  # type: ignore
    query = request.args.get('q', '')

    if not len(query):
        raise Exception('No query')

    tx = get_client().new_transaction()  # type: ignore
    items = tx.q(query)

    return render_template('tag.html', title=query, context=[], props=[], items=items)


@app.route("/tag/<tagname>")
def tag(tagname):  # type: ignore
    tx = get_client().new_transaction()  # type: ignore
    props = [(single(filterfalse(has_sys_tag, get_flags(t))), get_value(t, "db", "count")) for t in tx.q(f"HINTS #{tagname}/") if get_flags(t)]

    tx = get_client().new_transaction()  # type: ignore
    items = tx.q(f"#{tagname}")

    return render_template('tag.html', title=tagname, context=[Tag(tagname)], props=props, items=items)


@app.route("/flag/<tagname>/<flagname>")
def flag(tagname, flagname):  # type: ignore
    flag_str = f"{tagname}/{flagname}"
    flag = Flag(tagname, flagname)

    tx = get_client().new_transaction()  # type: ignore
    props = [(single(filterfalse(has_sys_tag, get_flags(t))), get_value(t, "db", "count")) for t in tx.q(f"HINTS #{tagname}/") if get_flags(t)]

    tx = get_client().new_transaction()  # type: ignore
    items = tx.q(f"#{flag_str}")

    return render_template('tag.html', title=flag_str, context=[flag], props=props, items=items)


@app.route("/ref/<ref>")
def ref(ref):  # type: ignore
    tx = get_client().new_transaction()  # type: ignore
    item = tx.q(f"@{ref}")[0]
    item_tags = [Tag('db')] + [t for t in get_tags(item) if next(filter(tag_eq(t.tag), get_props(item)), None) is not None]

    return render_template('ref.html', title=ref, context=[Ref(ref)], item=item, item_tags=item_tags)
