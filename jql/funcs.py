# noqa
# type: ignore
from lupa import LuaRuntime

from jql.store.sqlite import SqliteStore
from jql.client import Client
from jql.types import is_content, Tag  # noqa


def run_functions(store, changeset):
    print(changeset)
    # Get functions
    fs = find_functions(store)
    for f in fs:
        print(f)
        run(store, changeset, f)


def find_functions(store):
    # fs = store.get_items([Tag('_function')])
    return [{}]


def run(store, changeset, f):
    lua = LuaRuntime(unpack_returned_tuples=True)
    lfunc = lua.eval("""

    function(changeset, utils)
      uuid = changeset['uuid']
      client = changeset['client']
      created = changeset['created']
      query = changeset['query']
      changes = changeset['changes']

      for _, change in python.enumerate(changes) do
        print(change)
        for i,j,k in python.iter(change['facts']) do
            print(i)
            print(j)
            print(k)
            -- f = change['facts'][i]
            -- print(f)
            -- print(type(fact))
            -- if type(fact) ~= "string" then
            --     print(utils['is_content'](fact))
            -- else
            --     print(fact)
            -- end
        end
      end

      return 1
    end



    """)
    res = lfunc(changeset, {'is_content': is_content})
    print(res)


store = SqliteStore()
client = Client(store=store, client="repl:user")
tx = client.new_transaction()

tx.q("CREATE new item #todo")
cs = tx.changeset

print()
print()
print()
run_functions(store, cs)
