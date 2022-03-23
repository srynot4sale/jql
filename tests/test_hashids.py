import pytest

from jql.store.sqlite import SqliteStore


examples = [1, 2, 3, 5, 10, 35, 543, 1234, 5567, 87678]

stores = [
    SqliteStore(),
    SqliteStore(salt="testsalt"),
    SqliteStore(salt="testdiff"),
    SqliteStore()
]


@pytest.mark.parametrize("test_int", examples)
def test_hash_conversions(test_int: int) -> None:
    hashes = []
    for s in stores:
        ref = s._id_to_ref(test_int)
        assert ref.value not in hashes
        hashes.append(ref.value)
        assert s._ref_to_id(ref) == test_int
