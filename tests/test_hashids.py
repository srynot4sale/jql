import pytest

from jql.memory import MemoryStore


examples = [1, 2, 3, 5, 10, 35, 543, 1234, 5567, 87678]

stores = [
    MemoryStore(),
    MemoryStore(salt="testsalt"),
    MemoryStore(salt="testdiff"),
    MemoryStore()
]


@pytest.mark.parametrize("test_int", examples)
def test_hash_conversions(test_int: int) -> None:
    hashes = []
    for s in stores:
        ref = s.id_to_ref(test_int)
        assert ref.value not in hashes
        hashes.append(ref.value)
        assert s.ref_to_id(ref) == test_int
