import pytest


@pytest.fixture(scope="module")
def db():
    import fogtools.db
    return fogtools.db.FogDB()


def test_init(db):
    assert db.sat is not None
    assert db.fog is not None
