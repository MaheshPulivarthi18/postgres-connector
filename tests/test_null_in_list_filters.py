"""inq / nin with None in the list: None means IS NULL, as everywhere else in the filter DSL."""
import pytest

from postgres_connector import Field, PostgreSQLModel


class NlUser(PostgreSQLModel):
    __table__ = "nl_users"
    id:   int = Field(primary_key=True)
    name: str = Field()
    age:  int = Field(nullable=True)


@pytest.fixture
async def users(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS nl_users")
    await connector.migrate()
    await connector.create_many(NlUser, [{"name": "a", "age": 1}, {"name": "b", "age": None}, {"name": "c", "age": 3}])
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS nl_users")


async def names(connector, where):
    return [u.name for u in await connector.find(NlUser, {"where": where, "order": ["name ASC"]})]


@pytest.mark.parametrize(
    "where, expected",
    [
        ({"age": {"nin": [1, None]}}, ["c"]),
        ({"age": {"inq": [1, None]}}, ["a", "b"]),
        ({"age": {"nin": [None]}}, ["a", "c"]),
        ({"age": {"inq": [None]}}, ["b"]),
        ({"age": {"nin": [1]}}, ["c"]),  # unchanged: SQL NOT IN excludes NULL rows
        ({"age": {"inq": []}}, []),
        ({"age": {"nin": []}}, ["a", "b", "c"]),
    ],
)
async def test_none_in_inq_and_nin(connector, users, where, expected):
    assert await names(connector, where) == expected
