import pytest
import asyncio


@pytest.fixture
def connect(make_connection):

    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from make_connection(**kwargs)
        conn2 = yield from make_connection(**kwargs)
        cur = yield from conn2.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS foo")
        yield from conn2.close()
        return conn

    return go


@asyncio.coroutine
def test_connection_on_server_restart(connect, pg_server, docker):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret

    docker.restart(container=pg_server['Id'])

    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret
