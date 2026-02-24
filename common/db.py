import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from common.config import get_settings

settings = get_settings()

pool: SimpleConnectionPool | None = None


def init_pool(minconn: int = 1, maxconn: int = 5):
    global pool
    if pool:
        return pool
    pool = SimpleConnectionPool(
        minconn,
        maxconn,
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )
    return pool


def get_conn():
    if pool is None:
        init_pool()
    return pool.getconn()


def put_conn(conn):
    if pool:
        pool.putconn(conn)


def get_db_connection():
    """Backward-compatible helper to get a single connection."""
    return get_conn()


@contextmanager
def db_cursor():
    conn = get_conn()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        put_conn(conn)
