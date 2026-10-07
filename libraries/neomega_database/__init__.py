"""Installation-scoped SQL adapters; callers own SQL and migrations."""
from contextlib import contextmanager
from pathlib import Path
import re
import sqlite3
import threading

__version__ = '1.0.0'


class Database:
    """Synchronous DB-API connection. Use within one managed blocking task.

    Transactions serialize through this object. A connection is never shared
    across installations; close it in the consumer plugin's on_stop hook.
    """
    def __init__(self, connection):
        self._connection = connection
        self._lock = threading.RLock()
        self._active = False
        self._closed = False

    @contextmanager
    def transaction(self):
        with self._lock:
            if self._closed:
                raise RuntimeError('database is closed')
            if self._active:
                raise RuntimeError('nested transactions are not supported')
            self._active = True
            cursor = self._connection.cursor()
            try:
                cursor.execute('BEGIN')
                yield Session(cursor)
                self._connection.commit()
            except BaseException:
                self._connection.rollback()
                raise
            finally:
                cursor.close()
                self._active = False

    def close(self):
        with self._lock:
            if self._active:
                raise RuntimeError('cannot close an active transaction')
            if not self._closed:
                self._connection.close()
                self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Session:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, parameters=()):
        """Bind values (? in SQLite, %s in MySQL); never interpolate values."""
        if not isinstance(sql, str) or not isinstance(parameters, (tuple, list, dict)):
            raise TypeError('SQL requires text and bound parameters')
        self._cursor.execute(sql, parameters)
        return self._cursor.rowcount

    def executemany(self, sql, rows):
        self._cursor.executemany(sql, rows)
        return self._cursor.rowcount

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchmany(self, size=100):
        if type(size) is not int or size < 1:
            raise ValueError('size must be positive')
        return self._cursor.fetchmany(size)


def sqlite(ctx, name='application', *, timeout=5.0):
    """Open only <installation data>/databases/<name>.sqlite3, never Host KV."""
    if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,63}', name):
        raise ValueError('invalid database name')
    base = Path(ctx.data_dir).resolve()
    root = base / 'databases'
    root.mkdir(mode=0o700, exist_ok=True)
    if root.is_symlink() or root.resolve().parent != base:
        raise ValueError('database directory must belong to the installation')
    path = root / (name + '.sqlite3')
    if path.is_symlink():
        raise ValueError('database file cannot be a symlink')
    connection = sqlite3.connect(path, timeout=timeout, isolation_level=None, check_same_thread=False)
    try:
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA journal_mode=WAL')
        path.chmod(0o600)
    except BaseException:
        connection.close()
        raise
    return Database(connection)


def mysql(ctx, *, host, user, database, password_ref, port=3306, timeout=5, ssl=None, connector=None):
    """Read an installation secret; connector follows pymysql.connect contract.

    PyMySQL is an optional consumer release dependency, never installed at run
    time. The database user must be restricted to this installation's schema.
    """
    if connector is None:
        import pymysql
        connector = pymysql.connect
    password = ctx.secrets.get(password_ref)
    connection = connector(host=host, user=user, password=password, database=database,
                           port=port, connect_timeout=timeout, read_timeout=timeout,
                           write_timeout=timeout, autocommit=False, charset='utf8mb4', ssl=ssl)
    return Database(connection)
