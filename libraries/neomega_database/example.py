from neomega_database import sqlite


def save_setting(ctx, name, value):
    with sqlite(ctx) as db:
        with db.transaction() as tx:
            tx.execute('CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)')
            tx.execute('INSERT INTO settings(name,value) VALUES (?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value', (name, value))
            tx.execute('SELECT value FROM settings WHERE name=?', (name,))
            return tx.fetchone()[0]
