import sqlite3


def create_connection():
    """Create a database connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect('Datasets/mental_health.sqlite')
        return conn
    except sqlite3.Error as e:
        print(e)


def close_connection(conn):
    """Close the database connection."""
    if conn:
        conn.close()
