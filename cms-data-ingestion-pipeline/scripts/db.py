import psycopg2
from config.db_config import DB_CONFIG
from contextlib import contextmanager
from psycopg2.extras import RealDictCursor

# Database connection and cursor management
@contextmanager
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    except Exception as e:
        print(f"Database connection error: {e}")
    finally:
        conn.close()

@contextmanager
def get_cursor():
    with get_conn() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Cursor error: {e}")
            raise
        finally:
            cursor.close()

print("Database connection utilities are set up.")