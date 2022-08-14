from sqlite3 import Error
import sqlite3


def db_create(db_name):
    """ create a database """
    name = "data/" + db_name
    conn = None
    try:
        conn = sqlite3.connect(name)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    db_create(r"sqlite_test.db")
