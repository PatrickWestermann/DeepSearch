import os
import sqlite3
from sqlite3 import Error

db_file = 'deepsearch_db.sqlite'

def create_connection(db_file):
    """ create a database connection to a database that resides
        in the memory
    """
    abs_path = os.path.dirname(os.path.abspath(__file__))
    path = abs_path + '/' + db_file
    print(path)
    conn = None;
    try:
        conn = sqlite3.connect(path)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_connection(db_file)
