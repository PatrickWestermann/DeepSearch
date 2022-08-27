import os
import sqlite3
from sqlite3 import Error
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import event
import pandas as pd


class ToolKit:
    """Database Toolkit for sqlite database with big data tables"""

    def __init__(self):
        print("Toolkit functions: create_db")
        print("Please use the full path to your database")

    def create_db(self, db):
        """ create a database """
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_name)
            print(sqlite3.version)
        except Error as e:
            print(e)
        finally:
            if self.conn:
                self.conn.close()

    def connect_db(self, db):
        """ create a database connection to a database that resides
            in the memory
        """
        #abs_path = os.path.dirname(os.path.abspath(__file__))
        #path = abs_path + '/data/' + db_file
        #print(path)
        self.engine = create_engine('sqlite:///'+db, echo=False)
        return self.engine

    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(self, engine):
        self.engine = engine
        self.cursor = self.engine.cursor()
        self.cursor.execute("PRAGMA journal_mode=WAL")
        self.cursor.execute("PRAGMA synchronous=normal")
        self.cursor.execute("PRAGMA temp_store=memory")
        self.cursor.execute("PRAGMA mmap_size=30000000000")
        self.cursor.close()
        return print("Pragma arguments added")

    def disconnect_db(engine):
        self.engine = engine
        self.engine.close()

    def csv_to_db(self, engine, csv_file, table_name):
        # creating a data frame
        self.df = pd.read_csv(csv_file)
        """Take a data frame and add it to a sqlite db"""
        self.df = self.df.applymap(str)
        self.df.to_sql(str(table_name), con=engine, if_exists='replace')
        engine.close()

    def db_db2df(table_name, db):
        # SQLAlchemy engine
        engine = create_engine('sqlite:///' + db).connect()
        # Read table out to data frame
        df = pd.read_sql_table(table_name, engine)
        return df
