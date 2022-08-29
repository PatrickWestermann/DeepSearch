#!/usr/bin/python

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
        print("Toolkit functions: ")
        print("     create_db(database)")
        print("     connect_db(database)")
        print("     disconnect_db(engine)")
        print("     csv_to_db(engine, csv_file, tbl_name)")
        print("     tbl_to_df(db, tbl_name)")
        print("Please use the full path to your database and your engine")
        print("variable as return of connect_db for engine argument")

    def create_db(self, db):
        """ create a database """
        self.conn = None
        try:
            self.conn = sqlite3.connect(db)
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

    def disconnect_db(self, engine):
        self.engine = engine
        self.engine.close()

    def csv_to_db(self, engine, csv_file, tbl_name):
        # creating a data frame
        self.df = pd.read_csv(csv_file)
        """Take a data frame and add it to a sqlite db"""
        self.df = self.df.applymap(str)
        self.df.to_sql(str(tbl_name), con=engine, if_exists='replace')
        engine.close()

    def tbl_to_df(self, db, tbl_name):
        # SQLAlchemy engine
        engine = create_engine('sqlite:///' + db).connect()
        # Read table out to data frame
        df = pd.read_sql_table(tbl_name, engine)
        return df
