#!/usr/bin/python
import os
import sqlite3
from sqlite3 import Error
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import event
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import connectorx as cx


class ToolKit:
    """Database Toolkit for sqlite database with big data tables"""

    def __init__(self):
        print("Toolkit functions: ")
        print("     create_db(database)")
        print("     create_tbl(engine, tbl_name)")
        print("     connect_db(database)")
        print("     disconnect_db(engine)")
        print("     csv_to_db(engine, csv_file, tbl_name)")
        print("     tbl_to_df(db, tbl_name)")
        print("Please use the full path to your database and your engine")
        print("variable as return of connect_db for engine argument")

    def create_db(self, db):
        """ create a database """
        self.db = db
        self.engine = create_engine(
            'sqlite:///'+self.db, echo=False)
        print(f"Connected to database >> {db}")
        try:
            with self.engine.connect() as self.conn:
                self.conn.execute("SELECT 1")
            print('Engine is valid')
        except Exception as e:
            print(f'Engine invalid: {str(e)}')

    def connect_db(self, db):
        """ create a database connection to a database that resides
            in the memory
        """
        self.db = db
        self.engine = create_engine(
            'sqlite:///'+self.db, echo=False)
        print(f"Connected to database >> {db}")
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            print('Engine is valid')
        except Exception as e:
            print(f'Engine invalid: {str(e)}')

    def disconnect_db(self, engine):
        engine.close()
        print(f"Disconnected from database")

    def create_tbl(self, engine, tbl_name):
        try:
            self.engine.connect()
            print("Connection successful")
        except SQLAlchemyError as err:
            print("error", err.__cause__)  # this will give what kind of error

    def csv_to_db(self, engine, csv_file, tbl_name, on_exists='replace'):
        # creating a data frame
        self.df = pd.read_csv(csv_file)
        """Take a data frame and add it to a sqlite db"""
        self.df = self.df.applymap(str)
        self.df.to_sql(str(tbl_name), con=engine, if_exists=on_exists)
        engine.close()

    def tbl_to_df(self, db, tbl_name):
        # SQLAlchemy engine
        engine = create_engine('sqlite:///' + db).connect()
        # Read table out to data frame
        df = pd.read_sql_table(tbl_name, engine)
        return df

    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        self.cursor = self.dbapi_connection.cursor()
        self.cursor.execute("PRAGMA journal_mode=WAL")
        self.cursor.execute("PRAGMA synchronous=normal")
        self.cursor.execute("PRAGMA temp_store=memory")
        self.cursor.execute("PRAGMA mmap_size=30000000000")
        self.cursor.close()
        return print("Pragma arguments added")
