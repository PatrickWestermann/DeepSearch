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
import pandas as pd
import pickle
import glob


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
        engine = create_engine(
            'sqlite:///'+db, echo=False)
        print(f"Database engine created >> {db}")

    def connect_db(self, db):
        """Create and run engine running db"""
        engine = create_engine('sqlite:///'+db, echo=False)
        print(f"Start engine >> {db}")
        return engine

    def disconnect_db(self, engine):
        engine.close()
        print(f"Disconnected from database engine")

    def create_tbl(self, engine, tbl_name):
        print("Not implemented yet.")

    def csv_to_db(self, engine, csv_file, tmp_path, tbl_name, chunk_size=400000, on_exists='replace'):
        """Take a data frame from csv file and add it to a sqlite db as table"""
        separator = ","
        # Read csv in chunks saved as pickles in tmp_path
        reader = pd.read_csv(csv_file, sep=separator,
                             chunksize=chunk_size,
                             encoding='ISO-8859-1',
                             low_memory=False)
        for i, chunk in enumerate(reader):
            tmp_path = tmp_path + "/data_{}.pkl".format(i+1)
            with open(csv_file, "wb") as f:
                pickle.dump(chunk, f, pickle.HIGHEST_PROTOCOL)
        # Creating a data frame from the pickles
        # Get list of pickle files
        data_p_files = []
        for name in glob.glob(tmp_path + "/data_*.pkl"):
            data_p_files.append(name)
        # Create data frame and add data from pickle files
        data = pd.DataFrame([])
        for i in range(len(data_p_files)):
            data = data.append(pd.read_pickle(
                data_p_files[i]), ignore_index=True)
        # Add data frame to
        data.to_sql(str(tbl_name), con=engine, if_exists=on_exists)
        print(f"Data saved in table {tbl_name} of {engine}.")

    def tbl_to_df(self, db, tbl_name):
        # SQLAlchemy engine
        engine = create_engine('sqlite:///' + db).connect()
        # Read table out to data frame
        df = pd.read_sql_table(tbl_name, engine)
        return df

    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=normal")
        cursor.execute("PRAGMA temp_store=memory")
        cursor.execute("PRAGMA mmap_size=30000000000")
        cursor.close()
        return print("Pragma arguments added")
