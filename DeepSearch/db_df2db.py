from sqlite3 import Error
import sqlite3
import pandas as pd
import os


def db_df2db(db_file, csv_file, table_name):
    # creating a data frame
    df = pd.read_csv(csv_file)
    """Take a data frame and add it to a sqlite db"""
    df = df.applymap(str)
    engine = create_engine('sqlite:///'+db_file, echo=False)
    df.to_sql(str(table_name), con=engine, if_exists='replace')
    engine.close()


if __name__ == '__main__':
    db_file = 'deepsearch.db'
    abs_path = os.path.dirname(os.path.abspath(__file__))
    db_file = abs_path + '/data/' + db_file
    csv_file = '/home/hal9000/Downloads/tokenized.csv'
    table_name = 'tokenized'
    db_df2db(db_file, csv_file, table_name)
