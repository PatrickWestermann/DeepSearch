import pandas as pd
from sqlalchemy import create_engine


def db_db2df(table_name, db):
    # SQLAlchemy engine
    engine = create_engine('sqlite:///' + db).connect()
    # Read table out to data frame
    df = pd.read_sql_table(table_name, engine)
    return df


if __name__ == '__main__':
    db = "data/sqlite_test.db"
    name = "TestTable"
    df = db_db2df(name, db)
    print(df)
