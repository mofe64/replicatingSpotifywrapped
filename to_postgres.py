from sqlalchemy import create_engine
import pandas as pd
import argparse
from time import time


parser = argparse.ArgumentParser(description="Ingest parquet data to postgres")


def to_postgres(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table_name
    file_name = args.file_name

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # we read parquet file no chunks
    df = pd.read_parquet(
        f"data\parquet\{file_name}.parquet", engine='pyarrow')

    # create the table
    # df.head(n=0) returns just the column names for dataframe
    # we convert to sql, provide the table name, engine and if a table with such name
    # already exists we replace
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # read_parquet does not allow chunking, so we manually do that ourselves
   # split into 10 chunks
    for chunk in np.array_split(df, 10):
        t_start = time()
        # add chunk data to table
        chunk.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('inserted chunk in %.3f seconds' % (t_end - t_start))


if __name__ == '__main__':
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgress')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument(
        '--table-name', help='name of the table we will write results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    to_postgres(args)
