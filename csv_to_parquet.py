import pandas as pd
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


parser = argparse.ArgumentParser(description="Convert csv data to parquet")


def csv_to_parquet(args: dict) -> None:
    path = args.path
    output_file = args.output_file
    chunk_size = args.chunk_size
    output_path = f"data\parquet\{output_file}.parquet"

    pqwriter = None
    print("reading csv file")
    try:
        for i, df in enumerate(pd.read_csv(path, chunksize=int(chunk_size))):
            # remove values with a stream time of 0
            df = df[df.msPlayed != 0]
            # get rid of first row which contains the column names
            df.drop(df.head(1).index, inplace=True)

            # manually set the type of the msPlayed Column to int64
            df['msPlayed'] = pd.to_numeric(df['msPlayed'])
            df['endTime'] = pd.to_datetime(df['endTime'])
            df['artistName'] = df['artistName'].astype(str)
            df['trackName'] = df['trackName'].astype(str)
            table = pa.Table.from_pandas(df)

            if i == 0:
                # create a parquet writer object and give it an output file
                print("setting schema and file destination")
                pqwriter = pq.ParquetWriter(output_path, table.schema)

            print(f"writing chunk {i+1}")
            pqwriter.write_table(table)
            print(f"chunk {i+1} write complete")

    except Exception as e:
        print("Error converting files")
        print(str(e))
    finally:
        if pqwriter:
            print("closing pq writer...")
            pqwriter.close()
            print(
                "View --------------------------------------------------------------------------------------\n")
            df = pd.read_parquet(output_path, engine='pyarrow')
            print(
                "head --------------------------------------------------------------------------------------")
            print(df.head(10))
            print("\n")
            print(
                "tail --------------------------------------------------------------------------------------")
            print(df.tail(10))


if __name__ == '__main__':
    parser.add_argument('--path', help='path to json file')
    parser.add_argument('--output_file', help='password for postgress')
    parser.add_argument(
        '--chunk_size', help='size of chunks in which to process data')

    args = parser.parse_args()
    print(f"recieved follwoung args {args}")
    csv_to_parquet(args)


# python csv_to_parquet.py --path=D:\devStrawhat\dataEngProjects\replicatingSpotifyWrapped\data\csv\streamingHistory.csv --output_file=streamingHistory --chunk_size=10000
