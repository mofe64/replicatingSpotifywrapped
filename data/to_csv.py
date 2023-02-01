import pandas as pd
import json
import chardet
import os
import argparse


parser = argparse.ArgumentParser(description="Ingest csv data to postgres")

# params we need to collect
# path to file
# if we should process json in chunks
# chunk size
# output file name


def to_csv(args: dict) -> None:
    # store string as raw string to avoid path name issues
    # path = r"D:\devStrawhat\dataEngProjects\replicatingSpotifyWrapped\data\json\streamingHistory\StreamingHistoryPartOne.json"
    # output_file = "StreamingHistoryPartOne"
    path = args.path
    output_file = args.output_file
    chunk_size = args.chunk_size
    output_path = f"csv\{output_file}.csv"
    # detect json file's encoding in order to to avoid encoding issues when opening the json file
    # we will pass the encoding to the open func
    enc = chardet.detect(open(path, 'rb').read())['encoding']
    with open(path, encoding=enc) as infile:
        json_data = json.load(infile)
        for i in range(0, len(json_data), chunk_size):
            print(f"processing chunk {i}")
            x = json_data[i:i+chunk_size]
            print(f"extracted chunk {i}")
            print("converting to dataframe")
            df = pd.DataFrame(x)
            print("writing to csv")
            df.to_csv(output_path, mode='a',
                      header=not os.path.exists(output_path))
            print(f"write to csv complete for chunk {i}")


if __name__ == '__main__':
    parser.add_argument('--path', help='path to json file')
    parser.add_argument('--output_file', help='password for postgress')
    parser.add_argument(
        '--chunk_size', help='size of chunks in which to process data')

    args = parser.parse_args()

    to_csv(args)
