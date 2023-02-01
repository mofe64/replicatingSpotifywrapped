import pandas as pd
import json
import chardet
import os
import argparse


parser = argparse.ArgumentParser(description="Convert Json data to csv")

# params we need to collect
# path to file
# if we should process json in chunks
# chunk size
# output file name


def to_csv(args: dict) -> None:
    print(f"recieved args {args}")
    path = args.path
    output_file = args.output_file
    chunk_size = int(args.chunk_size)
    output_path = f"data\csv\{output_file}.csv"
    should_overwrite = args.should_overwrite

   # if we are overwriting, we check if file exists and then delete it since our write operation is in append mode
   # and will create the file if it does not exist
    if should_overwrite is None:
        if (os.path.isfile(output_path)):
            os.remove(output_path)
    elif should_overwrite.lower() in ['true'] or should_overwrite is None:
        if (os.path.isfile(output_path)):
            os.remove(output_path)

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

    parser.add_argument('--should_overwrite')

    args = parser.parse_args()

    to_csv(args)
