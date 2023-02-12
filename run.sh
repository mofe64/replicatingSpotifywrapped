#!/usr/bin/env bash

python3 json_to_csv.py --path=D:\devStrawhat\dataEngProjects\replicatingSpotifyWrapped\data\json\streamingHistory\StreamingHistoryParOne.json --output_file=streamingHistory --chunk_size=10000 --should_overwrite=False

python3 json_to_csv.py --path=D:\devStrawhat\dataEngProjects\replicatingSpotifyWrapped\data\json\streamingHistory\StreamingHistoryPartTwo.json --output_file=streamingHistory --chunk_size=10000 --should_overwrite=False
