import json
import sys
import os
from os import listdir
from os.path import isfile, join

def main():
    file_in = sys.argv[1]
    file_out = sys.argv[2]

    raw = []
    with open(file_in, "r") as fin:
        for line in fin:
            raw.append(line.strip())
    
    parsed = [(json.loads(json_in), json_in) for json_in in raw]
    to_sort = [(obj['visitStartTime'], json_in) for (obj, json_in) in parsed]
    sorted_objects = sorted(to_sort)
    to_save = [json_in for (t, json_in) in sorted_objects]

    with open(file_out, "w") as fout:
        fout.write('\n'.join(to_save))
        fout.write('\n')

if __name__ == '__main__': 
    main()
