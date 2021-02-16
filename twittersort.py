"""
Title: PA1
Author: Jacob Voyles
Date: Feb 13th, 2021
Version: 1.0
"""

import re
import sys
import getopt
from datetime import datetime

import numpy as np
import pandas as pd

from Scanner import Scanner


# Main function that controls user interaction and calls other functions specified in the assignment description
def main(argv):
    dfs = []
    l_list = []
    input_files = []
    output_files = []

    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        print('test.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputfile> (-i <another input>) -o <outputfile> (-o <another output>)')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_files.append(arg)
        elif opt in ("-o", "--ofile"):
            output_files.append(arg)


    print("Reading files...")
    for elem in input_files:
        s_list = read_records(elem)
        l_list.append(s_list)
        dfs.append(pd.DataFrame.from_records(s_list, columns=["Usernames", "Tweets", "DateTime"]))

    highest = 0
    for x in range(len(l_list) - 1):
        if not is_more_recent(l_list[highest], l_list[x + 1]):
            highest = x + 1

    print(input_files[highest] + " contained the most recent tweet.")

    print("Merging files...")
    merge = merge_and_sort_tweets(dfs)

    print("Writing file...")
    for output in output_files:
        write_records(output, merge)

    print("File written. Displaying 5 earliest tweeters and tweets")
    for i in range(5):
        new_str = merge.iloc[-6 + i:-5 + i, 0:2].to_string(header=False, index=False)
        new_str = ' '.join((new_str.split()))
        print(new_str)


# Given a filename, this function creates a record for each line in the file and returns a list containing the records
def read_records(input_file_name):
    record_list = []
    file = open(input_file_name)

    scan = Scanner("")
    for line in file.readlines():
        list_line = []
        line = line.rstrip()
        scan.fromstring(line)

        username = scan.readtoken()

        quotes = '"{}"'.format(''.join(str(e) for e in re.findall('"([^"]*)"', line)))

        date_str = line[-19:]
        date_str = date_str.lstrip()
        date = datetime.strptime(date_str, '%Y %m %d %H:%M:%S')

        list_line.append(username)
        list_line.append(quotes)
        list_line.append(date)
        record_list.append(list_line)

        scan.readline()
    return record_list


# Compares two records based on date and time, and returns True or False
def is_more_recent(record_1, record_2):
    high1 = record_1[0][2]
    high2 = record_2[0][2]

    for x in range(len(record_1) - 1):
        past = record_1[x][2]
        present = record_1[x + 1][2]

        if present > past:
            high1 = present

    for y in range(len(record_2) - 1):
        past = record_2[y][2]
        present = record_2[y + 1][2]

        if present > past:
            high2 = present

    if high1 >= high2:
        return True
    else:
        return False


# Merges two lists of sorted records and returns a sorted list of records
def merge_and_sort_tweets(record_list):
    merge = pd.concat(record_list, ignore_index=True)
    merge = merge.sort_values(by="DateTime", ascending=False)
    return merge


# Write a list of records to an output file
def write_records(output_file_name, record_list):
    np.savetxt(output_file_name, record_list.values, delimiter=" ", newline = "\n", fmt="%s")


if __name__ == "__main__":
    main(sys.argv[1:])
