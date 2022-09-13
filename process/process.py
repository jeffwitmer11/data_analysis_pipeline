"""
Manifold Data Processing Application
Author: Jeff Witmer
Last Updated: September 5, 2022
Created for Manifold Inc.

About
-----
The Manifold Data Processing Application reads a dataset stored as individual
JSON files, processes the data, writes the results to a structured data set, and
displays the results of some data analysis.

Design
------
Identify all JSON files in the data folder
For each json file:
   Read each data file as a Dataframe, one at a time
   Calculate required information
   Determine which records should be processed and which should be skipped
   Perform analysis and save the results in memory
Write the processed records to disk
Print final result to console
"""
import json
import os
import sys
import pandas as pd

class DataFile:
    """A data file and metadata, initialized using the path to a JSON file"""
    def __init__(self, file_path):
        self.file_path = file_path
        self.required_cols = ["first_name", "middle_name",
                              "last_name", "zip_code"]
        self.num_processed = None
        self.num_skipped = None
        self.all_records = pd.DataFrame()
        self.processed_records = pd.DataFrame()
        self.records_info = pd.DataFrame()

    def read_data(self):
        """Read a JSON file and store all records read in"""

        df = load_json_to_df(self.file_path)

        # Downstream analysis will require certian columns to be preset. Add
        # them if they are missing.
        self.all_records = df.reindex(self.required_cols, axis=1)

        # Determine and store summary info, can be used for downstream analysis
        self.set_records_info(self.all_records)

        return self

    def process_records(self):
        """Set metadata about records and select the records to be processed"""
        self.set_records_info(self.all_records)

        df = self.all_records.loc[self.records_info["process"]]
        self.processed_records = df

        return self

    def write_output(self, file_path):
        """Save processed records to a CSV"""
        df = self.processed_records
        df.to_csv(file_path, mode='a', header=not os.path.exists(file_path))

        return self

    def determine_process_or_skip(self, df):
        """Determine records to be skipped and those to be processed

        Records are skipped if all columns are missing data. `df` should only
        contain columns intended to be used in this determination

        Returns
        -------
        A DataFrame with the same row index as `df` as two boolean columns:
        "skip" and "process" specifying if a record should be skipped or
        processed respectively.
        """
        skipped_locs = (df
                        .isnull()
                        .all(axis=1)
                        )

        df_proc_skip = pd.DataFrame(
            {"skip":skipped_locs,
             "process":~skipped_locs},
            index=df.index)

        return df_proc_skip

    def set_records_info(self, df):
        """Set information about the records"""

        df_proc_skip = self.determine_process_or_skip(df)
        df_proc_skip["file_name"] = self.file_path
        self.records_info = df_proc_skip

        self.num_processed = sum(df_proc_skip["process"])
        self.num_skipped = sum(df_proc_skip["skip"])


def process():
    """Process all JSON files stored in the data folder, write the processed
    records to CSV

    All JSON files in the data folder of the working directory are identified
    and iteratively loaded.
    Records selected to be processed are saved to the processed_data.csv.
    Descriptive analytics are calculated and displayed in the console.

    """
    data_dir = 'data'
    file_list = get_files_from_path(path=data_dir, extension='.json')
    n_files = len(file_list)

    # This program iteratively appends data to the output file. It checks if one
    # already exists prior to running, if it does, delete it. Effectively
    # overwriting any past output data files
    output_file_path = 'output/processed_data.csv'
    if os.path.exists(output_file_path):
        print("Overwriting: " + output_file_path)
        os.remove(output_file_path)

    # Initialize object to hold analyze
    zip_codes = pd.DataFrame()
    top_num_proc = pd.Series(dtype="int64")
    top_num_skip = pd.Series(dtype="int64")

    print("Processing Data...")
    for i, file_path in enumerate(file_list):

        # Progress Bar
        # https://stackoverflow.com/q/3002085
        j = (i + 1) / n_files
        sys.stdout.write('\r')
        sys.stdout.write("[%-20s] %d%%" % ('='*int(20*j), 100*j))
        sys.stdout.flush()

        # Read, Process, Write
        data = (DataFile(file_path)
                .read_data()
                .process_records()
                .write_output(output_file_path)
               )

        # Top 10 Files by Records Processed
        top_num_proc = (pd.concat([top_num_proc,
            pd.Series(data.num_processed, index=[data.file_path])])
            .nlargest(10)
            )

        # Top 10 Files by Records Skipped
        top_num_skip = (pd.concat([top_num_skip,
            pd.Series(data.num_skipped, index=[data.file_path])])
            .nlargest(10)
            )

        # Top 10 Zip Codes by Number of Unique Last Names
        zip_codes_i = (
            data.processed_records
            .groupby(by="zip_code")
            .agg({"last_name":"nunique"})
            .nlargest(10, "last_name")
        )
        zip_codes = (
            pd.concat([zip_codes, zip_codes_i])
            .nlargest(10, "last_name")
        )

    sys.stdout.write('\n')
    print("Data Processing Complete")
    print()

    # TODO: Reformat outputs
    print("Top Zip Codes by Number of Unique Last Names")
    print(zip_codes)
    print()

    print("Top Files by Records Skipped")
    print(top_num_skip)
    print()

    print("Top Files by Records Processed")
    print(top_num_proc)
    print()

    return 0


def get_files_from_path(path: str = '.', extension: str = None) -> list:
    """return list of files from path"""
    # https://stackoverflow.com/q/68327646

    result = []
    for subdir, dirs, files in os.walk(path):
        for filename in files:
            filepath = subdir + os.sep + filename
            if extension is None:
                result.append(filepath)
            elif filename.lower().endswith(extension.lower()):
                result.append(filepath)
    return result

def load_json_to_df(file_path):
    """Load a JSON file to a pandas DataFrame"""
    # TODO change this to match extention in get files from path
    file_extention = os.path.splitext(file_path)[-1].lower()
    if file_extention != ".json":
        raise ValueError('File path has invalid file extention. File extention must be ".json"')

    with open(file_path) as file:
        file_text = file.read()
    # TODO add comment about why I am not using eval
    # TODO add comments explaining double encoding
    if file_text == '':
        return pd.DataFrame()

    else:
        json_dict = json.loads(file_text)
        if isinstance(json_dict, str):
            json_dict = json.loads(json_dict)

        return pd.DataFrame.from_dict(json_dict)


if __name__ == "__main__":
    process()

    file_path = "17259.json"

    with open(file_path) as file:
        file_text = file.read()
    json_load_1 = json.loads(file_text)
    print(json_load_1)
    isinstance(json_load_1, str)

    """
    dict_not_encoded = [{"A":"foo", "B":123}, {"C": "bar", "A":"baz"}]
    encoded_data = json.dumps(dict_not_encoded)
    double_encoded_data = json.dumps(encoded_data)

    file_path = "17259.json"
    with open(file_path, "a") as file:
        file_text = file.write(encoded_data)

    with open(file_path) as file:
        file_text = file.read()
    json_load_1 = json.loads(file_text)
    print(json_load_1)
    isinstance(json_load_1, str)
    """

    """
    load_json_to_df("17259.json")
    load_json_to_df("data/altius/group00/client00/13583.json")
    load_json_to_df("sample2.json")

    file_path = "data/altius/group00/client00/13583.json"
    with open(file_path) as file:
        json_str = file.read()
    pd.read_json(json_str, orient='records')

    file_path = "17259.json"

    with open(file_path) as file:
        json_str = file.read()
    df = pd.DataFrame(eval(json_str)))

    eval(json_str)

    df = pd.DataFrame(eval("[{'code': '8', 'name': 'Human'}, {'code': '11', 'name': 'Orc'}]"))
    df
    """