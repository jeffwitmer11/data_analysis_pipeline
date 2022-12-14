"""
Manifold Data Processing Application
Author: Jeff Witmer
Last Updated: September 18, 2022
Created for Manifold Inc.

About
-----
The Manifold Data Processing Application reads a dataset, stored as individual
JSON files, processes the data, writes the results to a structured data set, and
displays the results of some data analysis.

Design
------
1) Identify all JSON files in the data folder
2) For each json file:
    * Read each data file as a Dataframe, one at a time
    * Calculate required information
    * Determine which records should be processed and which should be skipped
    * Perform analysis and save the results in memory
    * Write the processed records to disk
3) Write final analysis results to disk
"""
import os
import sys
import json
import warnings
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

        df = self.load_json()

        # Downstream analysis will require certian columns to be preset. Add
        # them if they are missing.
        self.all_records = df.reindex(self.required_cols, axis=1)

        return self

    def process_records(self):
        """Set metadata about records and select the records to be processed"""
        self.set_records_info()

        df = self.all_records.loc[self.records_info["process"]]
        self.processed_records = df

        return self

    def write_output(self, file_path):
        """Save processed records to a CSV"""
        df = self.processed_records
        df.to_csv(file_path, mode='a', header=not os.path.exists(file_path))

        return self

    def load_json(self):
        """Read, deserialize, and normalize a JSON file"""
        json_decoded = decode_json_string(self.file_path)
        df = normalize_json_with_cols(json_decoded, self.required_cols)
        return df

    def determine_process_or_skip(self):
        """Determine records to be skipped and those to be processed

        Records are skipped if all columns are missing data. `df` should only
        contain columns intended to be used in this determination

        Returns
        -------
        A DataFrame with the same row index as `df` as two boolean columns:
        "skip" and "process" specifying if a record should be skipped or
        processed respectively.
        """
        df = self.all_records
        skipped_locs = (df
                        .isnull()
                        .all(axis=1)
                        )

        df_proc_skip = pd.DataFrame(
            {"skip":skipped_locs,
             "process":~skipped_locs},
            index=df.index)

        return df_proc_skip

    def set_records_info(self):
        """Set information about the records"""

        df_proc_skip = self.determine_process_or_skip()
        df_proc_skip["file_name"] = self.file_path
        self.records_info = df_proc_skip

        self.num_processed = sum(df_proc_skip["process"])
        self.num_skipped = sum(df_proc_skip["skip"])


def process(input_path, output_path):
    """Process all JSON files stored in a folder, write the processed
    records to a CSV.

    All JSON files in the folder of the working directory are identified
    and iteratively loaded (one at a time).
    Records selected to be processed are saved to the to a csv.
    Descriptive analytics are calculated and displayed in the console.

    """
    file_list = get_files_from_path(path=input_path, extension='.json')
    n_files = len(file_list)

    # This program iteratively appends data to the output file. It checks if the output file
    # already exists prior to running, if it does, delete it. Effectively
    # overwriting any past output data files
    output_file_path = os.path.join(output_path, 'processed_records.csv')
    if os.path.exists(output_file_path):
        print("Overwriting: " + str(output_file_path))
        os.remove(output_file_path)

    # Initialize objects to hold the analysis results
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
    print("Results saved to: " + output_file_path)

    zip_codes.rename(columns={"last_name":"num_last_names"}, inplace=True)
    zip_codes.to_csv(os.path.join(output_path, 'zip_codes.csv'))

    top_num_skip.to_csv(os.path.join(output_path, "records_skipped.csv"),
        index_label = "file", header = ["num_skipped"])

    top_num_proc.to_csv(os.path.join(output_path, "records_processed.csv"),
        index_label = "file", header = ["num_processed"])

    print("Data Analysis Processing Complete")
    print("Results saved to: " + str(output_path))

    return 0

def get_files_from_path(path: str = '.', extension: str = None) -> list:
    """return list of files from path"""
    # https://stackoverflow.com/q/68327646

    result = []
    for subdir, _, files in os.walk(path):
        for filename in files:
            filepath = subdir + os.sep + filename
            if extension is None:
                result.append(filepath)
            elif filename.lower().endswith(extension.lower()):
                result.append(filepath)
    return result

def decode_json_string(file_path):
    """ This a docstring """
    file_extention = os.path.splitext(file_path)[-1].lower()
    if file_extention != ".json":
        raise ValueError('File path has invalid file extention. File extention must be ".json"')

    with open(file_path, "r", encoding="utf-8") as file:
        file_text = file.read()

    if file_text == '':
        json_decoded = {}

    # Deserialize the JSON string
    else:
        try:
            json_decoded = json.loads(file_text)
        except json.decoder.JSONDecodeError:
            warnings.warn("Warning: Could not decode JSON file at " + str(file_path))
            json_decoded = {}

        # The client's data is often stored as double encoded JSON strng.
        # We choose not to `eval` this string inorder to mitigate code injection.
        # Rather than `eval` we can just deserialze the srting a second time.
        if isinstance(json_decoded, str):
            try:
                json_decoded = json.loads(json_decoded)
            except json.decoder.JSONDecodeError:
                warnings.warn("Warning: Could not decode JSON file at " + str(file_path))
                json_decoded = {}

    return json_decoded

def normalize_json_with_cols(json_decoded, required_cols=None):
    """ This a docstring """

    df = pd.json_normalize(json_decoded)

    if required_cols is None:
        required_cols = df.columns

    if df.empty:
        df = df.reindex(required_cols, axis=1)

    else:
        for col in required_cols:
            matched_cols = df.filter(like=col).columns
            if any(matched_cols):
                df[col] = df[matched_cols].bfill(axis=1).iloc[:, 0]

    return df


if __name__ == "__main__":
    process("data", "output")
