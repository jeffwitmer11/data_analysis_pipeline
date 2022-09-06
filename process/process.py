import json
import os
import sys
import pandas as pd

class DataFile:
    def __init__(self, file_path):
        self.file_path = file_path
        self.required_cols = ["first_name", "middle_name", "last_name", "zip_code"]
        self.num_processed = None
        self.num_skipped = None
        self.all_records = pd.DataFrame()
        self.processed_records = pd.DataFrame()
        self.records_info = pd.DataFrame()

    def read_data(self):
        df = load_json_to_df(self.file_path)

        self.all_records = df.reindex(self.required_cols, axis=1)
        self.set_records_info(self.all_records)

        return self

    def process_records(self):
        self.set_records_info(self.all_records)

        df = self.all_records.loc[self.records_info["process"]] 
        self.processed_records = df

        return self

    def write_output(self, file_path):
        df = self.processed_records
        df.to_csv(file_path, mode='a', header=not os.path.exists(file_path))

        return self

    def determine_process_or_skip(self, df):
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
        df_proc_skip = self.determine_process_or_skip(df)
        df_proc_skip["file_name"] = self.file_path
        self.records_info = df_proc_skip

        self.num_processed = sum(df_proc_skip["process"])
        self.num_skipped = sum(df_proc_skip["skip"])


def process():
    data_dir = 'data'
    file_list = get_files_from_path(path=data_dir, extension='.json')
    n_files = len(file_list)

    output_file_path = 'processed_data_test.csv'
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    zip_codes = pd.DataFrame()
    top_num_proc = pd.Series(dtype="int64")
    top_num_skip = pd.Series(dtype="int64")

    for i, file_path in enumerate(file_list):

        # Prgress Bar
        # https://stackoverflow.com/q/3002085
        j = (i + 1) / n_files
        sys.stdout.write('\r')
        sys.stdout.write("[%-20s] %d%%" % ('='*int(20*j), 100*j))
        sys.stdout.flush()

        data = (DataFile(file_path)
            .read_data()
            .process_records()
            .write_output(output_file_path)
        )

        # Top Files by Records Processed
        top_num_proc = (pd.concat([top_num_proc, 
            pd.Series(data.num_processed, index=[data.file_path])])
            .nlargest(10)
            )

         # Top Files by Records Skipped
        top_num_skip = (pd.concat([top_num_skip, 
            pd.Series(data.num_skipped, index=[data.file_path])])
            .nlargest(10)
            )

        # Top Zip Codes by Number of Unique Last Names
        zip_codes_i = (data.processed_records
            .groupby(by = "zip_code")
            .agg({"last_name":"nunique"})
            .nlargest(10, "last_name")
        )

        zip_codes = (pd.concat([zip_codes, zip_codes_i])
           .nlargest(10, "last_name")
        )

    sys.stdout.write('\r')
    print("Data Loading complete")
    print()

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


def get_files_from_path(path: str='.', extension: str=None) -> list:

    """return list of files from path"""
    # https://stackoverflow.com/q/68327646

    result = []
    for subdir, dirs, files in os.walk(path):
        for filename in files:
            filepath = subdir + os.sep + filename
            if extension == None:
                result.append(filepath)
            elif filename.lower().endswith(extension.lower()):
                result.append(filepath)
    return result

def load_json_to_df(file_path):

    # To do add with
    file = open(file_path)
    file_text = file.read()
    file.close()

    json_load_1 = json.loads(file_text)
    json_load_2 = json.loads(json_load_1)
    df = pd.DataFrame.from_dict(json_load_2)
    return df


if __name__ == "__main__":
    process()
