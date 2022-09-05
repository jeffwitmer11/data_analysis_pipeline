import json
import os
import sys
import pandas as pd

class data_file:
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
        
        data = (data_file(file_path)
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

    print(zip_codes)
    print(top_num_skip)
    print(top_num_proc)

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
    f = open(file_path)
    s = f.read()
    f.close()
    
    data0 = json.loads(s)
    data1 = json.loads(data0)
    df = pd.DataFrame.from_dict(data1)
    return df


if __name__ == "__main__":
    process()

    df = pd.DataFrame({"A":[1,2,3], "B":[2,None,4]})
    print(df)
    df.isnull()

    # pd.Series(6, index=["hello/my/file"])

    # df = pd.DataFrame({"A":[1.5,2.3,3.6], "B":[4,5,6], "C":[7.1,8.8,9.3]})
    # df
    # cols = ["A", "C", "D"]
    # df[set(cols).intersection(df.columns)].apply(round)
    # df.all(axis=1)

    # df2 = df.reindex(cols, axis=1)
    # df2
    # df2.isnull().all(axis=1)
    # process()

    # n1 = 3
    # n2 = 4

    # n_list = pd.Series(dtype="int64")
    # n_list = pd.concat([n_list, pd.Series(n1)])
    # n_list
    # n_list = pd.concat([n_list, pd.Series(n2)])
    # n_list
    # n_list.nlargest(1)

    # dat1 = data_file("data/altius/group00/client00/36526.json")
    # dat1.read_to_df()
    # dat1.process_records()
    # #dat1.write_to_file()
    
    # dat2 = data_file("data/chemecca/study00/org00/set00/15761.json")
    # dat2.read_to_df()
    # dat2.process_records()

    # dat2.processed_records

    # zip_codes = pd.DataFrame()
    # zip_codes_i = (dat2.processed_records
    #         .groupby(by = "zip_code")
    #         .agg({"last_name":"nunique"})
    #         .sort_values(by = "last_name", ascending=False)
    #         .iloc[0:9]
    #     )
    # zip_codes_i

    # zip_codes = (zip_codes.append(zip_codes_i)
    #         .sort_values(by = "last_name", ascending=False))
    #         #.iloc[0:9])

    # zip_codes

    # df = pd.DataFrame()
    # df.empty
    # )
    # file1 = data_file()

    # df_proc = process()
    # df_proc_skipped = write_or_skip(df_proc)

    # print(df_proc)
    # df_proc.to_csv('processed_data.csv')
    # df_proc_skipped.to_csv('processed_data_skipped.csv')
    
    # len(df_proc)
    # len(df_proc_skipped)


    # q1_df = q1_fun(df_proc_skipped)
    # q1_df.to_csv('q1_ans.csv')

    # q2_df = q2_fun(df_proc_skipped)
    # q2_df.to_csv('q2_ans.csv')

    # q3_df = q3_fun(df_proc_skipped)
    # q3_df.to_csv('q3_ans.csv')


    # dat1 = data_file("data/altius/group00/client00/36526.json")
    # dat1.records
    # dat1.records_info
    # print(dat1.num_processed)
    # print(dat1.num_skipped)

    # df1 = dat1.read_to_df()
    # dat1.records
    # dat1.records_info
    # print(dat1.num_processed)
    # print(dat1.num_skipped)

    # dat1.write_to_file("test")

    # dat2 = data_file("data/altius/group00/client00/36526.json")
    # dat2.read_to_df()
    # dat2.write_to_file("test")

    # dat1.records
    # dat1.records_info
    # print(dat1.num_processed)
    # print(dat1.num_skipped)


    # whatamI = df1["last_name"].isnull()
    # whatamI.index
    # print(dat1.records_processed)

    # q2 = (df_proc
    #     .groupby(by = "file_name")
    #     .agg({"skip":"sum"})
    #     #.sort_values(by = "skip", ascending=False)
    #     .iloc[0:9]
    #     )

    

    #with open("data/altius/group00/client00/36526.json", 'r') as f:
     #   json_file = json.loads(f)
      #  df = pd.read_json(json_file)

    

    # import json
    # import pandas as pd
  
    # # Opening JSON file
    # file_path = 'data/altius/group00/client00/36526.json'
    

    # for i,l in enumerate(data1):
    #     print(i)
    #     print(l)
    #     for key, value in recursive_items(l):
    #         print(key, value)



    # my_dict = {
    #     "key1":"foo",
    #     "key2":{"key1":"bar", "key3":"baz"},
    #     "key3":"hello"
    # }

    # for i in my_dict:
    #     print(i)

    
    

    # print(df)

    # import os
    # data_dir = 'data'
    # file_list = get_files_from_path(path=data_dir, extension='.json')

    # total_data = pd.DataFrame()
    # for file_path in file_list:
    #     print(file_path)
    #     df = load_json_to_df(file_path)


    #     #df["skip"] = df["first_name"].isnull() & df["middle_name"].isnull() & df["last_name"].isnull() & df["zip_code"].isnull()
    #     df["file_name"] = file_path
    #     total_data = pd.concat([total_data, df])

    # len(total_data)

    # # Question 1
    # df = total_data
    # df["skip"] = df["first_name"].isnull() & df["middle_name"].isnull() & df["last_name"].isnull() & df["zip_code"].isnull()
    # df["process"] = ~df["skip"]
    
    # q1 = (df
    #     .groupby(by = "file_name")
    #     .agg({"process":"sum"})
    #     .sort_values(by = "process", ascending=False)
    #     .iloc[0:9]
    #     )
    # print(q1)

    # q2 = (df
    #     .groupby(by = "file_name")
    #     .agg({"skip":"sum"})
    #     .sort_values(by = "skip", ascending=False)
    #     .iloc[0:9]
    #     )
    # print(q2)

    # q3 = (df
    #     .groupby(by = "zip_code")
    #     .agg({"last_name":"nunique"})
    #     .sort_values(by = "last_name", ascending=False)
    #     .iloc[0:9]
    #     )
    # print(q3)

    # df.columns

#jsonlist = []
#for filepath in filelist:
 #   with open(filepath) as infile:
  #      jsonlist.append(json.load(infile)
#from pprint import pprint
#pprint(jsonlist)