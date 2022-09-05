import json
import os
import pandas as pd

class data_file:
    def __init__(self, file_path):
        self.file_path = file_path
        self.num_processed = None
        self.num_skipped = None
        self.records = pd.DataFrame()
        self.records_info = pd.DataFrame()

    def read_to_df(self):
        self.records = load_json_to_df(self.file_path)
        
        self.set_records_info(self.records)

        return self.records

    def set_records_info(self, df):
        
        df_proc_skip = process_or_skip(df)
        df_proc_skip["file_name"] = self.file_path
        self.records_info = df_proc_skip

        self.num_processed = sum(df_proc_skip["process"])
        self.num_skipped = sum(df_proc_skip["skip"])


def process():
    data_dir = 'data'
    file_list = get_files_from_path(path=data_dir, extension='.json')
    output_file_path = "output"
    #n_files = len(file_list)

    total_data = pd.DataFrame()
    for i, file_path in enumerate(file_list):
        data =  data_file(file_path)
        data.read_to_df()

        #TODO: implement write_records into 
        data.write_records()

        #df = load_json_to_df(file_path)
        #df["file_name"] = file_path
        
        total_data = pd.concat([total_data, df])

        #print("{:.0%} complete".format(i/n_files))

    #df.to_csv('processed_data.csv')
    return total_data

def process_or_skip(df):
    df_proc_skip = pd.DataFrame(index=df.index)
    
    df_proc_skip["skip"] = (
        df["first_name"].isnull() & 
        df["middle_name"].isnull() & 
        df["last_name"].isnull() & 
        df["zip_code"].isnull())

    df_proc_skip["process"] = ~df_proc_skip["skip"]

    return df_proc_skip

def q1_fun(df):
    q1 = (df
        .groupby(by = "file_name")
        .agg({"process":"sum"})
        .sort_values(by = "process", ascending=False)
        .iloc[0:9]
        )
    print(q1)
    return q1

def q2_fun(df):
    q2 = (df
        .groupby(by = "file_name")
        .agg({"skip":"sum"})
        .sort_values(by = "skip", ascending=False)
        .iloc[0:9]
        )
    print(q2)
    return q2

def q3_fun(df):
    q3 = (df
        .groupby(by = "zip_code")
        .agg({"last_name":"nunique"})
        .sort_values(by = "last_name", ascending=False)
        .iloc[0:9]
        )
    print(q3)
    return q3


# https://stackoverflow.com/questions/68327646/how-can-we-read-all-json-files-from-all-sub-directory
def get_files_from_path(path: str='.', extension: str=None) -> list:

    """return list of files from path"""
    # see the answer on the link below for a ridiculously 
    # complete answer for this. I tend to use this one.
    # note that it also goes into subdirs of the path
    # https://stackoverflow.com/a/41447012/9267296
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

    f = open(file_path)
    s = f.read()
    f.close()
    
    data0 = json.loads(s)
    data1 = json.loads(data0)
    df = pd.DataFrame.from_dict(data1)
    return df



if __name__ == "__main__":
    df_proc = process()
    df_proc_skipped = write_or_skip(df_proc)

    print(df_proc)
    df_proc.to_csv('processed_data.csv')
    df_proc_skipped.to_csv('processed_data_skipped.csv')
    
    len(df_proc)
    len(df_proc_skipped)


    q1_df = q1_fun(df_proc_skipped)
    q1_df.to_csv('q1_ans.csv')

    q2_df = q2_fun(df_proc_skipped)
    q2_df.to_csv('q2_ans.csv')

    q3_df = q3_fun(df_proc_skipped)
    q3_df.to_csv('q3_ans.csv')


    dat1 = data_file("data/altius/group00/client00/36526.json")
    dat1.records
    dat1.records_info
    print(dat1.num_processed)
    print(dat1.num_skipped)

    df1 = dat1.read_to_df()
    dat1.records
    dat1.records_info
    print(dat1.num_processed)
    print(dat1.num_skipped)

    whatamI = df1["last_name"].isnull()
    whatamI.index
    print(dat1.records_processed)

    q2 = (df_proc
        .groupby(by = "file_name")
        .agg({"skip":"sum"})
        #.sort_values(by = "skip", ascending=False)
        .iloc[0:9]
        )

    

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