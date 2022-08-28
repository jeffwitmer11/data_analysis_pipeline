# process.py

def process():
    import json
    import os
    import pandas as pd
  
    data_dir = 'data'
    file_list = get_files_from_path(path=data_dir, extension='.json')

    total_data = pd.DataFrame()
    for file_path in file_list:
        df = load_json_to_df(file_path)
        df["file_name"] = file_path
        total_data = pd.concat([total_data, df])

    # Question 1
    df = total_data
    df["skip"] = df["first_name"].isnull() & df["middle_name"].isnull() & df["last_name"].isnull() & df["zip_code"].isnull()
    df["process"] = ~df["skip"]
    
    q1 = (df
        .groupby(by = "file_name")
        .agg({"process":"sum"})
        .sort_values(by = "process", ascending=False)
        .iloc[0:9]
        )
    print(q1)

    q2 = (df
        .groupby(by = "file_name")
        .agg({"skip":"sum"})
        .sort_values(by = "skip", ascending=False)
        .iloc[0:9]
        )
    print(q2)

    q3 = (df
        .groupby(by = "zip_code")
        .agg({"last_name":"nunique"})
        .sort_values(by = "last_name", ascending=False)
        .iloc[0:9]
        )
    print(q3)

#https://stackoverflow.com/questions/39233973/get-all-keys-of-a-nested-dictionary
def recursive_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is dict:
            yield from recursive_items(value)
        else:
            yield (key, value)

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
    process()

    import json
    import pandas as pd
  
    # Opening JSON file
    file_path = 'data/altius/group00/client00/36526.json'
    

    for i,l in enumerate(data1):
        print(i)
        print(l)
        for key, value in recursive_items(l):
            print(key, value)



    my_dict = {
        "key1":"foo",
        "key2":{"key1":"bar", "key3":"baz"},
        "key3":"hello"
    }

    for i in my_dict:
        print(i)

    
    

    print(df)

    import os
    data_dir = 'data'
    file_list = get_files_from_path(path=data_dir, extension='.json')

    total_data = pd.DataFrame()
    for file_path in file_list:
        print(file_path)
        df = load_json_to_df(file_path)


        #df["skip"] = df["first_name"].isnull() & df["middle_name"].isnull() & df["last_name"].isnull() & df["zip_code"].isnull()
        df["file_name"] = file_path
        total_data = pd.concat([total_data, df])

    len(total_data)

    # Question 1
    df = total_data
    df["skip"] = df["first_name"].isnull() & df["middle_name"].isnull() & df["last_name"].isnull() & df["zip_code"].isnull()
    df["process"] = ~df["skip"]
    
    q1 = (df
        .groupby(by = "file_name")
        .agg({"process":"sum"})
        .sort_values(by = "process", ascending=False)
        .iloc[0:9]
        )
    print(q1)

    q2 = (df
        .groupby(by = "file_name")
        .agg({"skip":"sum"})
        .sort_values(by = "skip", ascending=False)
        .iloc[0:9]
        )
    print(q2)

    q3 = (df
        .groupby(by = "zip_code")
        .agg({"last_name":"nunique"})
        .sort_values(by = "last_name", ascending=False)
        .iloc[0:9]
        )
    print(q3)

    df.columns

#jsonlist = []
#for filepath in filelist:
 #   with open(filepath) as infile:
  #      jsonlist.append(json.load(infile)
#from pprint import pprint
#pprint(jsonlist)