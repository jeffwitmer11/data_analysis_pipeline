# process.py

def process():
    return 2+2
    # this is where your code goes

#https://stackoverflow.com/questions/39233973/get-all-keys-of-a-nested-dictionary
def recursive_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is dict:
            yield from recursive_items(value)
        else:
            yield (key, value)

def recursive_dict2df(dict):


if __name__ == "__main__":
    #n = process()

    import json
    import pandas as  pd
  
    # Opening JSON file
    f = open('data/altius/group00/client00/36526.json')
    s = f.read()
    f.close()
    data0 = json.loads(s)
    data1 = json.loads(data0)
    data = data1[0]

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

    df = pd.DataFrame.from_dict(data1)
    
    df["skip"] = df["first_name"].isnull() & df["middle_name"].isnull() & df["last_name"].isnull() & df["zip_code"].isnull()
    print(df)
    sum(df["skip"])