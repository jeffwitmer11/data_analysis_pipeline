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



if __name__ == "__main__":
    #n = process()

    import json
  
    # Opening JSON file
    f = open('data/altius/group00/client00/36526.json')
    s = f.read()
    f.close()
    data0 = json.loads(s)
    data1 = json.loads(data0)
    data = data1[0]

    recursive_items(data)