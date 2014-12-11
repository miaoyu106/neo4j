import uuid,json
def jsonRefresh(fileIn, fileOut):
    with open(fileIn) as content_file:
        obj = json.loads(content_file.read())
    tmp=[]
    dic={}
    for attr in obj:
        if attr!="prefix" and type(obj[attr]) is dict:
            tmp+=obj[attr].keys()
    for ele in tmp:
        #ele=json.dumps(ele)
        dic[ele]=str(uuid.uuid1())
    print dic
    with open(fileOut, "wt") as fout:
        with open(fileIn, "rt") as fin:
            for line in fin:
                for key, val in dic.iteritems():
                    #print key[1:-1], type(key), type(val)
                    line=line.replace(key,val)
                print line
                fout.write(line)

jsonRefresh("file1.json","file11.json")
