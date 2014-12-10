from geoprovdm import *
from help_json import *
import sys,json
from py2neo import neo4j, node, rel

# convert a prov-dm json to a python dictionary object
#   with key and value of type string

def main():
    p = GeoProvDM("http://localhost:7474/db/data/", True)
    # input file
    #filename =['file01.json'] #'file1.json'
    filename=['file1.json','file2.json','file3.json','file4.json','file5.json','file6.json','file7.json']
#     if len(sys.argv) <3  or sys.argv[1]!="-i" :
#         sys.exit("Must provide files")
#     filename=[]
#     cur=2
#     while cur<len(sys.argv):
#         filename.append(sys.argv[cur])
#         cur+=1
    
        
    # read input data
    for f in filename:
        with open(f) as content_file:
            obj = json.loads(content_file.read())
            # === add all objects ===
            createGraph(obj,p)
            
    res1 = p.getNodeByUuid("1234-4567-8985")
    res01= neo2json(res1)
    print(res01)
    
    f = open('file01.json','w')
    f.write(res01) # python will convert \n to os.linesep
    f.close()
    
    res2 = p.getNodeByUuidWithActivity("1234-4567-8979","act1")
    res02= neo2json(res2)
    print(res02)
    f = open('file02.json','w')
    f.write(res02) # python will convert \n to os.linesep
    f.close()

    res3 = p.getNodeByUuidwasGeneratedBy("1234-4567-8983","foundry:ac2")
    res03= neo2json(res3)
    print(res03)
    f = open('file03.json','w')
    f.write(res03) # python will convert \n to os.linesep
    f.close()
    
    res4 = p.getNodeByUuidGenerate("foundry:ac1","1234-4567-8979")
    res04= neo2json(res4)
    print(res04)
    f = open('file04.json','w')
    f.write(res04) # python will convert \n to os.linesep
    f.close()

    res5 = p.getNodeByUuidWithAncestral("1234-4567-8981", "act1", "foundry:en1")
    res05= neo2json(res5)
    print(res05)
    f = open('file05.json','w')
    f.write(res05) # python will convert \n to os.linesep
    f.close()
    
    res6 = p.getNodeByUuidWithForward("1234-4567-8979", "act2", "foundry:en4")
    res06= neo2json(res6)
    print(res06)
    f = open('file06.json','w')
    f.write(res06) # python will convert \n to os.linesep
    f.close()

    #neo4jrestclient.options.DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    res7 = p.getNodeUsedByActivityWithTimestamp("act3","2014-11-01T00:17:00","2014-11-01T00:19:00")
    res07= neo2json(res7)
    print(res07)
    f = open('file07.json','w')
    f.write(res07) # python will convert \n to os.linesep
    f.close()

    
    res8 = p.getNodeGeneratedByActivityWithTimestamp("act2","2014-11-01T00:14:00","2014-11-01T00:17:00")
    res08= neo2json(res8)
    print(res08)
    f = open('file08.json','w')
    f.write(res08) # python will convert \n to os.linesep
    f.close()


if __name__ == '__main__':
    main()

