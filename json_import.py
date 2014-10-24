from geoprovdm import *
import sys,json
from py2neo import neo4j, node, rel
# convert a prov-dm json to a python dictionary object
#   with key and value of type string
def json2obj(ajson):
    res = {}
    for attr in ajson:
        if type(ajson[attr]) is dict:
            if ajson[attr]['type'] == 'xsd:string':
                res[attr] = ajson[attr]['$']
            else:
                res[attr] = str(ajson[attr]['$'])
        else:
            res[attr] = str(ajson[attr])
    return res

def neo2json(aneo):
    res={}
#(69 {"_geoprovdm_version":0.1,"prov:startTime":"2014-07-21T16:05:02-07:00","_id":"foundry:assignId","prov:type":"doc-id-assigment","prov:endTime":"2014-07-21T16:05:03-07:00"})
#(85 {"_geoprovdm_version":0.1,"_id":"foundry:en_1","foundry:sourceId":"nlx_999998","foundry:UUID":"1234-4567-8978","foundry:batchId":"20140721"})
#(86 {"_geoprovdm_version":0.1,"foundry:name":"DocIngestorCLI","_id":"foundry:ag_1","prov:type":"prov:SoftwareAgent"})
    if aneo is not None:
        for node in aneo:
#             print(type(node))
#             print(node[0]["prov:startTime"])
#             print(node[1])
#             print(node[2])
#             print(node[3].type)
#             print(node[4])
#             print("!!!!!!!!!")
#             print("oooooooooooo")
#             print(len(node))
            size = len(node)-1
            #print node[1]
            while(size>=0):
                if node[size] is not None:
                    #print node[size].get_labels()
                    #print node[size]
                    #print node[size].type
                    if hasattr(node[size], 'type'):
                        #print node[3].type
                        if node[size].type not in res:
                            res[node[size].type] ={}
                        o_id=node[size-1]["_id"] if node[size-1] is not None else "null"
                        i_id=node[size+1]["_id"] if node[size+1] is not None else "null"
                        if node[size].type == 'wasAssociatedWith':
                            res['wasAssociatedWith'][node[size]["name"]]={"prov:activity": o_id, "prov:agent": i_id}
                        elif node[size].type == 'wasInvalidatedBy':
                            res['wasInvalidatedBy'][node[size]["name"]]={ "prov:entity": i_id,"prov:activity": o_id}
                        elif node[size].type == 'wasGeneratedBy':
                            res['wasGeneratedBy'][node[size]["name"]]={"prov:entity": o_id, "prov:activity": i_id}  
                        elif node[size].type == 'wasAttributedTo':
                            res['wasAttributedTo'][node[size]["name"]]={"prov:entity": o_id, "prov:agent": i_id}
                        elif node[size].type == 'used':
                            res['used'][node[size]["name"]]={"prov:activity": o_id, "prov:entity": i_id}
                        elif node[size].type == 'wasDerivedFrom':
                            res['wasDerivedFrom'][node[size]["name"]]={"prov:generatedEntity": o_id, "prov:usedEntity": i_id}
                        elif node[size].type == 'actedOnBehalfOf':
                            res['actedOnBehalfOf'][node[size]["name"]]={"prov:delegate": o_id, "prov:responsible": i_id}
                        elif node[size].type == 'wasInformedBy':
                            res['wasInformedBy'][node[size]["name"]]={"prov:informed": o_id, "prov:informant": i_id}
                        elif node[size].type == 'wasStartedBy':
                            res['wasStartedBy'][node[size]["name"]]={"prov:activity": o_id, "prov:trigger": i_id}
                        elif node[size].type == 'wasEndedBy':
                            res['wasEndedBy'][node[size]["name"]]={"prov:activity": o_id, "prov:trigger": i_id}    
                        else:
                            print node[size].type
                    elif "Activity" in node[size].get_labels():
                        if "activity" not in res:
                            res["activity"]={}
                        res["activity"][node[size]["_id"]]={"prov:type":{"$":  node[size]["prov:type"], "type": "xsd:string"},\
                                                  "prov:startTime":node[size]["prov:startTime"],\
                                                  "prov:endTime":node[size]["prov:endTime"]}
                    elif "Entity" in node[size].get_labels():
                        if "entity" not in res:
                            res["entity"]={}
                        res["entity"][node[size]["_id"]]={"foundry:sourceId":{"$": node[size]["foundry:sourceId"], "type": "xsd:string"},\
                                                      "foundry:UUID":node[size]["foundry:UUID"],"foundry:creationTime":node[size]["foundry:creationTime"],\
                                                      "foundry:batchId":node[size]["foundry:batchId"]}
                    elif "Agent" in node[size].get_labels():#if node[size]["prov:type"] is not None:#and not node[size].has_key("prov:startTime") and not node[size].has_key("foundry:UUID"):
                        if "agent" not in res:
                            res["agent"] ={}
                        res["agent"][node[size]["_id"]]={"prov:type":{"$": node[size]["prov:type"], "type": "xsd:string"}}
                    else:
                        pass
                size = size-1
        res2 = json.dumps(res,ensure_ascii=True)
        #print res2
    return res2
    

def main():
    
    # input file
    filename = 'doc1.json' #'doc_full_test.json' 
    if len(sys.argv) == 2:
        filename = sys.argv[1]
        #print(filename)
        #print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        
    # read input data
    with open(filename) as content_file:
        obj = json.loads(content_file.read())
    
    # =======================
    # === add all objects ===

    p = GeoProvDM("http://localhost:7474/db/data/", True)
    #print(p)
    #print("**********************************")
    
    # make all entities
    try:
        entities = obj['entity']
        for k in entities.keys():
            entity = json2obj(entities[k])
            #print(entity)
            entity[u'_id'] = k
            p.addEntity(entity)
    except KeyError:
        print("There is no Entity!! ERROR!!")
    
    # make all agents
    if 'agent' in obj:
        agents = obj['agent']        
        for k in agents.keys():
            agent = json2obj(agents[k])
            agent[u'_id'] = k
            p.addAgent(agent)
        
    # make all activities
    if 'activity' in obj:
        acts = obj['activity']
        for k in acts.keys():
            act = json2obj(acts[k])
            act[u'_id'] = k
            p.addActivity(act)

    # =========================
    # === add all relations ===
    for rel in p.getRequiredIdsInRelation().keys():
        #print(rel)
        try:
            relations = obj[rel]
            #print(relations)
            for name in relations.keys():
                #print(name)
                p.addRelation(rel, name, relations[name])
        except KeyError:
            pass
    res1 = p.getNodeByUuid("1234-4567-8978")
    res2 = p.getNodeByUuidWithActivity("1234-4567-8978","foundry:ac1")
    res3 = p.getNodeByUuidwasGeneratedBy("1234-4567-8978","foundry:ac1")
    res4 = p.getNodeByUuidGenerate("foundry:ac1","1234-4567-8978")
    res5 = p.getNodeByUuidWithAncestral("1234-4567-8979", "foundry:ac1", "foundry:en1")
    res6 = p.getNodeByUuidWithForward("1234-4567-8978", "foundry:ac1", "foundry:en2")
    res7 = p.getNodeUsedByActivityWithTimestamp("foundry:ac1","2014-07-21T16:05:05-07:00","2014-07-21T16:05:07-07:00")
    res8 = p.getNodeGeneratedByActivityWithTimestamp("foundry:ac1","2014-07-21T16:05:00-07:00","2014-07-21T16:05:09-07:00")
    res9 = p.getNodeAttributedToAgentWithActivity()
    res10 = p.getNodeAttributedToAgent()
    res01= neo2json(res1)
    res02= neo2json(res2)
    res03= neo2json(res3)
    res04= neo2json(res4)
    res05= neo2json(res5)
    res06= neo2json(res6)
    res07= neo2json(res7)
    res08= neo2json(res8)
    res09= neo2json(res9)
    res010= neo2json(res10)
        #res3=json.dumps(res2)#,sort_keys=True, separators=(':'))
        #     res3=json.dumps(res2, ensure_ascii=False
    
    print(res01)
    print("1st Done")
    print(res02)
    print("2nd Done")
    print(res03)
    print("3rd Done")
    print(res04)
    print("4th Done")
    print(res05)
    print("5th Done")
    print(res06)
    print("6th Done")
    print(res07)
    print("7th Done")
    print(res08)
    print("8th Done")
    print(res09)
    print("9th Done")
    print(res010)
    print("10th Done")

    #print(json.loads(res3))
if __name__ == '__main__':
    main()
