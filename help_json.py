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
    if aneo is not None:
        for paths in aneo:
            for path in paths:
                if hasattr(paths, "relationships"):
                    rels= path.relationships               
                    #nodes= path.nodes
                    for r in rels:
                        if r.type not in res:
                                res[r.type] ={}
                        if r.type == 'wasDerivedFrom':
                            res['wasDerivedFrom'][r["name"]]={"prov:generatedEntity": r.start_node["_id"], "prov:usedEntity": r.end_node["_id"]}
                        elif r.type == 'actedOnBehalfOf':
                            res['actedOnBehalfOf'][r["name"]]={"prov:delegate": r.start_node["_id"], "prov:responsible": r.end_node["_id"]}
                        elif r.type == 'wasInformedBy':
                            res['wasInformedBy'][r["name"]]={"prov:informed": r.start_node["_id"], "prov:informant": r.end_node["_id"]}
                        elif r.type == 'wasStartedBy':
                            res['wasStartedBy'][r["name"]]={"prov:activity": r.start_node["_id"], "prov:trigger":  r.end_node["_id"]}
                        elif r.type == 'wasEndedBy':
                            res['wasEndedBy'][r["name"]]={"prov:activity": r.start_node["_id"], "prov:trigger":  r.end_node["_id"]}    
                        else:
                            s=r.start_node.get_labels()# type is set
                            e=r.end_node.get_labels()
                            if "Activity" in s:
                                st='activity'
                            elif "Entity" in s:
                                st='entity'
                            elif "Agent" in s:
                                st='agent'
                            if "Activity" in e:
                                et='activity'
                            elif "Entity" in e:
                                et='entity'
                            elif "Agent" in e:
                                et='agent'
                            snode='prov:'+ st
                            enode='prov:'+ et 
                            res[r.type][r["name"]]={snode: r.start_node["_id"], enode: r.end_node["_id"]}
                nodes= path.nodes            
                for n in nodes:
                    print n
                    print n.get_indexes()
                    if "Activity" in n.get_labels():
                        if "activity" not in res:
                            res["activity"]={}
                        res["activity"][n["_id"]]={"prov:type":{"$":  n["prov:type"], "type": "xsd:string"},\
                                                  "prov:startTime":n["prov:startTime"],\
                                                  "prov:endTime":n["prov:endTime"]}
                    elif "Entity" in n.get_labels():
                        if "entity" not in res:
                            res["entity"]={}
                        res["entity"][n["_id"]]={"foundry:sourceId":{"$": n["foundry:sourceId"], "type": "xsd:string"},\
                                                      "foundry:UUID":n["foundry:UUID"],"foundry:creationTime":n["foundry:creationTime"],\
                                                      "foundry:batchId":n["foundry:batchId"]}
                    elif "Agent" in n.get_labels():#if node[size]["prov:type"] is not None:#and not node[size].has_key("prov:startTime") and not node[size].has_key("foundry:UUID"):
                        if "agent" not in res:
                            res["agent"] ={}
                        res["agent"][n["_id"]]={"prov:type":{"$":n["prov:type"], "type": "xsd:string"}}
                    else:
                        pass
                
        res2 = json.dumps(res,ensure_ascii=True)
        #print res2
    return res2
    
def createGraph(obj,p):
    if p.getRequestId() is None:
        p.addRequestId()
    else:
        p.updateRequestId() 
    
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
            order= p.getOrder(act[u'prov:type'])
            if order is None:
                p.addOrder(act[u'prov:type'])
                p.addActivity(act)
            else:                
                old=p.getNodeInformant(act[u'prov:type'],order["prov:order"])
                p.updateOrder(act[u'prov:type'])
                p.addActivity(act)
                nodelist=json.loads(json.dumps({"prov:informed":act[u'_id'],"prov:informant":old["_id"]}))
                p.addRelation("wasInformedBy","_:wIB"+act[u'_id']+old['_id'],nodelist)

    # =========================
    # === add all relations ===
    for rel in p.getRequiredIdsInRelation().keys():
        try:
            relations = obj[rel]
            for name in relations.keys():
                p.addRelation(rel, name, relations[name])
        except KeyError:
            pass

