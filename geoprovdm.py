import sys, os, datetime, decimal

# http://book.py2neo.org/en/latest/fundamentals/#node-relationship-abstracts
from py2neo import neo4j, node, rel

# VERSION
_geoprovdm_version=0.1

class GeoProvDM:
  """ Main class that provides  all functionality needed
  to create entities, agents, activities and to relations among them
  """
  # global connections for neo4j
  _neo_graph = None

  # reset db command
  # neo4j (http://localhost:7474/browser/): match (n) optional match (n)-[r]-() delete n, r

  def __init__(self, url = "http://localhost:7474/db/data/", cleanStart = False):
    self._neo_graph = neo4j.GraphDatabaseService(url)
    # reset the neo graph 
    if cleanStart:
      neo4j.CypherQuery(self._neo_graph,\
        "MATCH (n) WHERE n._geoprovdm_version = " + str(_geoprovdm_version) + " OPTIONAL MATCH (n)-[r]-() DELETE n, r").execute()

  _AGENT = "Agent"
  def addAgent(self, agent):
    """ Add an agent to database
  
    :param agent: a dictionary of attributes of the agent
    """
    return self._addObject(self._AGENT, agent)
    
  _ACTIVITY = "Activity"
  def addActivity(self, act):
    """ Add an activity to database
    
    :param act: a dictionary of attributes of the activity
    """
    return self._addObject(self._ACTIVITY, act)
    
  _ENTITY = "Entity"
  def addEntity(self, entity):
    """ Add an entity to database
    
    :param entity: a dictionary of attributes of the entity
    """
    return self._addObject(self._ENTITY, entity)
    
  def _addObject(self, objType, obj):
    if not obj.has_key("_id"):
      obj["_id"] = hex(random.getrandbits(128))[2:-1] 
    elif not self._getNodeById(objType, obj["_id"]) is None:
      return False
      
    obj['_geoprovdm_version'] = _geoprovdm_version
    a_node, = self._neo_graph.create(obj)
    a_node.add_labels(objType)
    return True
    
  def addRelation(self, relationType, name, objectIdList):
    """ Add a relation to database
    
    :param relationType: one of the relation type specified in requiredIdsInRelation
    :param name: name of the relation
    :param objectIdList: a dictionary o f2 object identities of source and dest 
    of the relation specified as 'prov:xxx' where xxx is entity, agent, or activity
    """
    try:
      sType = self._requiredIdsInRelation[relationType][0]
      dType = self._requiredIdsInRelation[relationType][1]
      if relationType=='wasDerivedFrom':
        if 'prov:generatedEntity' in objectIdList:
          sId = objectIdList['prov:generatedEntity']
        if 'prov:usedEntity' in objectIdList:
          dId = objectIdList['prov:usedEntity']
      elif relationType=='wasInformedBy':
        if 'prov:informed' in objectIdList:
          sId = objectIdList['prov:informed']
        if 'prov:informant' in objectIdList:
          dId = objectIdList['prov:informant']
      elif relationType=='actedOnBehalfOf': 
        if 'prov:delegate' in objectIdList:
          sId = objectIdList['prov:delegate']
        if 'prov:responsible' in objectIdList:
          dId = objectIdList['prov:responsible']
      elif relationType=='wasStartedBy' or relationType=='wasEndedBy': 
        if 'prov:activity' in objectIdList:
          sId = objectIdList['prov:activity']
        if 'prov:trigger' in objectIdList:
          dId = objectIdList['prov:trigger']
      else:
          sId = objectIdList['prov:'+ sType]
          dId = objectIdList['prov:'+ dType]              
      #~ print "addRelation", relationType, name, sId, dId
      if sId is not None:
          source = self._getNodeById(sType, sId)
      if dId is not None:
          dest = self._getNodeById(dType, dId)
      if not (source is None or dest is None):
        self._neo_graph.create(rel(source, (relationType, {"name":name}), dest))
    except KeyError:
      print "Error: Incorrect type or id list"

  def _getNodeById(self, nodeType, nodeId):
    query = neo4j.CypherQuery(self._neo_graph, \
      "MATCH (ee:" + nodeType.title() + ") WHERE ee._id = {p_id} RETURN ee;")
    node = query.execute_one(p_id = nodeId)
#     print("33333333333333333333333333333333")
#     print(node)
# #     print(len(node))
#     if node is not None:
#       for i in node:
#         print(i)    
#         print(node[i])
#     print(type(node))
#     print("33333333333333333333333333333333")
    return node

  #'used':['activity', 'entity'], 
  _requiredIdsInRelation = {'wasAssociatedWith':['activity', 'agent'],\
    'used':['activity', 'entity'],\
    'wasInvalidatedBy':['entity', 'activity'], \
    'wasGeneratedBy':['entity', 'activity'], \
    'wasAttributedTo':['entity', 'agent'], \
    'wasDerivedFrom':['entity', 'entity'], \
    'actedOnBehalfOf':['agent', 'agent'], \
    'wasInformedBy':['activity', 'activity'],\
    'wasStartedBy':['activity', 'entity'], \
    'wasEndedBy':['activity', 'entity']}

  def getRequiredIdsInRelation(self):
    return self._requiredIdsInRelation

######
#   def getAct(self):
#     query = neo4j.CypherQuery(self._neo_graph, \
#       "MATCH (e)-[r:`wasInformedBy`]->a \
#       RETURN e,r,a;")
#     result = query.execute()
#     #print(result)
#     return result      
######

############################
#Retrieve PROV-DM compliant provenance about the resource with the given ‘uuid’.
  def getNodeByUuid(self, nodeUuid):
    #print("!!!")
    query = neo4j.CypherQuery(self._neo_graph, \
      "MATCH (en:Entity)-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      Where en.`foundry:UUID` = {p_uuid} \
      RETURN a1,r1,a,r2,a2;")
    result = query.execute(p_uuid = nodeUuid)
    #print(result)
    return result  #*

#Retrieve PROV-DM compliant provenance  of a resource with a given ‘uuid’, and which has activity ‘activityname’ in its path
  def getNodeByUuidWithActivity(self, nodeUuid, activityname):
    query = neo4j.CypherQuery(self._neo_graph, \
      #"MATCH (agt:Agent)-[r1]-(act:Activity)-[r2]-(en:Entity) WHERE en.`foundry:UUID` = {p_uuid} AND act._id={aid} RETURN agt, r1, act, r2, en;")
      "MATCH (act:Activity)-[*..]-(en:Entity) \
      OPTIONAL MATCH en--a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      WHERE en.`foundry:UUID` = {p_uuid} AND act._id={aid} \
      RETURN a1,r1,a,r2,a2;")
    node = query.execute(p_uuid = nodeUuid, aid=activityname)
    #print node[0]
    return node

#Retrieve PROV-DM compliant provenance  of a resource with a given ‘’uuid’, and which was generated by resource UUID2
  def getNodeByUuidwasGeneratedBy(self, enUuid, UUID2):#UUID2 must be Activity,enUuid must be Entity
    query = neo4j.CypherQuery(self._neo_graph, \
      #"MATCH (agt:Agent)-[r1]-(act:Activity)-[r2:`wasGeneratedBy`]-(en:Entity) WHERE en.uuid = {p_uuid} AND act._id = {aid} RETURN agt, r1, act, r2, en;")
      "MATCH (en:Entity)-[r:`wasGeneratedBy`]-(act:Activity) \
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      WHERE en.uuid = {p_uuid} AND act._id = {aid} \
      RETURN a1, r1, a, r2, a2;")
    node = query.execute(p_uuid = enUuid, aid = UUID2)
    return node

#Retrieve PROV-DM compliant provenance  of a resource with a given ‘uuid’, and which generated resource UUID2
  def getNodeByUuidGenerate(self, acUuid, UUID2):   #UUID2 must be Entity, acUuid must be Activity
    query = neo4j.CypherQuery(self._neo_graph, \
      #"MATCH (agt:Agent)-[r1]-(act:Activity)-[r2:`wasGeneratedBy`]-(en:Entity) WHERE en.uuid = {p_uuid} AND act._id = {aid}  RETURN agt, r1, act, r2, en;")
      "MATCH (en:Entity)-[r:`wasGeneratedBy`]-(act:Activity) \
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      WHERE en.uuid = {p_uuid} AND act._id = {aid} \
      RETURN a1, r1, a, r2, a2;")
    node = query.execute(p_uuid = UUID2, aid = acUuid)
    return node

#Retrieve PROV-DM compliant provenance  of a resource with a given ‘’uuid’, which has activity ‘activity name’ and resource ‘id2’ in its ancestral path
  def getNodeByUuidWithAncestral(self, nodeUuid, activityname, id2):
    query = neo4j.CypherQuery(self._neo_graph, \
      "MATCH res-[*..]-(en:Entity)-[*..]-(act:Activity) \
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      WHERE en.uuid = {p_uuid} AND act._id = {aid} AND res._id = {rid2} \
      AND en.`foundry:creationTime`>= act.`prov:endTime` \
      AND (en.`foundry:creationTime`>= res.`prov:endTime`  OR en.`foundry:creationTime`>=res.`foundry:creationTime` )\
      RETURN a1, r1, a, r2, a2;")  
    node = query.execute(p_uuid = nodeUuid, aid= activityname,rid2= id2)
    return node

#Retrieve PROV-DM compliant provenance  of a resource with a given ‘’uuid’, which has activity ‘activity name’ and resource ‘id2’ in its forward path
  def getNodeByUuidWithForward(self, nodeUuid, activityname, id2):
    query = neo4j.CypherQuery(self._neo_graph, \
      "MATCH res-[*..]-(en:Entity)-[*..]-(act:Activity) \
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      WHERE en.uuid = {p_uuid} AND act._id = {aid} AND res._id = {rid2} \
      AND en.`foundry:creationTime`<= act.`prov:endTime` \
      AND (en.`foundry:creationTime`<= res.`prov:endTime`  OR en.`foundry:creationTime`<=res.`foundry:creationTime` )\
      RETURN a1, r1, a, r2, a2;")  
    node = query.execute(p_uuid = nodeUuid, aid= activityname,rid2= id2)
    return node


#Retrieve PROV-DM compliant provenance  of all resources used by an activity ‘activity name’ between time ‘datetime1’ and ‘datetime2’
  def getNodeUsedByActivityWithTimestamp(self, activityname, datetime1, datetime2):
    query = neo4j.CypherQuery(self._neo_graph, \
      #"MATCH (agt:Agent)-[r1]-(act:Activity)-[r2:`used`]-(en:Entity) WHERE act._id={aid} AND act.`prov:startTime`>={t1} AND act.`prov:endTime` <={t2} RETURN agt, r1, act, r2, en;")
      "MATCH (act:Activity)-[r:`used`]-(en:Entity) \
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      WHERE act._id = {aid} AND act.`prov:startTime`>={t1} AND act.`prov:endTime` <={t2}  \
      RETURN a1, r1, a, r2, a2;")  
    node = query.execute(aid=activityname, t1= datetime1, t2 = datetime2)
    #print node[0]
    return node

# #Retrieve PROV-DM compliant provenance  of all resources generated by ‘activity name’ between time ‘dateime1’ and ‘datetime2’
  def getNodeGeneratedByActivityWithTimestamp(self, activityname, datetime1, datetime2):
    query = neo4j.CypherQuery(self._neo_graph, \
      #"MATCH (agt:Agent)-[r1]-(act:Activity)-[r2:`wasGeneratedBy`]-(en:Entity) WHERE act._id={aid} AND act.`prov:startTime`>={t1} AND act.`prov:endTime` <={t2} RETURN agt, r1, act, r2, en;")
      "MATCH (en:Entity)-[r:`wasGeneratedBy`]-(act:Activity) \
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      WHERE act._id = {aid} AND en.`foundry:creationTime`>={t1} AND en.`foundry:creationTime` <={t2}  \
      RETURN a1, r1, a, r2, a2;")  
    node = query.execute(aid=activityname, t1= datetime1, t2 = datetime2)
    #print node[0]
    return node

#Retrieve PROV-DM compliant provenance  of all resources attributed to agent
  def getNodeAttributedToAgent(self):
    query = neo4j.CypherQuery(self._neo_graph, \
      #"MATCH (en:Entity)-[r1:`wasAttributedTo`]-(agt:Agent)-[r2]-(act:Activity)  RETURN agt, r1, act, r2, en;")
      "MATCH (en:Entity)-[r:`wasAttributedTo`]-(agt:Agent)\
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      RETURN a1, r1, a, r2, a2;")
    node = query.execute()
    return node

#Retrieve PROV-DM compliant provenance  of all resources attributed to agent who was associated with activity
  def getNodeAttributedToAgentWithActivity(self):
    query = neo4j.CypherQuery(self._neo_graph, \
      #"MATCH (en:Entity)-[r1:`wasAttributedTo`]-(agt:Agent)-[r2:`wasAssociatedWith`]-(act:Activity) RETURN agt, r1, act, r2, en;")
      "MATCH (en:Entity)-[r01:`wasAttributedTo`]-(agt:Agent)-[r02:`wasAssociatedWith`]-(act:Activity)\
      OPTIONAL MATCH en-[*..]-a \
      OPTIONAL MATCH (a1)-[r1]-a \
      OPTIONAL MATCH a-[r2]-(a2) \
      RETURN a1, r1, a, r2, a2;")
    node = query.execute()
    return node

############################

# === other functions from dump.py ===
# --- neo4j functions ---
def _neo_dump_prov(resource):
  global _neo_graph, uuid2record
  
  a_node, = _neo_graph.create(resource)
  a_node.add_labels("Entity", "Record")
  
  siteuuid = resource['siteuuid']
  if siteuuid == '':
    # don't have parent, link to user
    u_dn, u_username = pg_get_user(resource['owner'])
    query = neo4j.CypherQuery(_neo_graph, "MATCH (ee:Agent) WHERE ee.username = {p_username} RETURN ee;")
    agent = query.execute_one(p_username = u_username)
    if agent is None:
      agent, _ = _neo_graph.create( \
          node(userid = resource['owner'], dn = u_dn, username = u_username), \
          rel(a_node, "wasAttributedTo", 0))
      agent.add_labels("Agent")
    else:
      _neo_graph.create(rel(a_node, "wasAttributedTo", agent))
  else:
    # has parent
    # link to its resourceURI node
    try:
      parent_node = uuid2record[siteuuid]
    except KeyError:
      query = neo4j.CypherQuery(_neo_graph, "MATCH (ee:Entity) WHERE ee.docuuid = {p_uuid} RETURN ee;")
      parent_node = query.execute_one(p_uuid = siteuuid)
      if not parent_node is None:
        uuid2record[siteuuid] = parent_node
        parent_node.add_labels("Resource")
        parent_node.remove_labels("Record")
    if not parent_node is None:
      _neo_graph.create(rel(a_node, "wasAssociatedWith", parent_node))
    
    # and link to harvest node
    try:
      harvest = uuid2harvest[resource['juuid']]
      _neo_graph.create(rel(a_node, "wasGeneratedBy", harvest))
    except KeyError:
      print "Harvest node not found!"

def _neo_update_user():
  global _neo_graph, userid2agent
  print "neo_update_user ..."
  all_users = pg_get_all_users()
  for (u_id, u_dn, u_username) in all_users:
    query = neo4j.CypherQuery(_neo_graph, "MATCH (ee:Agent) WHERE ee.username = {p_username} RETURN ee;")
    agent = query.execute_one(p_username = u_username)
    if agent is None:
      agent, = _neo_graph.create( \
          node(userid = u_id, dn = u_dn, username = u_username))
      agent.add_labels("Agent")
      userid2agent[u_id] = agent
  
def _neo_update_harvest():
  global _neo_graph, uuid2harvest
  print "neo_update_harvest ..."
  all_harvest = pg_get_all_harvest()
  field_names = "uuid, harvest_id, input_date, harvest_date, job_type, service_id, agent".split(", ")
  for record in all_harvest:
    query = neo4j.CypherQuery(_neo_graph, "MATCH (ee:Harvest) WHERE ee.uuid = {p_uuid} RETURN ee;")
    harvest = query.execute_one(p_uuid = record[0])
    if harvest is None:
      harvest_dict = {}
      for i in range(len(field_names)):
        harvest_dict[field_names[i]] = record[i]
      
      harvest, = _neo_graph.create(harvest_dict)
      harvest.add_labels("Harvest", "Activity")
      uuid2harvest[record[0]] = harvest
      try:
        agent = userid2agent[harvest_dict['agent']]
        _neo_graph.create(rel(harvest, "wasAssociatedWith", agent))
      except KeyError:
        pass

def _neo_update_harvest_resource():
  global _neo_graph, uuid2harvest
  print "neo_update_harvest_resource ..."
  for harvest in uuid2harvest.values():
    query = neo4j.CypherQuery(_neo_graph, "MATCH (ee:Resource) WHERE ee.docuuid = {p_uuid} RETURN ee;")
    resource = query.execute_one(p_uuid = harvest['harvest_id'])
    if resource is None:
      print harvest['harvest_id'], "not found"
    else:
      _neo_graph.create(rel(harvest, "used", resource))
      