from platform import node
import sys
import os
import random
import linecache
import json
from py2neo import Graph, Node, Relationship

# path = "E:/Data/freebase/processed_data/"
path = "./"

graph = Graph("http://10.1.121.215:7475", username="neo4j", password='123456')
#graph.delete_all()

# a = Node('aerson', name='Bob1')
# graph.merge(a, 'aerson', "name" )
# b = Node('aerson',  name='Bob1')
# graph.merge(b, 'aerson',"name")
# c = Node('Person',  name='lily')
# graph.merge(c, 'aerson',"name")

# node_num, relation_num = 0, 0
# node2StrucNode, relation_pairs = {}, []

# with open('neo4j_nodes.json', 'r') as f:
#     try:
#         node2StrucNode = json.loads(f.read())
#     except:
#         pass
# print(len(node2StrucNode))

# cnt = 0
# with open(path+'nodes.txt','r') as node_file:
#     for line in node_file:
#         cnt += 1
#         if cnt > len(node2StrucNode):
#             line = line.strip('\n').split('\t')
#             entity_name, entity_type = line[0], line[1]
#             node = Node(entity_type, name = entity_name)
#             graph.merge(node, entity_type, 'name' )
#             node2StrucNode[entity_name] = {"labels": [entity_type],"properties": {"name": entity_name}}
#             node_num += 1

#             if cnt % 10000 ==0:
#                 print('!!!!!!!!!!!!!!!!!!!!!nodes', len(node2StrucNode))
#                 with open('neo4j_nodes.json', 'w') as g:
#                     g.write(json.dumps(node2StrucNode))
#         else:
#             continue


# with open('neo4j_relations.txt', 'r') as f:
#     try:
#         relation_pairs = eval(f.read())
#     except:
#         pass
# print(len(relation_pairs))

# cnt = 0
# with open(path+'relations.txt','r') as relationship_file:
#     for line in relationship_file:
#         cnt += 1
#         if cnt > len(relation_pairs):
#             line = line.strip('\n').split('\t')
#             from_node, relation, to_node = line[0], line[1],  line[2]
#             # if relation != 'is_instance_of' :
#             # if [from_node, to_node] not in relation_pairs and [to_node, from_node] not in relation_pairs:
#             # if [from_node, to_node] not in relation_pairs:
#             if from_node in node2StrucNode and to_node in node2StrucNode:
#                 rel = Relationship(node2StrucNode[from_node], relation, node2StrucNode[to_node])
#                 graph.create(rel)
#                 relation_pairs.append([from_node, relation, to_node])
                
#                 if cnt % 10000 ==0:
#                     print('!!!!!!!!!!!!!!!!!!!!!relation', len(relation_pairs))
#                     with open('neo4j_relations.txt', 'w') as g:
#                         g.write(relation_pairs)
#         else:
#             continue
        
node_num, relation_num = 0, 0
node2StrucNode, relation_pairs = {}, []

with open(path+'nodes.txt','r') as node_file:
    for line in node_file:
        line = line.strip('\n').split('\t')
        entity_name, entity_type = line[0], line[1]
        node = Node(entity_type, name = entity_name)
        graph.merge(node, entity_type, 'name' )
        node2StrucNode[entity_name] = {"labels": [entity_type],"properties": {"name": entity_name}}
        node_num += 1


with open(path+'relations.txt','r') as relationship_file:
    for line in relationship_file:
        cnt += 1
        if cnt > len(relation_pairs):
            line = line.strip('\n').split('\t')
            from_node, relation, to_node = line[0], line[1],  line[2]
            if from_node in node2StrucNode and to_node in node2StrucNode:
                rel = Relationship(node2StrucNode[from_node], relation, node2StrucNode[to_node])
                graph.create(rel)
                relation_pairs.append([from_node, relation, to_node])
                
        

