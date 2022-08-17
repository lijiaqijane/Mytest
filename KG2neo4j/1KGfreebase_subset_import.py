from platform import node
import sys
import os
import random
import linecache
from py2neo import Graph, Node, Relationship

path = "E:/Data/freebase/processed_data/"

graph = Graph("http://localhost:7474", username="neo4j", password='123456')
graph.delete_all()

dirs = os.listdir(path)

node_num, relation_num, relationship_num = 0, 0, 0
nodes, relation_pairs = [], []


####random_node+BFS_searching+limited_neighbors
# init = ['/user/ovguide/tvdb_season_id/570471']
init = ['/music/recording']
while len(init) > 0:
    tmp = []
    for cand in init:
        print(cand)
        res = os.popen('grep '+cand+' relations.txt').readlines()
        print(len(res))
        res_num = 0
        for line in res:
            res_num += 1
            if res_num <200:
                line = line.strip('\n').split('\t')
                from_node, relation, to_node = line[0], line[1],  line[2]
                if from_node != cand and from_node not in tmp:
                    tmp.append(from_node)
                if to_node != cand and to_node not in tmp:
                    tmp.append(to_node)

                if from_node not in nodes:
                    node1 = Node('node', name = from_node)
                    nodes.append(from_node)
                    graph.create(node1)
                if to_node not in nodes:
                    node2 = Node('node', name = to_node)
                    nodes.append(to_node)
                    graph.create(node2)

                rel = Relationship(node1, relation, node2)
                graph.create(rel)
            else:
                break

    init = tmp
    print(init)

# LINE = 0
# with open(path+'nodes.txt','r') as node_file:
#     for line in node_file:
#         if LINE < 50000:
#             line = line.strip('\n').split('\t')
#             entity_name, entity_type = line[0], line[1]
#             node = Node(entity_type, name = entity_name)
#             graph.create(node)
#             node2StrucNode[entity_name] = node
#             node_num += 1
#             LINE += 1


# cnt = 0
# while cnt <= 1000000:
#     line_num = random.randint(1,455626378)
#     line = linecache.getline(path+'relations.txt', line_num)
# 
# cnt = 0
# with open(path+'relations.txt','r') as relationship_file:
#     for line in relationship_file:
#         cnt += 1
#         if cnt >= 300000000:
#             line = line.strip('\n').split('\t')
#             from_node, relation, to_node = line[0], line[1],  line[2]
#             if relation != 'is_instance_of' :
#                 if [from_node, to_node] not in relation_pairs and [to_node, from_node] not in relation_pairs:
#                     # if from_node in node2StrucNode and to_node in node2StrucNode:
                        
#                         node1 = Node('node', name = from_node)
#                         node2 = Node('node', name = to_node)
#                         graph.create(node1)
#                         graph.create(node2)
#                         node_num += 2
#                         rel = Relationship(node1, relation, node2)

#                         # rel = Relationship(node2StrucNode[from_node], relation, node2StrucNode[to_node])
#                         graph.create(rel)
#                         relation_num += 1
#                         relation_pairs.append([from_node,to_node])
            
print('node_num:', node_num, 'relation_num:', relation_num)

