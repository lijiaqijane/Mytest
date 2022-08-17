import sys
import os
import random
import json
from py2neo import Graph, Node, Relationship
import datetime
import time

graph = Graph("http://10.1.31.208:7476", username="neo4j", password='123456')
# graph.delete_all()

node_num, relation_num = 0, 0
nodes, relations = [], []

gl = open('output/lines_walkthrough.txt', 'a+', encoding='UTF-8') 
gn = open('output/node2id_pkubase.txt', 'a+', encoding='UTF-8') 
gr = open('output/relation2id_pkubase.txt', 'a+', encoding='UTF-8') 

# with open('output/neo4j_nodes_baike.txt', 'r') as f1:
#     try:
#         nodes = eval(f1.read())
#     except:
#         pass

# with open('output/neo4j_relations_baike.txt', 'r') as f2:
#     try:
#         relations = eval(f2.read())
#     except:
#         pass

line_num = 0
with open('pkubase.nt','r',encoding='UTF-8') as source_file:
    for line in source_file:
        line = line.strip('\n').split('\t')
        if line_num >= 0 and len(line) == 3:
            head, relation, tail = line[0], line[1], line[2]
            node1 = Node('Node', name = head)
            graph.merge(node1, 'Node', 'name')
            node2 = Node('Node', name = tail)
            graph.merge(node2, 'Node', 'name' )
            rel = Relationship(node1, relation, node2)
            graph.create(rel)
            

            if head not in nodes:
                gn.write(str(node_num)+'\t'+head+'\n')
                nodes.append(head)
                node_num += 1
                
            if tail not in nodes:
                gn.write(str(node_num)+'\t'+tail+'\n')
                nodes.append(tail)
                node_num += 1

            
            if relation not in relations:
                gr.write(str(relation_num)+'\t'+relation+'\n')
                relations.append(relation)
                relation_num += 1
            
            # if line_num > 29999:
            #     print('line_num:',line_num)
            #     print('node_num:',node_num)
            #     print('relation_num:',relation_num)
            #     print(datetime.datetime.now())
            #     with open('neo4j_nodes_baike.txt', 'w') as g1:
            #         g1.write(str(nodes))
            #     with open('neo4j_relations_baike.txt', 'w') as g2:
            #         g2.write(str(relations))
            #     break

        gl.write(str(line_num)+',')
        line_num += 1

