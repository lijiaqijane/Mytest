from calendar import c


known_nodes, known_relations = [], []
nodeid, relationid = 0, 0

gn = open('node2id.txt', 'a')
with open('nodes.txt', 'r') as node_file:
    for line in node_file:
        line = line.strip('\n').split('\t')
        cur_node = line[0]
        if cur_node not in known_nodes:
            known_nodes.append(cur_node)
            nodeid += 1 
            gn.write(cur_node+'\t'+str(nodeid)+'\n')


gr = open('relation2id.txt', 'a')
with open('relations.txt', 'r') as relation_file:
    for line in relation_file:
        line = line.strip('\n').split('\t')
        head, rel, tail = line[0], line[1], line[2]
        if head not in known_nodes:
            known_nodes.append(head)
            nodeid += 1 
            gn.write(head+'\t'+str(nodeid)+'\n')
        
        if tail not in known_nodes:
            known_nodes.append(tail)
            nodeid += 1 
            gn.write(tail+'\t'+str(nodeid)+'\n')

        if rel not in known_relations:
            known_relations.append(rel)
            relationid += 1 
            gr.write(rel+'\t'+str(relationid)+'\n')

gn.close()
gr.close()