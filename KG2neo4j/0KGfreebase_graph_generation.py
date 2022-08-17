from mailbox import MMDF
from platform import node
import sys
import os
import logging
import json
from py2neo import Graph, Node, Relationship


logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)


logging.info('Starting running')

id_mappings, type_domain_dict, property_type_dict, instance_type_dict = {}, {}, {}, {}
with open('./processed_data/id_mappings.json','r', encoding='UTF-8') as g:
    id_mappings = json.loads(g.read())
g.close()
logging.info('End id_mappings')

with open('./processed_data/type_domain_dict.txt','r', encoding='UTF-8') as g:
    for line in g:
        line = line.strip('.\n').split('\t')
        type_domain_dict[line[0]] = line[1]
g.close()

with open('./processed_data/property_type_dict.txt','r', encoding='UTF-8') as g:
    for line in g:
        line = line.strip('.\n').split('\t')
        property_type_dict[line[0]] = line[1]
g.close()

logging.info('Start instance_type_dict')
z2 = open('./processed_data/relations1.txt', 'a', encoding='UTF-8')
with open('./processed_data/instance_type_dict.txt','r', encoding='UTF-8') as g:
    for line in g:
        line = line.strip('.\n').split('\t')
        instance_type_dict[line[0]] = '/'+line[1].replace('.', '/')
        z2.write(line[0]+'\t'+'is_instance_of'+'\t'+'/'+line[1].replace('.', '/')+'\n')
        # instance_type_dict.append([line[0] + '^^'+'/'+line[1].replace('.', '/')])
g.close()
z2.close()
print('instance_length:'+str(len(instance_type_dict.keys())))
logging.info('End processing')



with open('freebase.txt','r', encoding='UTF-8') as f:
    line_cnt = 0
    nodes, relations, node_types = [], [], []
    for line in f:
        line_cnt += 1
        line = line.strip('.\n').split('\t')

        if line_cnt % 100000==0:
            logging.info(line_cnt)
            z1 = open('./processed_data/nodes1.txt', 'a', encoding='UTF-8')
            z1.writelines(nodes)
            z1.close()
            z2 = open('./processed_data/relations1.txt', 'a', encoding='UTF-8')
            z2.writelines(relations)
            z2.close()
            nodes, relations = [], []

        if len(line) == 4:
            if '<http://rdf.freebase.com/' in line[2]:
                subject, predicate, object = line[0].strip('<>').replace('\"','').split('/')[-1], line[1].strip('<>').replace('\"','').split('/')[-1], line[2].strip('<>').replace('\"','').split('/')[-1]
            else:
                subject, predicate, object = line[0].strip('<>').replace('\"','').split('/')[-1], line[1].strip('<>').replace('\"','').split('/')[-1], line[2].strip('"')


            if (subject.startswith('m.') or subject.startswith('g.')) and ( object.startswith('m.') or  object.startswith('g.') or '"' in object or object in ['XMLSchema#gYear', 'XMLSchema#gYearMonth', 'XMLSchema#dateTime','XMLSchema#date']):
                # subject = id_mappings[subject] if subject in id_mappings else subject
                # object = id_mappings[object] if object in id_mappings else object
                if subject in id_mappings and object in id_mappings:
                    subject = id_mappings[subject]
                    object = id_mappings[object]

                    ##version1: instance_type_dict is a list, each element is a str
                    # #nodes_generation
                    # if subject+'\t'+subject_domain+'\n' not in nodes:
                    #     subject_domain = "unknown_domain"
                    #     subject_type = list(filter(lambda x: subject+'^^' in x, instance_type_dict))
                    #     if len(subject_type) > 0 and subject_type[0] in type_domain_dict:
                    #         subject_domain = type_domain_dict[subject_type[0]]
                    #     nodes.append(subject+'\t'+subject_domain+'\n')

                    # if object+'\t'+object_domain+'\n' not in nodes:
                    #     object_domain = "unknown_domain"
                    #     object_type = list(filter(lambda x: object+'^^' in x, instance_type_dict))
                    #     ##case1: object is another instance
                    #     if len(object_type) > 0 and object_type[0] in type_domain_dict:
                    #         object_domain = type_domain_dict[object_type[0]]
                    #     ##case2: object is property of str noun /numeric
                    #     elif object in property_type_dict and property_type_dict[object] in type_domain_dict:
                    #         object_domain = type_domain_dict[property_type_dict[object]]
                    #     nodes.append( object+'\t'+object_domain+'\n')

                    # # #relations_generation
                    # if subject+'\t'+predicate+'\t'+object+'\n' not in relations:
                    #     relations.append(subject+'\t'+predicate+'\t'+object+'\n')


                    ##version2: instance_type_dict is a dict, can be hashed
                    #nodes_generation
                    subject_domain = "default_domain"
                    if subject in instance_type_dict and instance_type_dict[subject] in type_domain_dict:
                        subject_domain = type_domain_dict[instance_type_dict[subject]]
                    if subject+'\t'+subject_domain+'\n' not in nodes:
                        nodes.append(subject+'\t'+subject_domain+'\n')
                    if subject_domain not in node_types:
                        node_types.append(subject_domain)

                    object_domain = "default_domain"
                    ##case1: object is another instance
                    if object in instance_type_dict and instance_type_dict[object] in type_domain_dict:
                        object_domain = type_domain_dict[instance_type_dict[object]]
                    ##case2: object is property of str noun /numeric
                    elif object in property_type_dict and property_type_dict[object] in type_domain_dict:
                        object_domain = type_domain_dict[property_type_dict[object]]
                    if object+'\t'+object_domain+'\n' not in nodes:
                        nodes.append( object+'\t'+object_domain+'\n')
                    if object_domain not in node_types:
                        node_types.append(object_domain)

                    #relations_generation
                    if subject+'\t'+predicate+'\t'+object+'\n' not in relations:
                        relations.append(subject+'\t'+predicate+'\t'+object+'\n')

                
logging.info('End running')
print('node_types:'+str(len(node_types))) 
print(node_types)  

