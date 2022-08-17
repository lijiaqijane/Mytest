from mailbox import MMDF
from platform import node
import sys
import os
import logging
import json
from py2neo import Graph, Node, Relationship
import pandas as pd  

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)


logging.info('Starting running')

g1 = open('./processed_data/type_domain_dict.txt', 'w+', encoding='UTF-8')
g2 = open('./processed_data/instance_type_dict.txt', 'w+', encoding='UTF-8')
g3 = open('./processed_data/property_type_dict.txt', 'w+', encoding='UTF-8')


line_cnt = 0
# subjects, predicates, objects,temp = [], [], [], []
# id_mappings, type_domain_dict, instance_type_dict, property_type_dict = {}, {}, {}, {}

with open('./processed_data/id_mappings.json','r', encoding='UTF-8') as g:
    id_mappings = json.loads(g.read())
type_domain_dict, instance_type_dict, property_type_dict = {}, {}, {}

with open('freebase.txt','r', encoding='UTF-8') as f:
    for line in f:
        line_cnt += 1
        if line_cnt % 100000000 ==0:
            logging.info(line_cnt)
        
        line = line.strip('.\n').split('\t')
        
        if len(line) == 4:
            if '<http://rdf.freebase.com/' in line[2]:
                subject, predicate, object = line[0].strip('<>').replace('\"','').split('/')[-1], line[1].strip('<>').replace('\"','').split('/')[-1], line[2].strip('<>').replace('\"','').split('/')[-1]
            else:
                subject, predicate, object = line[0].strip('<>').replace('\"','').split('/')[-1], line[1].strip('<>').replace('\"','').split('/')[-1], line[2].strip('"')

            subject = id_mappings[subject] if subject in id_mappings else subject
            object = id_mappings[object] if object in id_mappings else object

            # if 'type.object.id' in predicate:
            #     if subject not in id_mappings:
            #         id_mappings[subject] = object
            # elif 'type.object.key' in predicate:
            #     if subject not in id_mappings:
            #         id_mappings[subject] = object

            if 'type.type.domain' in predicate:
                if subject not in type_domain_dict:
                    type_domain_dict[subject] = object
                    g1.write(subject+'\t'+object+'\n')

            if 'type.type.instance' in predicate:
                if object not in instance_type_dict:
                    instance_type_dict[object] = [subject]
                    g2.write(object+'\t'+subject+'\n')
                elif object in instance_type_dict and subject not in instance_type_dict[object] :
                    g2.write(object+'\t'+subject+'\n')

            if 'type.type.properties' in predicate:
                if object not in property_type_dict:
                    property_type_dict[object] = subject
                g3.write(object+'\t'+subject+'\n')
        

f.close()
logging.info('End running')

            #####generate object pairs
            # if ('m.' in subject or 'g.' in subject) and ('m.' in object or 'g.' in object):
            #     if predicate not in temp:
            #         print(subject+'\t'+predicate+'\t'+object)
            #         temp.append(predicate)



            ####generate schema tripples
            # if 'type.' in object:
            #     if predicate+'\t'+object  not in temp:
            #         temp.append(predicate+'\t'+object)
            #         g.write(subject+'\t'+predicate+'\t'+object+'\n')
            # else:
            #     if predicate  not in temp:
            #         temp.append(predicate)
            #         g.write(subject+'\t'+predicate+'\t'+object+'\n')
                    

        #     if subject.startswith('m.') or  subject.startswith('g.') or subject.startswith('"')  or '"' in subject:
        #         subject = 'MM'

        #     if predicate.startswith('m.') or  predicate.startswith('g.') or predicate.startswith('"')   or '"' in predicate:
        #         object = 'MM'


        #     if object.startswith('m.') or  object.startswith('g.') or object.startswith('"')  or '"' in object or object in ['XMLSchema#gYear', 'XMLSchema#gYearMonth', 'XMLSchema#dateTime','XMLSchema#date'] :
        #         object = 'MM'
            
        #     cand1 = subject+'\t'+predicate
        #     cand2 = predicate+'\t'+object

        #     if cand1 not in subjects and cand2 not in subjects:
        #         if subject=='MM' and object=='MM':
        #             pass
        #         else:
        #             subjects.append(cand1)
        #             subjects.append(cand2)
        #             g.write(subject+'\t'+predicate+'\t'+object+'\n')
        #             #print(subject+'\t'+predicate+'\t'+object)



