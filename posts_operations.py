'''
Created on Nov 17, 2014

@author: qxi
'''
import os
import csv
import ast
from elasticsearch import Elasticsearch

# Global Settings
INDEX_NAME = 'posts'
TYPE_NAME = 'post'
ID_FIELD = 'postid'

ES_HOST = {
    "host" : "localhost", 
    "port" : 9200
}
request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}



# Read CSV file
# FILE_URL = os.getcwd() + "\\posts.data"
# csv_file_object = csv.reader(open(FILE_URL)) 
FILE_URL = os.getcwd() + "/posts.data"
csv_file_object = csv.reader(open(FILE_URL)) 

# Get the header (First line in the csv file)
header = csv_file_object.next()
header = [item.lower() for item in header]
print "CSV Header: "
print header

# Deal with each row in the file
bulk_data = [] 
for row in csv_file_object: 
    data_dict = {}
    for i in range(len(row)):
        if header[i] == "tags":
            # Title function:  Capitalize the first letter of each word in a string
            data_dict[header[i]] = (ast.literal_eval(row[i].title()))
        else:
            data_dict[header[i]] = row[i]
    op_dict = {
        "index": {
            "_index": INDEX_NAME, 
            "_type": TYPE_NAME, 
            "_id": data_dict[ID_FIELD]            
        }  
    }
    bulk_data.append(op_dict)
    # Delete id in the data 
    data_dict.pop(ID_FIELD, None)
    bulk_data.append(data_dict)   

# Print Bulk data
print "Bulk data:"
for line in bulk_data:
    print line  
    
# Create ES client
es = Elasticsearch(hosts = [ES_HOST])

# Delete index if it exists
if es.indices.exists(INDEX_NAME):
    print("Deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print("Response: '%s'" % (res))
    
# Create the Index    
print("Creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print("Response: '%s'" % (res))


#  Mapping setting

# post_mapping = {
#     'post': {
#         'properties': {
#             'company': {
#                 'type': 'string'
#              },
#             'email': {
#                 'type': 'string'
#             },
#             'postdate': {
#                 'type': 'string'
#             },
#             'tags': {
#                 'type': 'string', 'analyzer': 'keyword'
#             },
#             'username': {
#                 'type': 'string'
#             }
#         }
#     }
# }
post_mapping = {
    'post': {
        'properties': {
            'company': {
                'type': 'string'
             },
            'email': {
                'type': 'string'
            },
            'postdate': {
                'type': 'string'
            },
            'tags': {
                "type": "multi_field",
                    "fields": {
                        "tags": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "untouched": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                     }
             },
            'username': {
                'type': 'string'
            }
        }
    }
}
es.indices.put_mapping(index = INDEX_NAME, doc_type = TYPE_NAME, body = post_mapping)


# Bulk Index
print("Bulk indexing...")
res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
print("Response: '%s'" % (res))


# Search 

query_body1 ={
                "query": {
                   # "match_all": {}
                    "match": {
                        "tags" : "machine Learning"
                    }
                },
                "facets" : {
                    "tags" : { 
                        "terms" : {
                            "field" : "tags.untouched"
                        } 
                    }
               }            
}

query_body2 = {"query": { "query_string": { "query": "tags:Java" } }}
query_body3 = {"query": {"match_all": {}}}


print("Searching...")
res = es.search(index = INDEX_NAME, doc_type = TYPE_NAME, body = query_body1)
print(res)




 
    
