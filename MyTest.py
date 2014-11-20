'''
Created on Nov 17, 2014
@author: qxi
'''
from elasticsearch import Elasticsearch

ES_HOST = {
    "host" : "localhost", 
    "port" : 9200
}

INDEX_NAME = 'posts'

doc1 = {
    'Email': 'a@gmail.com',
    'Company': 'Google',
    'Tags': ['C++', 'Java']
}

doc2 = {
    'Email': 'b@gmail.com',
    'Company': 'Facebook',
    'Tags': ['Python', 'Java']
}

request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}


# Create ES client
es = Elasticsearch(hosts = [ES_HOST])

# Delete index if it exists
if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))
    
# Create the Index    
print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print(" response: '%s'" % (res))

# Add a post
res = es.index(index="posts", doc_type='post', id=1, body=doc1)
print(res['created'])  

# Add another post
res = es.index(index="posts", doc_type='post', id=2, body=doc2)
print(res['created'])  
    
    
    