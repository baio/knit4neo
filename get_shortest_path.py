# -*- coding: utf-8 -*-
__author__ = 'baio'

import os
import pymongo as mongo
from  bson.objectid import ObjectId
from py2neo import neo4j, cypher

def get_shortest_path(name_1, name_2):
    client = mongo.MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB")]
    graph_db = neo4j.GraphDatabaseService(os.getenv("NEO4J_URI"))
    query = "START n=node:person(name=\"{}\"), m=node:person(name=\"{}\") MATCH p = shortestPath(n-[*]-m) RETURN p;".format(name_1, name_2)
    data, metadata = cypher.execute(graph_db, query)
    refs = map(lambda x: x.get_properties()["refs"], data[0][0].relationships)
    plain_refs = sum(refs, [])
    plain_refs = map(lambda x: ObjectId(x), plain_refs)
    items = list(db.contribs_v2.find({"items._id" : {"$in" : plain_refs}}))
    i = 0
    res = []
    for ref in refs:
        res.append(items[i:len(ref)])
        i += len(ref)
    print res
    return res


if __name__ == "__main__":
    get_shortest_path("володин валерий", "карманов александр")






