__author__ = 'baio'

import os
import pymongo as mongo
from py2neo import neo4j

def unique(seq, idfun=None):
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result

def convert():
    client = mongo.MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB")]
    graph_db = neo4j.GraphDatabaseService(os.getenv("NEO4J_URI")) #"http://localhost:7474/db/knit/")
    graph_db.clear()
    graph_db.get_or_create_index(neo4j.Node, "person")
    graph_db.get_or_create_index(neo4j.Node, "org")
    graph_db.get_or_create_index(neo4j.Relationship, "pp")
    graph_db.get_or_create_index(neo4j.Relationship, "po")
    graph_db.get_or_create_index(neo4j.Relationship, "oo")
    batch = neo4j.WriteBatch(graph_db)

    for c in db.contribs_v2.find():

        if "items" in c:
            batch.clear()
            for i in c["items"]:
                batch.get_or_create_indexed_node("person", "name", i["object"], {"name" : i["object"]})
                batch.get_or_create_indexed_node("person", "name", i["subject"], {"name" : i["subject"]})

            nodes = batch.submit()

            node_ix = 0
            predicates = []
            for i in c["items"]:
                n_1 = nodes[node_ix]
                n_2 = nodes[node_ix + 1]
                node_ix += 2
                for p in i["predicates"]:
                    if n_1.id != n_2.id:
                        predicates.append(p)
                        batch.get_or_create_indexed_relationship(p["type"][:2], "knows", "{}&{}&{}".format(n_1.id, n_2.id, p["type"]), n_1, p["type"], n_2)

            relations = batch.submit()

            for ix in xrange(0, len(relations)):
                r = relations[ix]
                p = predicates[ix]
                tags = r.get_properties().get("tags", [])
                tags += [p["val"]]
                tags = list(set(tags))
                batch.set_property(r, "tags", tags)

            batch.submit()


if __name__ == "__main__":
    convert()





