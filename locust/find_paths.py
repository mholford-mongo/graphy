import os
import random

from dotenv import load_dotenv
from pymongo import MongoClient

from graphy_user import GraphyUser
from mongo_user import mongodb_task


class FindPaths(GraphyUser):
    load_dotenv() # Load .env to get secret values
    mongo_uri = os.environ['MONGO_URI']

    def __init__(self, environment):
        super().__init__(environment)
        self.mc = MongoClient(self.mongo_uri)
        self.num_root_nodes = len(self.root_node_cache)

    @mongodb_task()  # Decorator defined in mongo_user.py; don't forget the parens!
    def find_paths(self):
        resp = {}

        # pick a random start node
        rand_root_node = random.choice(self.root_node_cache)

        chain = []

        db = self.mc['graphy']
        coll = db['rels']

        # Aggregation
        match = {
            "$match": {
                "source": rand_root_node
            }
        }
        graphLookup = {
            "$graphLookup": {
                "from": "rels",
                "startWith": "$source",
                "connectFromField": "target",
                "connectToField": "source",
                "as": "chain"
            }
        }
        limit = {
            "$limit": 1
        }
        project = {
            "$project": {
                "chain": {
                    "$sortArray": {
                        "input": "$chain",
                        "sortBy": {
                            "source": 1,
                            "target": 1
                        }
                    }
                }
            }
        }

        project2 = {
            "$project": {
                "chain.source": 1,
                "chain.target": 1,
                "_id": 0
            }
        }

        pipeline = [match, graphLookup, limit, project, project2]
        resp = coll.aggregate(pipeline)

        for r in resp:
            chain.append(r['chain'])

        return chain




