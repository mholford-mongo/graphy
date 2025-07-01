import argparse
from pprint import pprint

from pymongo import MongoClient


# USAGE
# python pathfinder.py --uri <Mongo URI> --rootNode <node to start from>

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--uri', required=True, action='store', dest='uri', help='Mongo URI')
    parser.add_argument('--rootNode', required=True, action='store', dest='root_node', help='ID of root node')
    return parser.parse_args()


def exec(args):
    mc = MongoClient(args.uri)
    db = mc['graphy']
    coll = db['rels']
    chain = []

    match = {
        "$match": {
            "source": args.root_node
        }
    }
    graphLookup = {
        "$graphLookup": {
            "from": "rels",
            "startWith": "$source",
            "connectFromField": "target",
            "connectToField": "source",
            "as": "chain",
            "maxDepth": 99
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

    pprint(chain)
    return chain