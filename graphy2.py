import argparse
from pymongo import MongoClient
import numpy as np
import uuid


# This builds a hierarchical "graph" of specified depth.  Each level of the hierarchy gets a collection
# and a "rels" collection holds each relationship.  To make it easy to test different number of hops
# this fudges up some simple naming conventions.
# The 'root' collection is called "acoll" and the collection for each level gets the next char.  So, if
# depth  is 5, we have collections acoll, bcoll, ccoll, dcoll, ecoll...
# Starting from the root, each level creates a random number of rels to the next level.  The name of the relationship
# is based on the collections it links.  The "AB" rel links acoll and bcoll; "BC" links bcol and ccoll; and so on..
# As a consequence of how this is built the collections have more docs going from Root ('acoll') to the last collection


# Faster random number generation by getting a slice of 10 million random numbers from numpy
# When generating random content, call next() to iterate through these.  When it runs out,
# a new slice is retrieved; so users don't have to worry about catching StopIteration
# This concept is extended for getting UUIDs also
class Pool:
    def __init__(self, size, slice_fn):
        self.size = size
        self.slice_fn = slice_fn
        self.slice = self.slice_fn(self.size)
        self.iter = iter(self.slice)

    def __next__(self):
        try:
            return next(self.iter)
        except StopIteration:
            self.slice = self.slice_fn(self.size)
            self.iter = iter(self.slice)
            return next(self.iter)


class UUIDPool(Pool):
    def __init__(self, size):
        def slice_fn(s):
            return [uuid.uuid4() for _ in range(s)]

        super().__init__(size, slice_fn)


class RndPool(Pool):
    def __init__(self, size):
        self.rng = np.random

        def slice_fn(s):
            return self.rng.random(s)

        super().__init__(size, slice_fn)

    def pick(self, num, l):
        return self.rng.choice(l, size=num, replace=False)


class BinomialDistributionPool(Pool):
    def __init__(self, size, n, p):
        self.n = n
        self.p = p
        self.rng = np.random.default_rng()

        def slice_fn(s):
            return self.rng.binomial(self.n, self.p, s)

        super().__init__(size, slice_fn)


alph = 'abcdefghijklmnopqrstuvwxyz'
ttl_cnt = 1


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--uri', required=True, action='store', dest='uri', help='MongoDB uri')
    parser.add_argument('--db', required=False, default='graphy', action='store', dest='db', help='Name of DB in Mongo')
    parser.add_argument('--numRootDocs', required=False, default=100_000, type=int, action='store',
                        dest='num_root_docs', help='Number of docs in the root vertex collections')
    parser.add_argument('--relNParam', required=False, default=5, type=int, action='store', dest='rel_n_param',
                        help='Number of tests (Binomial Distribution)')
    parser.add_argument('--relPParam', required=False, default=0.5, type=float, action='store', dest='rel_p_param',
                        help='P param (Binomial Distribution)')
    parser.add_argument('--batchSize', required=False, default=1_000, type=int, action='store', dest='batch_size',
                        help='Batch size for Mongo inserts')
    parser.add_argument('--levels', required=False, default=5, type=int, action='store', dest='levels',
                        help='Number of levels in the hierarchy')
    parser.add_argument('--drop', required=False, default=True, type=bool, action='store', dest='drop',
                        help='Drop existing collections')

    return parser.parse_args()


def rnd_words(size, words, rnds):
    output = ''
    ttl_words = len(words)
    while len(output) < size:
        rnd = int(next(rnds) * ttl_words)
        output += f' {words[rnd]}'
    return output


# Random node.  Some junk data to give the doc some size
def rnd_doc(uuids, rnds, words, simple_id):
    return {
        'fake_id': str(next(uuids)),
        'description': rnd_words(200, words, rnds),
        'boilerplate': rnd_words(2000, words, rnds),
        'diff_id': str(next(uuids)),
        'per': [next(rnds) for _ in range(20)],
        'simple_id': simple_id
    }


def mk_rel_doc(src_id, src_coll, tgt_id, tgt_coll, rel_name):
    return {
        'source': src_id,
        'source_coll': src_coll,
        'target': tgt_id,
        'target_coll': tgt_coll,
        'type': rel_name
    }


# flush any of the batches that has > BATCH_SIZE docs
def flush_batches(batches, size, cnt_so_far):
    to_empty = []
    for coll, b in batches.items():
        if len(b) >= size:
            print(f'*** Insert batch to {coll.name}')
            coll.insert_many(b)
            to_empty.append(coll)
    if len(to_empty) > 0:
        for e in to_empty:
            batches[e] = []
        sz_map = {k.name: len(batches[k]) for k in batches.keys()}
        sk = sorted(sz_map.keys())
        print(', '.join([f'{k}: {sz_map[k]}' for k in sk]))
        print(f'{cnt_so_far} vertex docs seen so far')
    return batches


# Recursive method to create nodes and rels for each step of the hierarchy
def extend_chain(source_id, depth, batches, nodes, rel_coll, rel_cnts, uuids, rnds, words):
    if depth >= len(nodes):
        return
    num_rels = next(rel_cnts)
    global ttl_cnt
    for _ in range(num_rels):
        tdoc = rnd_doc(uuids, rnds, words, f'{alph[depth]}-{ttl_cnt}')
        tid = tdoc['simple_id']
        tgt_pre, src_pre = alph[depth], alph[depth - 1]

        doc = mk_rel_doc(source_id, f'{src_pre}coll', tid, f'{tgt_pre}coll',
                         f'{src_pre.upper()}{tgt_pre.upper()}')
        batches[rel_coll].append(doc)
        batches[nodes[depth]].append(tdoc)
        ttl_cnt += 1

        # Create nodes and rels for next level of hierarchy
        extend_chain(tid, depth + 1, batches, nodes, rel_coll, rel_cnts, uuids, rnds, words)


def exec(args):
    uuids = UUIDPool(100_000)
    rnds = RndPool(10_000_000)
    # pool of random numbers to determine how many rel's to create between each node
    rel_cnts = BinomialDistributionPool(100_000, args.rel_n_param, args.rel_p_param)
    words = []

    # Read the list of words in the system dictionary into a list
    # For generating random text
    with open('/usr/share/dict/words', 'r') as f:
        for line in f.readlines():
            words.append(line.strip())

    mc = MongoClient(args.uri)
    db = mc[args.db]
    num_node_types = args.levels
    nodes = []
    for x in range(num_node_types):
        collname = f'{alph[x]}coll'  # e.g. 'acoll', 'bcoll',, etc.
        nodes.append(db[collname])

    rel_coll = db['rels']

    if args.drop:
        # drop existing collections
        [x.drop() for x in [*nodes, rel_coll]]

    # keep batches of docs for each collection
    # so they can be written to Mongo when batch limit is reached
    batches = {nodes[x]: [] for x in range(len(nodes))}
    batches[rel_coll] = []
    cnt = 0
    global ttl_cnt

    # Root level
    while cnt < args.num_root_docs:
        depth = 1
        batches = flush_batches(batches, args.batch_size, ttl_cnt)
        # Generate a random acoll doc
        sdoc = rnd_doc(uuids, rnds, words, f'{alph[0]}-{cnt}')
        batches[nodes[0]].append(sdoc)
        cnt = cnt + 1
        ttl_cnt = ttl_cnt + 1
        sid = sdoc['simple_id']

        # Create next level of hierarchy
        extend_chain(sid, depth, batches, nodes, rel_coll, rel_cnts, uuids, rnds, words)

    # For reference, this "unrolls" the extend_chain for an example 4-level hierarchy

    # batches = {a_coll: [], b_coll: [], c_coll: [], d_coll: [], rel_coll: []}
    # while cnt < args.num_coll_docs:
    #     batches = flush_batches(batches, args.batch_size, ttl_cnt)
    #     adoc = rnd_doc(uuids, rnds, words, f'a-{cnt}')
    #     batches[a_coll].append(adoc)
    #     cnt = cnt + 1
    #     ttl_cnt = ttl_cnt + 1
    #     aid = adoc['simple_id']
    #     num_rels = next(rel_cnts)
    #     for _ in range(num_rels):
    #         bdoc = rnd_doc(uuids, rnds, words, f'b-{ttl_cnt}')
    #         bid = bdoc['simple_id']
    #         batches[rel_coll].append(mk_rel_doc(aid, bid, 'AB'))

    #         batches[b_coll].append(bdoc)
    #         ttl_cnt = ttl_cnt + 1
    #         num_bc_rels = next(rel_cnts)
    #         for _ in range(num_bc_rels):
    #             cdoc = rnd_doc(uuids, rnds, words, f'c-{ttl_cnt}')
    #             cid = cdoc['simple_id']
    #             batches[rel_coll].append(mk_rel_doc(bid, cid, 'BC'))
    #             batches[c_coll].append(cdoc)
    #             ttl_cnt = ttl_cnt + 1
    #             num_cd_rels = next(rel_cnts)
    #             for _ in range(num_cd_rels):
    #                 ddoc = rnd_doc(uuids, rnds, words, f'd-{ttl_cnt}')
    #                 did = ddoc['simple_id']
    #                 batches[rel_coll].append(mk_rel_doc(cid, did, 'CD'))
    #                 batches[d_coll].append(ddoc)
    #                 ttl_cnt = ttl_cnt + 1

    # Flush whatever is left in the batches.  Size arg set to 0 so everything left over gets inserted
    flush_batches(batches, 0, ttl_cnt)


def main():
    args = setup_args()
    exec(args)


# Requirements:
# Python 3.9 +
# pip install numpy
# pip install pymongo
# USAGE:
# python graphy2.py --uri <MongoURI> --levels <depth> --numRootDocs <num>
# Other params:
# --batchSize: defaults to 1000
# --db: name of db; defaults to 'graphy'
# Params around the binomial distribution used to determine the number of relations
# are also tunable
if __name__ == '__main__':
    main()
