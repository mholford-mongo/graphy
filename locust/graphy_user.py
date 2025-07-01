from mongo_user import MongoUser

# Extend the base locust User.
# This adds a cache to hold root nodes so Locust can pick random nodes to start from
# This gets populated by running the get_root_nodes.py script
class GraphyUser(MongoUser):
    abstract = True
    def __init__(self, environment):
        super().__init__(environment)
        self.root_node_cache = []

    def on_start(self):
        with open('root-nodes.csv') as f:
            for line in f.readlines():
                self.root_node_cache.append(line.strip())
