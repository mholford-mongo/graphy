from mongo_user import MongoUser


class GraphyUser(MongoUser):
    abstract = True
    def __init__(self, environment):
        super().__init__(environment)
        self.root_node_cache = []

    def on_start(self):
        with open('root-nodes.csv') as f:
            for line in f.readlines():
                self.root_node_cache.append(line.strip())
