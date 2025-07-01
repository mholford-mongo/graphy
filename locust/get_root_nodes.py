from dotenv import load_dotenv
from pymongo import MongoClient
import os

def main():
    load_dotenv()
    with open('root-nodes.csv', 'w') as f:
        mc = MongoClient(os.environ['MONGO_URI'])
        db = mc['graphy']
        coll = db['acoll']
        results = coll.find(projection={'simple_id': 1})
        for r in results:
            f.write(str(r['simple_id']) + '\n')




if __name__ == '__main__':
    main()