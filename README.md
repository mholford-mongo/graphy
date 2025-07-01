a
https:/branching hierarchy graph usecase
This repo includes code for building the database; querying paths from root nodes; and running this under high parallelism under Locust

## Create the graph

### dependencies
- python 3.9+
- numpy
- pymongo

### Load
```
python graphy2.py --uri <MONGODB URI> --levels <depth> 
```

See the source file for more execution options, comments and design rationale

It is recommended to create an index on `rels.source` after building the graph

### Query
```
python pathfinder.py --uri <MONGODB URI> --rootNode <simple_id of node>
```

## Load Test (locust)

The load test measures path finding under high load

### Pre-reqs
It is recommended to provision a dedicated VM to run locust.  You will want to make sure port 8089 is open.

Copy `locust/env.template` to `locust/.env` and fill in the `MONGODB_URI` including user and password in the connection string.

### Setup and run locust
- Push the `locust/` directory to the locust host.
- `cd locust`
- Run the `get_root_nodes` script to get node ids for locust to randomly choose from
  - ```
    python get_root_nodes.py
    ```
- Launch locust
  - ```
    locust --processes -1 -f find_paths.py --class-picker
    ```
- Open web browser at <locust-ip>:8089
- Run testsgithub.com/mholford-mongo/graphy.gitdd
