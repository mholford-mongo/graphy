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
**NB** - The `.env` file is intended to store secret values like passwords.. be careful not to check it into github! 

### Setup and run locust
- Push the `locust/` directory to the locust host.
- `cd locust`
- Run the `get_root_nodes` script to get node ids for locust to randomly choose from
  - ```
    python get_root_nodes.py
    ```
  - note: we run this as a separate process first so the retrieval doesn't impact the timings of the query under test
  

- Launch locust
  - ```
    locust --processes -1 -f find_paths.py --class-picker
    ```
    - Setting `processes` to `-1` means it will use `2*cores -1` processes
       - it's python so multi-processing is used to achieve better parallelism than would be available from multi-threading
    - The `-f` parameter can hold a comma-separated list of files in case there are multiple locust class files
        - Enclose this in quotes e.g. `-f "file1.py,file2.py"`
    - `--class-picker` will let you choose the locust class in the UI 
    
  - See comments in code for more details

- Open web browser at <locust-ip>:8089
- Run tests
