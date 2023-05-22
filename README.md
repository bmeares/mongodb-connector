# `mongodb-connector` Meerschaum Plugin

The `mongodb-connector` [Meerschaum plugin](https://meerschaum.io/reference/plugins/writing-plugins/) provides the `MongoDBConnector`, an [instance connector](https://meerschaum.io/reference/connectors/) interface for MongoDB.

For example, like `sql:main`, you could now store pipes on `mongodb:main`.

## How to Use

The `MongoDBConnector` (type `mongodb`) requires two arguments:

- `uri`  
  Connection string to your MongoDB cluster, e.g.:  
  ```
  mongodb://localhost:27017
  ```
  **Note**: The database portion of the MongoDB URI is the auth database and does not refer to where the collections are stored. That's set in another key:

- `database`  
  Like the `SQLConnector`, each connector is "bound" to a single database. If the database doesn't exist, it will be created when needed.

Define a new connector with these keys, such as `mongodb:local` set in `docker-compose.yaml`:

```bash
export MRSM_MONGODB_LOCAL='{
  "uri": "mongodb://localhost:27017",
  "database": "meerschaum"
}'
```

## Test It Out

Start the Docker containers:

```bash
docker compose up -d --build
```

Hop into the `mrsm-compose` container:

```bash
docker compose exec -it mrsm-compose bash
```

Run the compose file to sync the two example pipes:

```bash
mrsm compose run
```