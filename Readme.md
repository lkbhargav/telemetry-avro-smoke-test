How to work with it,

1. Signup to Singlestore with personal account and grab license key (S2_LICENSE_KEY) and plug it into `.env` file.

2. Add the following variables to the `.env` file,
* export S2_PASSWORD="<pick a password>"
* export S2_LICENSE_KEY="<S2 license key>"
* export S2_DB_NAME="telemetry_db"
* export KAFKA_TOPIC="telemetry_topic"
* export S2_PIPELINE_NAME="kafka_telemtry_ingestion"

3. Source the env file `source .env`

4. Run docker compose command to bring services up and running,
`docker-compose up -d`

5. Let the DB come up online (takes 15s - confirm by navigating to S2 studio on `http://localhost:8080/cluster` | default S2 user would be `root`) and then run the Go project (`go run main.go`) to create S2 DB, tables, Kafka topic, the pipeline to start ingesting data, and writes message to Kafka topic.

6. To confirm if data is being ingested to S2 by the pipeline you can run the select query on the table (`select * from sdk_telemetry;`) on S2 studio.

For debugging,

You can connect to S2 Sql (using Mysql client) running in the Docker container by running the following,

`mysql -u root -P 3306 -h 0.0.0.0 -p<DB password>`

Arbitary queries for reference,

`show pipelines;`

`drop pipeline kafka_telemtry_ingestion;`