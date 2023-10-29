package main

import (
	"avro_package/message"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

func handleErr(err error, msg string) bool {
	if err != nil {
		log.Panic(msg, err)
		os.Exit(1)
		return true
	}

	return false
}

func createDBConn() (*sql.DB, error) {
	s2Password := os.Getenv("S2_PASSWORD")

	db, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(127.0.0.1:3306)/information_schema", s2Password))
	handleErr(err, "trying to connect to S2 DB")

	return db, nil
}

func createKafkaConn() (*kafka.Conn, error) {
	conn, err := kafka.Dial("tcp", "localhost:29092")
	handleErr(err, "trying to connect to Kafka")

	return conn, nil
}

func createKafkaTopic(conn *kafka.Conn, topicName string) {
	conn, err := kafka.Dial("tcp", "localhost:29092")
	handleErr(err, "trying to connect to Kafka")

	controller, err := conn.Controller()
	handleErr(err, "trying to initiate kafka controller")

	conncontroller, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	handleErr(err, "trying to dial to Kafka")

	err = conncontroller.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	handleErr(err, "trying to create kafka topic")
}

func listTopics(conn *kafka.Conn) {
	paritions, err := conn.ReadPartitions()
	handleErr(err, "trying to read partitions")

	m := map[string]struct{}{}

	for _, p := range paritions {
		m[p.Topic] = struct{}{}
	}

	for k := range m {
		fmt.Println(k)
	}
}

func createDbAndTable(db *sql.DB) {
	s2DbName := os.Getenv("S2_DB_NAME")

	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + s2DbName)
	handleErr(err, "trying to create DB")

	_, err = db.Exec("USE " + s2DbName)
	handleErr(err, "trying to use DB")

	_, err = db.Exec(" CREATE TABLE IF NOT EXISTS sdk_telemetry ( env_id bigint NOT NULL, type varchar(100) DEFAULT NULL, sent_time bigint NOT NULL, library varchar(1024) DEFAULT NULL, heap_version varchar(1024) DEFAULT NULL, user_agent varchar(1024) DEFAULT NULL, ip varchar(1024) DEFAULT NULL, custom_message varchar(1024) DEFAULT NULL, stacktrace varchar(500) DEFAULT NULL, week AS date_trunc('week', from_unixtime(sent_time / 1000)) persisted DATE, SORT KEY (env_id, week, sent_time), KEY (env_id) USING HASH, KEY (library) USING HASH, KEY (custom_message) USING HASH, KEY (heap_version) USING HASH );")
	handleErr(err, "trying to create table")
}

func writeMessageToKafka(topicName string, message []byte) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:29092", topicName, 0)
	handleErr(err, "trying to create Kafka dial handler")

	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: message},
	)
	handleErr(err, "trying to write messages to Kafka")
}

func addKafkaPipeline(db *sql.DB, topicName string) {
	_, err := db.Exec(fmt.Sprintf(`
		create pipeline if not exists kafka_telemtry_ingestion AS 
			LOAD DATA KAFKA 'localhost:29092/%s'
			CONFIG '{
				"sasl.mechanism": "PLAIN"
			}'
			SKIP DUPLICATE KEY ERRORS
			SKIP CONSTRAINT ERRORS
			INTO TABLE sdk_telemetry FORMAT AVRO SCHEMA '{
				"type": "record",
				"name": "TelmetryObject",
				"fields" : [
				  {"name": "env_id",          "type": "long"},
				  {"name": "type",            "type": "string"},
				  {"name": "custom_message",  "type": "string"},
				  {"name": "value",           "type": "string"},
				  {"name": "stacktrace",      "type": "string"},
				  {"name": "heap_version",    "type": "string"},
				  {"name": "library",         "type": "string"},
				  {"name": "user_agent",      "type": "string"},
				  {"name": "ip",              "type": "string"},
				  {"name": "sent_time",       "type": "long"}
				]
			  }'`, topicName))

	handleErr(err, "trying to create pipeline")
}

func main() {
	messageHandler, err := message.New()
	handleErr(err, "initiating new message handler")

	msg, err := messageHandler.ToAvro([]message.Message{{
		EnvId:         123456,
		Type:          "data",
		CustomMessage: "foo",
		Value:         "bar",
		Stacktrace:    "stacktrace",
		HeapVersion:   "2.18",
		Library:       "library",
		UserAgent:     "Chrome linux",
		IP:            "127.0.0.1",
		SentTime:      1698560695,
	}})
	handleErr(err, "compression err")

	// following is only for testing
	// messageHandler.ToData(msg)

	topic := os.Getenv("KAFKA_TOPIC")

	db, err := createDBConn()
	handleErr(err, "trying to establish db connection")
	defer db.Close()

	kafka, err := createKafkaConn()
	handleErr(err, "trying to create kafka connection")
	defer kafka.Close()

	createDbAndTable(db)
	createKafkaTopic(kafka, topic)
	// addKafkaPipeline(db, topic)
	listTopics(kafka)
	writeMessageToKafka(topic, msg)
}
