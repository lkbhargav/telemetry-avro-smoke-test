package message

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/linkedin/goavro"
)

type MessageType string

const (
	Data  MessageType = "data"
	Error MessageType = "error"
)

type Message struct {
	EnvId         int64       `json:"env_id"`
	Type          MessageType `json:"type"`
	CustomMessage string      `json:"custom_message"`
	Value         string      `json:"value"`
	Stacktrace    string      `json:"stacktrace"`
	HeapVersion   string      `json:"heap_version"`
	Library       string      `json:"library"`
	UserAgent     string      `json:"user_agent"`
	IP            string      `json:"ip"`
	SentTime      int64       `json:"sent_time"`
}

func (m Message) toDatum() (map[string]interface{}, error) {
	var resp map[string]interface{}

	d, err := json.Marshal(&m)
	if err != nil {
		return nil, fmt.Errorf("error marshaling. Err: %w", err)
	}

	err = json.Unmarshal(d, &resp)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling. Err: %w", err)
	}

	return resp, err
}

type MessageHandler struct {
	c *goavro.Codec
	s string
}

func New() (MessageHandler, error) {
	schema, err := os.ReadFile("./telemetry_schema.json")
	if err != nil {
		return MessageHandler{}, fmt.Errorf("trying to read the file. Err: %w", err)
	}

	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return MessageHandler{}, fmt.Errorf("trying to generate the AVRO codec for the given schema. Err: %w", err)
	}

	return MessageHandler{
		c: codec,
		s: string(schema),
	}, nil
}

func (m *MessageHandler) ToAvro(message Message) ([]byte, error) {
	codec, err := goavro.NewCodec(m.s)
	if err != nil {
		return []byte{}, fmt.Errorf("trying to create a codec instance. Err: %w", err)
	}

	msg, err := message.toDatum()
	if err != nil {
		return []byte{}, fmt.Errorf("trying to convert struct message to json. Err: %w", err)
	}

	avroData, err := codec.BinaryFromNative(nil, msg)
	if err != nil {
		return []byte{}, fmt.Errorf("trying to get avro binary data from native form")
	}

	return avroData, nil
}
