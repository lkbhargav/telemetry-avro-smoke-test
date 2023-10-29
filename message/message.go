package message

import (
	"bytes"
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

func (m *MessageHandler) ToAvro(messages []Message) ([]byte, error) {
	var ocfBuffer bytes.Buffer

	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		Codec: m.c,
		W:     &ocfBuffer,
	})
	if err != nil {
		return []byte{}, fmt.Errorf("trying to create an OCFWriter. Err: %w", err)
	}

	for _, structMsg := range messages {
		msg, err := structMsg.toDatum()
		if err != nil {
			return []byte{}, fmt.Errorf("trying to convert struct message to json. Err: %w", err)
		}

		err = writer.Append([]interface{}{msg})
		if err != nil {
			return []byte{}, fmt.Errorf("trying to append data to message. Err: %w", err)
		}
	}

	return ocfBuffer.Bytes(), nil
}

// func (m *MessageHandler) ToData(data []byte) error {
// 	ocfReader, err := goavro.NewOCFReader(strings.NewReader(string(data)))
// 	if err != nil {
// 		return err
// 	}

// 	fmt.Println("Records in OCF File")

// 	for ocfReader.Scan() {
// 		record, err := ocfReader.Read()
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Println("record", record)
// 	}

// 	return nil
// }
