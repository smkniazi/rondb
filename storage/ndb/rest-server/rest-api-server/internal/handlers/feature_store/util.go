package feature_store

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hamba/avro/v2"
	"hopsworks.ai/rdrs/internal/log"
)

func DeserialiseComplexFeature(value *json.RawMessage, schema *avro.Schema) (*interface{}, error) {
	valueString, err := decodeJSONString(value)
	if err != nil {
		if log.IsDebug() {
			log.Debugf("Failed to unmarshal. Value: %s", valueString)
		}
		return nil, err
	}

	jsonDecode, err := base64.StdEncoding.DecodeString(valueString)
	if err != nil {
		if log.IsDebug() {
			log.Debugf("Failed to decode base64. Value: %s", valueString)
		}
		return nil, err
	}
	var avroDeserialized interface{}
	err = avro.Unmarshal(*schema, jsonDecode, &avroDeserialized)
	if err != nil {
		if log.IsDebug() {
			log.Debugf("Failed to deserialize avro")
		}
		return nil, err
	}

	// fmt.Println("Schema %s\n", (*schema).String())
	// fmt.Println("Here -> %v\n", avroDeserialized)

	// by, err := json.MarshalIndent(avroDeserialized, "", " ")
	// fmt.Println("Avro %s\n", string(by))
	nativeJson := ConvertAvroToJson(avroDeserialized)
	by, err := json.MarshalIndent(nativeJson, "", " ")
	fmt.Printf("Avro %s\n", string(by))

	return &nativeJson, err
}

func decodeJSONString(raw *json.RawMessage) (string, error) {
	// Convert the raw message to a string
	rawStr := string(*raw)
	// Check that the first and last characters are quotes
	if len(rawStr) < 2 || rawStr[0] != '"' || rawStr[len(rawStr)-1] != '"' {
		return "", fmt.Errorf("invalid JSON string format")
	}
	// Remove the surrounding quotes
	unquotedStr := rawStr[1 : len(rawStr)-1]
	// Replace escape sequences with their actual characters
	decodedStr := strings.ReplaceAll(unquotedStr, `\"`, `"`)
	return decodedStr, nil
}

func ConvertAvroToJson(o interface{}) interface{} {
	var out interface{}
	switch o.(type) {
	case map[string]interface{}: // union or map
		m := o.(map[string]interface{})
		// fmt.Printf("Outer Case map\n")
		for key := range m {
			// sw := strings.Split(key, ".")[0]
			// fmt.Printf("Inner case ConvertAvroToJson called %v, key %s,  switch %s\n", m, key, sw)
			switch strings.Split(key, ".")[0] {

			case "data":
				result := make(map[string]interface{})
				structValue := m[key].(map[string]interface{})
				for structKey := range structValue {
					result[structKey] = ConvertAvroToJson(structValue[structKey])
				}
				out = result
			case "sturct":
				result := make(map[string]interface{})
				structValue := m[key].(map[string]interface{})
				for structKey := range structValue {
					result[structKey] = ConvertAvroToJson(structValue[structKey])
				}
				out = result
			case "array":
				result := make([]interface{}, 0)
				for _, item := range m[key].([]interface{}) {
					itemJson := ConvertAvroToJson(item)
					result = append(result, itemJson)
				}
				out = result
			default:
				// fmt.Printf("default case %v\n", m[key])
				out = ConvertAvroToJson(m[key])
			}
		}
	case []interface{}:
		//fmt.Printf("Outer Case interface \n ")
		result := make([]interface{}, 0)
		for _, item := range o.([]interface{}) {
			itemJson := ConvertAvroToJson(item)
			result = append(result, itemJson)
		}
		out = result
	default:
		out = o
	}
	return out
}
