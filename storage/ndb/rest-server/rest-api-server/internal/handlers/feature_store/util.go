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
	nativeJson := ConvertAvroToJson(avroDeserialized)
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
		for key := range m {
			switch strings.Split(key, ".")[0] {
			case "struct":
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
				out = ConvertAvroToJson(m[key])
			}
		}
	case []interface{}:
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
