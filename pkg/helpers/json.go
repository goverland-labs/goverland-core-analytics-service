package helpers

import (
	"encoding/json"
)

func AsJSON(v any) string {
	marshaled, _ := json.Marshal(v)

	return string(marshaled)
}
