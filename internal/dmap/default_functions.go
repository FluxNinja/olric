package dmap

import "encoding/json"

const (
	AddFunction = "Add"
)

type counterState struct {
	Value float64 `json:"value"`
}

func add(_ string, currentState, arg []byte) (newState []byte) {
	// unmarshal currentState
	var cs counterState
	if currentState != nil {
		err := json.Unmarshal(currentState, &cs)
		if err != nil {
			return
		}
	}

	// unmarshal arg into float64
	var i float64
	err := json.Unmarshal(arg, &i)
	if err != nil {
		return
	}

	// add the integer to the counter
	cs.Value += i

	// marshal the new state
	newState, err = json.Marshal(cs)
	if err != nil {
		return
	}
	return
}
