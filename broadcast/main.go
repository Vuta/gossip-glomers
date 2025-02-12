package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	values []int
	messages map[string][]int
}

func main() {
	n := maelstrom.NewNode()
	messages := map[string][]int{}
	state := &State{values: []int{}, messages: messages}
	var mu sync.Mutex

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for k, v := range body["topology"].(map[string]any) {
			for _, vv := range v.([]any) {
				if k == n.ID() {
					state.messages[vv.(string)] = []int{}
				}
			}
		}

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["messages"] = state.values
		body["type"] = "read_ok"

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgValue := int(body["message"].(float64))

		mu.Lock()
		if isIncluded(msgValue, state.values) {
			mu.Unlock()
			n.Reply(msg, map[string]any{"type":"broadcast_ok"})

			return nil
		} else {
			state.values = append(state.values, msgValue)
			n.Reply(msg, map[string]any{"type":"broadcast_ok"})
		}
		mu.Unlock()

		return nil
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		for _, message := range body["messages"].([]any) {
			message := int(message.(float64))
			if !isIncluded(message, state.values) {
				state.values = append(state.values, message)
			}

			if !isIncluded(message, state.messages[msg.Src]) {
				state.messages[msg.Src] = append(state.messages[msg.Src], message)
			}
		}
		mu.Unlock()

		return nil
	})

	go func() {
		for {
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			for p, _ := range state.messages {
				messages := delta(state.messages[p], state.values)
				n.Send(p, map[string]any{"type": "gossip", "messages": messages})
			}
			mu.Unlock()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func isIncluded[S ~[]E, E comparable](message E, values S) bool {
	for _, v := range values {
		if v == message {
			return true
		}
	}

	return false
}

// serious system uses Merkle tree
func delta(list1, list2 []int) []int {
	result := []int{}
	a := make(map[int]struct{}, len(list1))

	for _, i := range list1 {
		a[i] = struct{}{}
	}

	for _, i := range list2 {
		_, ok := a[i]
		if !ok {
			result = append(result, i)
		}
	}

	return result
}
