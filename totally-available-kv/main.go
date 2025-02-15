package main

import (
	"encoding/json"
	"log"
	"sync"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Operation struct {
	op string
	key int
	value *int
}

func (t Operation) toJSON() []any {
	return []any{t.op, t.key, t.value}
}

type Store struct {
	records map[int]int
	mu sync.RWMutex
}

func main() {
	nodes := []string{}
	n := maelstrom.NewNode()
	var mu sync.RWMutex
	store := &Store{records: make(map[int]int), mu: mu}

	n.Handle("init", func(msg maelstrom.Message) error {
		for _, node := range n.NodeIDs() {
			if node != n.ID() {
				nodes = append(nodes, node)
			}
		}

		return nil
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		data := []any{}
		changes := make(map[int]int)

		for _, transaction := range body["txn"].([]any) {
			op := transaction.([]any)[0].(string)
			key := int(transaction.([]any)[1].(float64))

			var operation Operation

			switch op {
			case "r":
				mu.RLock()
				v, _ := store.records[key]
				mu.RUnlock()
				operation = Operation{op: "r", key: key, value: &v}
			case "w":
				v := int(transaction.([]any)[2].(float64))
				mu.Lock()
				store.records[key] = v
				mu.Unlock()

				operation = Operation{op: "w", key: key, value: &v}
				changes[key] = v
			}

			data = append(data, operation.toJSON())
		}

		for _, node := range nodes {
			n.Send(node, map[string]any{"type": "write", "changes": changes})
		}

		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": data})
	})

	n.Handle("write", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		for k, v := range body["changes"].(map[string]any) {
			key, _ := strconv.Atoi(k)
			value := int(v.(float64))

			store.records[key] = value
		}
		mu.Unlock()

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
