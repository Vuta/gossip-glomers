package main

import (
	"encoding/json"
	"log"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	partitions map[string][]int
	offsets map[string]int
}

func main() {
	n := maelstrom.NewNode()
	state := &State{partitions: make(map[string][]int), offsets: make(map[string]int)}

	var mu sync.Mutex

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		partitionId := body["key"].(string) 
		value := int(body["msg"].(float64))
		mu.Lock()
		state.partitions[partitionId] = append(state.partitions[partitionId], value)
		mu.Unlock()
		offset := len(state.partitions[partitionId]) - 1

		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}

		result := make(map[string][][]int)

		for key, offset := range body["offsets"].(map[string]any) {
			offset := int(offset.(float64))
			partition := state.partitions[key]
			if offset >= 0 && offset <= len(partition) {
				endOffset := len(partition)
				if endOffset >= offset + 100 {
					endOffset = offset + 100
				}

				for i, v := range partition[offset:endOffset] {
					result[key] = append(result[key], []int{i+offset, v})
				}
			} else {
				return fmt.Errorf("offset %d for key %s is out of bound", offset, key)
			}
		}

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": result})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}

		mu.Lock()
		for key, offset := range body["offsets"].(map[string]any) {
			state.offsets[msg.Src+key] = int(offset.(float64))
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}

		result := make(map[string]int)
		for _, key := range body["keys"].([]any) {
			key := key.(string)
			result[key] = state.offsets[msg.Src+key]
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": result})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
