package main

import (
	"encoding/json"
	"log"
	"fmt"
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()

		partitionId := body["key"].(string) 
		value := int(body["msg"].(float64))
		offset := 0
		for {
			partition := []int{}
			kv.ReadInto(ctx, partitionId, &partition)
			err := kv.CompareAndSwap(ctx, partitionId, partition, append(partition, value), true)
			if err == nil {
				offset = len(partition)
				break
			}
		}

		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}

		ctx := context.Background()
		result := make(map[string][][]int)

		for key, offset := range body["offsets"].(map[string]any) {
			offset := int(offset.(float64))
			partition := []int{}
			kv.ReadInto(ctx, key, &partition)

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

		ctx := context.Background()
		for key, offset := range body["offsets"].(map[string]any) {
			kv.Write(ctx, msg.Src+key, int(offset.(float64)))
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}

		ctx := context.Background()
		result := make(map[string]int)
		for _, key := range body["keys"].([]any) {
			key := key.(string)
			offset := 0
			kv.ReadInto(ctx, msg.Src+key, &offset)
			result[key] = offset
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": result})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
