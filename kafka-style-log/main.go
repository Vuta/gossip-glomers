package main

import (
	"encoding/json"
	"log"
	"fmt"
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	hash "bitbucket.org/pcastools/hash"
)

type SendEvent struct {
	key string
	value int
}

type State struct {
 	partitions map[string][]int
}

func main() {
	n := maelstrom.NewNode()
	// We don't need LinKV service anymore since the routing ensure partitions will be written by their respectively leader node
	kv := maelstrom.NewSeqKV(n)
	state := &State{partitions: make(map[string][]int)}
	sendCh := make(chan SendEvent)
	doneCh := make(chan bool)

	// this goroutine synchronize write requests
	go func() {
		ctx := context.Background()

		for {
			select {
			case m := <-sendCh:
				state.partitions[m.key] = append(state.partitions[m.key], m.value)
				doneCh <- true

				// Unfortunately the KV service doesn't provide a range read operation,
				// so if we store each message in separate entry, poll PRC will issue a lot of reads to KV, which is really bad.
				// For now writing the whole thing still yield good enough result, so I'm gonna keep it this way.
				// TODO: Use segment to avoid writing the whole partition
				kv.Write(ctx, m.key, state.partitions[m.key])
			}
		}
	}()

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := int(body["msg"].(float64))
		partitionId := body["key"].(string)

		// round robin keys to leader node, only works with stable network
		leaderIndex := hash.ByteSlice([]byte(partitionId)) % uint32(len(n.NodeIDs()))
		leader := n.NodeIDs()[leaderIndex]
		if leader == n.ID() {
			sendCh <- SendEvent{key: partitionId, value: value}

			select {
			case <-doneCh:
				offset := len(state.partitions[partitionId]) - 1
				n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
			}
		} else {
			// using SyncRPC is the correct way, but in this challenge we assume a stable network
			n.RPC(leader, body, func(msg1 maelstrom.Message) error {
				var body map[string]any
				if err := json.Unmarshal(msg1.Body, &body); err != nil {
					return err
				}

				return n.Reply(msg, map[string]any{"type": "send_ok", "offset": body["offset"]})
			})
		}

		return nil
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
			leaderIndex := hash.ByteSlice([]byte(key)) % uint32(len(n.NodeIDs()))
			leader := n.NodeIDs()[leaderIndex]
			partition := []int{}

			if leader == n.ID() {
				partition = state.partitions[key]
			} else {
				kv.ReadInto(ctx, key, &partition)
			}

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
