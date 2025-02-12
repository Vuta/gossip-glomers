package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	// Custom epoch: time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	Epoch = 1735725600000
	NodeBits = 10
	SequenceBits = 12
	MaxSequence = 4096
)

type State struct {
	nodeId   int64
	sequence int64
	last     int64
}

func main() {
	n := maelstrom.NewNode()
	state := &State{}

	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		nodeID, err := buildNodeId(n.ID())
		if err != nil {
			return err
		}

		state.nodeId = nodeID

		body["type"] = "init_ok"
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := generateId(state)
		body["id"] = strconv.FormatInt(id, 10)

		body["type"] = "generate_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// mimic Twitter Snowflake ID
// first 41 bits: timestamp
// next 10 bits: node ID
// last 12 bits: sequence number
// 0 | 00000000000000000000000000000000000000000 | 00 0000 0000 | 0000 0000 0000
func generateId(state *State) int64 {
	var id int64

	currentTime := getTimestamp()

	if currentTime < state.last {
		panic("Clock moved backward. Abort!")
	}

	if currentTime == state.last {
		state.sequence = (state.sequence + 1) % MaxSequence

		if state.sequence == 0 {
			for currentTime == getTimestamp() {
			}
		}
	} else {
		state.sequence = 0
	}

	state.last = getTimestamp()
	id |= (state.last << (NodeBits + SequenceBits)) | (state.nodeId << SequenceBits) | state.sequence

	return id
}

func getTimestamp() int64 {
	return time.Now().UTC().UnixMilli() - Epoch
}

func buildNodeId(nodeID string) (int64, error) {
	var id int64

	if _, err := fmt.Sscanf(nodeID, "n%d", &id); err != nil {
		return id, fmt.Errorf("invalid node ID %s", nodeID)
	}

	return id, nil
}
