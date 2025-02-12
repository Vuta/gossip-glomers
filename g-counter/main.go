package main

import (
	"encoding/json"
	"log"
	"context"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		go func() {
			ctx := context.Background()
			for {
				value, _ := kv.ReadInt(ctx, "g-counter")
				err := kv.CompareAndSwap(ctx, "g-counter", int64(value), int64(body["delta"].(float64)) + int64(value), true)
				if err == nil {
					break
				}
			}
		}()

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx := context.Background()
		// Consider this
		// T1. n0 cas 0 to 1
		// T2. n1 cas 1 to 3
		// T3. n0 issued a read and got, not 3, but 1 (Why?)
		// Because in a sequential consistency memory model, the system doesn't say anything about multi-processes execution order 
		// It only guarantees that each process's (n0 & n1 in this example) operations are executed in program order
		// Which means, the system can deliberately decide to reorder the above example to
		// T1. n0 cas 0 to 1
		// T2. n0 issued a read and got 1
		// T3. n1 cas 1 to 3
		// This is a valid scenario because it doesn't violate the per-process order guarantee.
		// To tackle this scenario, here we intentionally force a random write before the read to create a "checkpoint" in the system state.
		// The example now looks like this
		// T1. n0 cas 0 to 1
		// T2. n1 cas 1 to 3
		// T3. n0 wrote randomly unique value
		// T4. n0 issued a read and got 3
		// This time, the system has no way to reorder the read. Thus any followed reads must have the latest already-completed value.
		kv.Write(ctx, n.ID(), time.Now().UnixMilli())

		value, _ := kv.ReadInt(ctx, "g-counter")

		return n.Reply(msg, map[string]any{"type": "read_ok", "value": int64(value)})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
