package workflow

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var _rawgraph = `
{
	"name": "database server load",
	"id": "3e8c8cc8-7567-4594-a8d0-c38d9f64765e",
	"account_id": "fe8927e9-a02a-416a-8928-c3a86dae4c61",
	"flow": {
		"input0": {
			"operator": "input",
			"type": "float",
			"name": "ramusage",
			"description": "RAM usage of the server",
			"id": "1377959e-97ce-46c1-9715-22c34bb9afbe"
		},
		"input1": {
			"operator": "input",
			"type": "float",
			"name": "loadaverage",
			"id": "1377959e-97ce-46c1-9715-22c34bb9afbe"
		},
		"input2": {
			"operator": "input",
			"type": "float",
			"name": "ram",
			"id": "1377959e-97ce-46c1-9715-22c34bb9afbe"
		},

		"const0": {
			"operator": "const",
			"type": "float",
			"ComputedValue": 0.8
		},

		"op_ram": {
			"operator": "div",
			"inputs": ["input0","input2"]
		},

		"op0": {
			"operator": "gt",
			"inputs": ["op_ram","const0"]
		},
		"op1": {
			"operator": "gt",
			"inputs": ["input1","const0"]
		},
		"op3": {
			"operator": "or",
			"inputs": ["op0","op1"]
		},

		"op4": {
			"operator": "max",
			"inputs": ["op_ram","input1"]
		},
		"op5": {
			"operator": "select",
			"inputs": ["op4"],
			"values": ["stop", "slow", "medium", "fast"],
			"condition": ["0:.1",".1:.3",".3:.6",".6:1"]
		},

		"output0": {
			"operator": "output",
			"name": "windmill_onfire",
			"type": "bool",
			"inputs": ["op3"],
			"id": "1949f63d-5e40-45bb-9d31-13ab52b5e92a"
		},
		"output1": {
			"operator": "output",
			"name": "windmill_propeler_speed",
			"type": "enum",
			"values": ["stop", "slow", "medium", "fast"],
			"inputs": ["op5"],
			"id": "1949f63d-5e40-45bb-9d31-13ab52b5e92a"
		},

		"webhook": {
			"operator": "send",
			"values": ["http://localhost:2030"],
			"id": "1949f63d-5e40-45bb-9d31-13ab52b5e92a"
		}
	}
}`

func TestFormat(t *testing.T) {
	var graph FlowGraph
	err := json.Unmarshal([]byte(_rawgraph), &graph)
	if err != nil {
		t.Error(err)
	}

	err = graph.Build()
	if err != nil {
		t.Error(err)
	}

	_, err = graph.SendInput("fe8927e9-a02a-416a-8928-c3a86dae4c61", "1377959e-97ce-46c1-9715-22c34bb9afbe", time.Unix(1, 0), map[string]interface{}{
		"ramusage":    1.0,
		"loadaverage": 0.2,
		"ram":         4.0,
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	// graph.WriteDotFormat(os.Stdout)
	graph.Compute()

	// j, err := json.MarshalIndent(graph, "", "  ")
	// fmt.Printf("%v\n%s\n", err, j)

	// outs := graph.Output()
	// for _, out := range outs {
	// 	fmt.Printf("%v\n", out)
	// }
	// for _, node := range graph.Nodes {
	// 	fmt.Printf("%- 8v %- 5v % 8v % 8d % 6v\n", node.Operator, node.Type, node.ComputedValue, node.rev, node.changed)
	// }
	_, err = graph.SendInput("fe8927e9-a02a-416a-8928-c3a86dae4c61", "1377959e-97ce-46c1-9715-22c34bb9afbe", time.Unix(2, 0), map[string]interface{}{
		"ramusage":    "1.0",
		"loadaverage": 1.0,
		"ram":         4.0,
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	graph.Compute()

	// fmt.Println()
	// for _, node := range graph.Nodes {
	// 	fmt.Printf("%- 8v %- 5v % 8v % 8d % 6v\n", node.Operator, node.Type, node.ComputedValue, node.rev, node.changed)
	// }

	_, err = graph.SendInput("fe8927e9-a02a-416a-8928-c3a86dae4c61", "1377959e-97ce-46c1-9715-22c34bb9afbe", time.Unix(3, 0), map[string]interface{}{
		"ramusage":    1.0,
		"loadaverage": 1.0,
		"ram":         4.0,
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	graph.Compute()

	recompute, err := graph.SendInput("fe8927e9-a02a-416a-8928-c3a86dae4c61", "1377959e-97ce-46c1-9715-22c34bb9afbe", time.Unix(2, 0), map[string]interface{}{
		"ramusage":    3.0,
		"loadaverage": 0.0,
		"ram":         4.0,
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	if recompute {
		t.Error("must not recompute with older message")
	}
	recompute, err = graph.SendInput("fe8927e9-a02a-416a-8928-c3a86dae4c61", "1377959e-97ce-46c1-9715-22c34bb9afbe", time.Unix(3, 0), map[string]interface{}{
		"ramusage":    3.0,
		"loadaverage": 0.0,
		"ram":         4.0,
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	if recompute {
		t.Error("must not recompute with same message")
	}

	fmt.Println()
	for _, node := range graph.Nodes {
		fmt.Printf("%- 8v %- 5v % 8v % 8d % 6v % 10v\n", node.Operator, node.Type, node.ComputedValue, node.rev, node.changed, node.lastChanged)
	}

	// t.Fail()
}

func TestMissingID(t *testing.T) {
	raw := `{
	"name": "database server load",
	"id": "3e8c8cc8-7567-4594-a8d0-c38d9f64765e",
	"account_id": "fe8927e9-a02a-416a-8928-c3a86dae4c61",
	"flow": {
		"in": {
			"operator": "input",
			"type": "string",
			"name": "status"
		},
		"out": {
			"operator": "output",
			"type": "string"
		}
	}
}`

	var graph FlowGraph
	err := json.Unmarshal([]byte(raw), &graph)
	if err != nil {
		t.Error(err)
	}

	err = graph.Build()
	if err == nil {
		t.Error("missing id is not catch")
	}
}

func TestInputsLen(t *testing.T) {
	raw := `{
	"name": "database server load",
	"id": "3e8c8cc8-7567-4594-a8d0-c38d9f64765e",
	"account_id": "fe8927e9-a02a-416a-8928-c3a86dae4c61",
	"flow": {
		"in": {
			"operator": "input",
			"type": "string",
			"id": "001"
		},
		"out": {
			"operator": "output",
			"type": "string",
			"id": "002"
		}
	}
}`

	var graph FlowGraph
	err := json.Unmarshal([]byte(raw), &graph)
	if err != nil {
		t.Error(err)
	}

	err = graph.Build()
	if err == nil {
		t.Error("wrong number of Inputs is not catch")
	}
}

func BenchmarkSameInput(b *testing.B) {
	var graph FlowGraph
	err := json.Unmarshal([]byte(_rawgraph), &graph)
	if err != nil {
		b.Error(err)
	}

	err = graph.Build()
	if err != nil {
		b.Error(err)
	}

	rams := []float64{0.01, 0.2, 0.4, 0.6, 0.8, 0.9, 1.0, 1.6, 2.0, 2.5, 3.0, 3.3, 3.5, 3.8, 4.0}
	lavg := []float64{0.01, 0.2, 0.4, 0.6, 0.8, 0.9, 1.0}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = graph.SendInput("fe8927e9-a02a-416a-8928-c3a86dae4c61", "1377959e-97ce-46c1-9715-22c34bb9afbe", time.Unix(int64(i), 0), map[string]interface{}{
			"ramusage":    rams[i%len(rams)],
			"loadaverage": lavg[i%len(lavg)],
			"ram":         rams[len(rams)-1],
		})
		if err != nil {
			b.Errorf("%v", err)
		}

		graph.Compute()
		graph.Output()
	}
}

func BenchmarkBuild(b *testing.B) {

	// b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var graph FlowGraph
		err := json.Unmarshal([]byte(_rawgraph), &graph)
		if err != nil {
			b.Error(err)
		}

		err = graph.Build()
		if err != nil {
			b.Error(err)
		}
	}
}
