package workflow

import (
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
)

const (
	typeContinue = 0
	typeRatio    = 1
	typeDiscret  = 8
)

// // Flow is the directed graph of the workflow
// //
// // It is implemented with a memory array for the input, the intermediary result and the output.
// // The operators must be organise that the compute can be done in order, due to the directed graph representation.
// // The input and output Value Type must be correct. A check must be performe before the instantiation of the workflow
// type Flow struct {
// 	mem []Value    // memory like resgister or RAM
// 	ops []operator // operators like ALU or FPU
// }

// func (f *Flow) compute() {
// 	for _, op := range f.ops {
// 		op.fn(op.input, op.output)
// 	}
// }

// ComputeFunc do the actual computation
type ComputeFunc func(node *FlowNode)

// Operation to check type value and containe actual compute function
type Operation struct {
	Name string      `json:"name"`
	Fn   ComputeFunc `json:"-"`

	Description string `json:"description,omitempty"`
	MinLenInput int    `json:"min_len_input,omitempty"`
	MaxLenInput int    `json:"max_len_input,omitempty"` // -1 for infinity number of inputs
	OutputType  string `json:"output_type,omitempty"`
	InputsType  string `json:"inputs_type,omitempty"`
}

// Operations map all possible Operation by there name
var Operations = map[string]Operation{
	"const": {Name: "const", Fn: func(node *FlowNode) {}, MinLenInput: 0, MaxLenInput: 0},

	"input": {Name: "input", Fn: func(node *FlowNode) {}, MinLenInput: 0, MaxLenInput: 0},
	"output": {
		Name: "output",
		Fn: func(node *FlowNode) {
			node.changed = node.ComputedValue != *node.ComputedInputs[0]
			if node.changed {
				node.ComputedValue = *node.ComputedInputs[0]
			}
		},
		MinLenInput: 1,
		MaxLenInput: 1,
	},

	// Float64 < <= > >= == !=
	"lt": {
		Name: "lt",
		Fn: func(node *FlowNode) {
			node.ComputedValue = Value((*node.ComputedInputs[0]).(float64) < (*node.ComputedInputs[1]).(float64))
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"le": {
		Name: "le",
		Fn: func(node *FlowNode) {
			node.ComputedValue = Value((*node.ComputedInputs[0]).(float64) <= (*node.ComputedInputs[1]).(float64))
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"gt": {
		Name: "gt",
		Fn: func(node *FlowNode) {
			node.ComputedValue = Value((*node.ComputedInputs[0]).(float64) > (*node.ComputedInputs[1]).(float64))
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"ge": {
		Name: "ge",
		Fn: func(node *FlowNode) {
			node.ComputedValue = Value((*node.ComputedInputs[0]).(float64) >= (*node.ComputedInputs[1]).(float64))
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"eq": {
		Name: "eq",
		Fn: func(node *FlowNode) {
			a := (*node.ComputedInputs[0]).(float64)
			b := (*node.ComputedInputs[1]).(float64)
			node.ComputedValue = Value(math.Abs(a-b) < 1.0e-9)
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"ne": {
		Name: "ne",
		Fn: func(node *FlowNode) {
			a := (*node.ComputedInputs[0]).(float64)
			b := (*node.ComputedInputs[1]).(float64)
			node.ComputedValue = Value(math.Abs(a-b) > 1.0e-9)
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "float64",
		OutputType:  "float64",
	},

	// Logical && || !
	"and": {
		Name: "and",
		Fn: func(node *FlowNode) {
			for _, v := range node.ComputedInputs {
				if !(*v).(bool) {
					node.ComputedValue = Value(false)
					return
				}
			}
			node.ComputedValue = Value(true)
		},
		MinLenInput: 1,
		MaxLenInput: -1,
		InputsType:  "bool",
		OutputType:  "bool",
	},
	"or": {
		Name: "or",
		Fn: func(node *FlowNode) {
			for _, v := range node.ComputedInputs {
				if (*v).(bool) {
					node.ComputedValue = Value(true)
					return
				}
			}
			node.ComputedValue = Value(false)
		},
		MinLenInput: 1,
		MaxLenInput: -1,
		InputsType:  "bool",
		OutputType:  "bool",
	},
	"not": {
		Name: "not",
		Fn: func(node *FlowNode) {
			node.ComputedValue = Value(!(*node.ComputedInputs[0]).(bool))
		},
		MinLenInput: 1,
		MaxLenInput: 1,
		InputsType:  "bool",
		OutputType:  "bool",
	},

	// Arimthetics + / min max
	"add": {
		Name: "add",
		Fn: func(node *FlowNode) {
			acc := 0.0
			for _, v := range node.ComputedInputs {
				acc += (*v).(float64)
			}
			node.ComputedValue = Value(acc)
		},
		MinLenInput: 1,
		MaxLenInput: -1,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"div": {
		Name: "div",
		Fn: func(node *FlowNode) {
			a := (*node.ComputedInputs[0]).(float64)
			b := (*node.ComputedInputs[1]).(float64)
			node.ComputedValue = Value(a / b)
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"min": {
		Name: "min",
		Fn: func(node *FlowNode) {
			acc := math.MaxFloat64
			for _, v := range node.ComputedInputs {
				v := (*v).(float64)
				if v < acc {
					acc = v
				}
			}
			node.ComputedValue = Value(acc)
		},
		MinLenInput: 1,
		MaxLenInput: -1,
		InputsType:  "float64",
		OutputType:  "float64",
	},
	"max": {
		Name: "max",
		Fn: func(node *FlowNode) {
			acc := -math.MaxFloat64
			for _, v := range node.ComputedInputs {
				v := (*v).(float64)
				if v > acc {
					acc = v
				}
			}
			node.ComputedValue = Value(acc)
		},
		MinLenInput: 1,
		MaxLenInput: -1,
		InputsType:  "float64",
		OutputType:  "float64",
	},

	// Select
	"select": {
		Name: "select",
		Fn: func(node *FlowNode) {
			in := (*node.ComputedInputs[0]).(float64)
			for i, cond := range node.Condition {
				maxima := strings.Split(cond, ":")
				if len(maxima) != 2 {
					continue
				}
				min, err := strconv.ParseFloat(maxima[0], 64)
				if err != nil {
					panic(fmt.Sprintf("Condition min not a float: %s: %s", cond, err))
				}
				max, err := strconv.ParseFloat(maxima[1], 64)
				if err != nil {
					panic(fmt.Sprintf("Condition max not a float: %s: %s", cond, err))
				}
				// Found the range
				if min <= in && in <= max {
					node.ComputedValue = Value(node.Values[i])
					return
				}
			}

			panic("Unmatched conditions")
		},
		MinLenInput: 1,
		MaxLenInput: -1,
		InputsType:  "float",
		OutputType:  "enum",
	},

	// string operator
	"contains_exactly": {
		Name: "contains_exactly",
		Fn: func(node *FlowNode) {
			a := (*node.ComputedInputs[0]).(string)
			b := (*node.ComputedInputs[1]).(string)
			node.ComputedValue = Value(a == b)
		},
		MinLenInput: 2,
		MaxLenInput: 2,
		InputsType:  "string",
		OutputType:  "bool",
	},

	"match_str": {
		Name: "match_str",
		Fn: func(node *FlowNode) {
			if (*node.ComputedInputs[0]).(bool) {
				node.ComputedValue = *node.ComputedInputs[1]
			} else {
				node.ComputedValue = *node.ComputedInputs[2]
			}
		},
		MinLenInput: 3,
		MaxLenInput: 3,
		InputsType:  "bool/string/string",
		OutputType:  "bool",
	},

	// Send to a webhook
	"send": {
		Name: "send",
		Fn: func(node *FlowNode) {
		},
		MinLenInput: 0,
		MaxLenInput: 0,
	},
}

// Value of the data
type Value interface{}

// FlowGraph is the parsed representation of a workflow graph in json
type FlowGraph struct {
	ID   uuid.UUID
	AID  uuid.UUID `json:"account_id"`
	Name string
	Flow map[string]*FlowNode

	Inputs  map[string]map[string]*FlowNode // sensor_id.field_name
	Outputs []*FlowNode
	Nodes   []*FlowNode
	Hooks   map[string]string

	rev uint64
}

// FlowNode in the FlowGraph used to parsed json graph
type FlowNode struct {
	ID        string
	Name      string
	Type      string
	Operator  string
	Inputs    []string
	Values    []string
	Condition []string

	rev     uint64
	changed bool
	lastChanged time.Time

	ComputedValue  Value
	ComputedInputs []*Value
	Operation      Operation
}

// SendInput to the graph before Compute
func (g *FlowGraph) SendInput(aid, sid string, createdAt time.Time, data map[string]interface{}) (mustRecompute bool, err error) {
	if g.Inputs == nil {
		g.Build()
	}
	input, wanted := g.Inputs[sid]
	if !wanted {
		return false, nil
	}
	g.rev++
	for name, value := range data {
		if node, wanted := input[name]; wanted {
			// don't update if the received message is older than the last one we see
			if createdAt.Before(node.lastChanged) || createdAt == node.lastChanged {
				continue
			}
			switch node.Type {
			case "string":
				if _, ok := value.(string); !ok {
					return false, fmt.Errorf("in node '%s' input wanted string, got %T", name, value)
				}
			case "enum":
				if _, ok := value.(string); !ok {
					return false, fmt.Errorf("in node '%s' input wanted enum, got %T", name, value)
				}
			case "float":
				switch v := value.(type) {
				case string:
					value, err = strconv.ParseFloat(v, 64)
					if err != nil {
						return false, fmt.Errorf("in node '%s' input wanted float: %v", name, err)
					}
				case int:
					value = float64(v)
				case float64:

				default:
					return false, fmt.Errorf("in node '%s' input wanted string, got %T", name, value)
				}
			case "bool":
				switch v := value.(type) {
				case string:
					value, err = strconv.ParseBool(v)
					if err != nil {
						return false, fmt.Errorf("in node '%s' input wanted float: %v", name, err)
					}
				case bool:

				default:
					return false, fmt.Errorf("in node '%s' input wanted string, got %T", name, value)
				}
			}
			node.ComputedValue = value
			node.lastChanged = createdAt
			node.rev = g.rev
			mustRecompute = true
		}
	}
	return mustRecompute, nil
}

// WantInput return true if we need to use this message
func (g *FlowGraph) WantInput(id string) bool {
	if g.Inputs == nil {
		g.Build()
	}
	_, wanted := g.Inputs[id]
	return wanted
}

// Build internal helper graph from basic json graph
func (g *FlowGraph) Build() error {
	g.Inputs = make(map[string]map[string]*FlowNode)
	g.Hooks = make(map[string]string)
	for nodeKey, node := range g.Flow {
		if (node.Operator == "input" || node.Operator == "output" || node.Operator == "webhook") && node.ID == "" {
			return fmt.Errorf("missing id in node '%s'", nodeKey)
		}

		if node.Operator == "output" {
			g.Outputs = append(g.Outputs, node)
		}
		if node.Operator == "input" {
			input, exist := g.Inputs[node.ID]
			if !exist {
				input = make(map[string]*FlowNode)
			}
			input[node.Name] = node
			g.Inputs[node.ID] = input
			switch strings.ToLower(node.Type) {
			case "enume":
				fallthrough
			case "string":
				node.ComputedValue = ""
			case "bool":
				node.ComputedValue = false
			case "float":
				node.ComputedValue = 0.0
			default:
				node.ComputedValue = ""
			}
			continue
		}
		if node.Operator == "send" {
			if len(node.Values) == 0 {
				return fmt.Errorf("send operator '%s' take 1 value an URL", nodeKey)
			}
			u, err := url.Parse(node.Values[0])
			if err != nil {
				return fmt.Errorf("error in '%s' url: %v", nodeKey, err)
			}
			_, err = uuid.FromString(node.ID)
			if err != nil {
				return fmt.Errorf("error in '%s' sensor id: %v", nodeKey, err)
			}
			g.Hooks[node.ID] = u.String()
		}
	}
	for nodeKey, node := range g.Flow {
		node.ComputedInputs = make([]*Value, 0, len(node.Inputs))
		for _, key := range node.Inputs {
			input, exist := g.Flow[key]
			if !exist {
				return fmt.Errorf("Unkown input %s in %s", key, nodeKey)
			}
			node.ComputedInputs = append(node.ComputedInputs, &input.ComputedValue)
		}

		// Assign Operation
		op, exist := Operations[node.Operator]
		if !exist {
			return fmt.Errorf("In node '%s': '%v' is not a valid operator", nodeKey, node.Operator)
		}
		node.Operation = op

		// check the number of inputs with the operator min/max
		if len(node.Inputs) < op.MinLenInput || (op.MaxLenInput >= 0 && len(node.Inputs) > op.MaxLenInput) {
			return fmt.Errorf("in '%s' operator %s, expected number of inputs between %d and %d, got %d", nodeKey, op.Name, op.MinLenInput, op.MaxLenInput, len(node.Inputs))
		}
	}

	// Order the Nodes DAG to compute the Operation of each node sequentialy
	g.Nodes = make([]*FlowNode, 0, len(g.Flow))
	for len(g.Nodes) < len(g.Flow) {
		for _, node := range g.Flow {
			// Use rev to indicate if the node is already placed in the Nodes list
			if node.rev != 0 {
				continue
			}
			childInNodes := true
			for _, key := range node.Inputs {
				input, _ := g.Flow[key]
				if input.rev == 0 {
					childInNodes = false
					break
				}
			}
			if childInNodes {
				node.rev = 1
				g.Nodes = append(g.Nodes, node)
			}
		}
	}

	return nil
}

// Compute don't check anything, the graph MUST be check before called
func (g *FlowGraph) Compute() {
	for _, node := range g.Nodes {
		node.Operation.Fn(node)
	}
}

// Output after the Compute to get the result
func (g *FlowGraph) Output() []*FlowNode {
	return g.Outputs
}

// WriteDotFormat to visualize the graphflow
func (g *FlowGraph) WriteDotFormat(w io.Writer) (n int, err error) {
	fmt.Fprintf(w, "digraph \"%v %s\" {\n", g.ID, g.Name)
	fmt.Fprint(w, " {\n")
	for k, node := range g.Flow {
		fmt.Fprintf(w, "    \"%s\" [", k)
		label := node.Name
		if label == "" {
			label = node.Operator
		}

		switch node.Operator {
		case "input":
			fmt.Fprintf(w, `label="%s\n%s\n%s\n%s\n%v\n%v"`, k, node.ID, node.Name, node.Operator, node.lastChanged, node.ComputedValue)
			fmt.Fprintf(w, " shape=box")
		case "output":
			fmt.Fprintf(w, `label="%s\n%s\n%s\n%s\n%v"`, k, node.ID, node.Name, node.Operator, node.ComputedValue)
			fmt.Fprintf(w, " shape=box")
		default:
			fmt.Fprintf(w, `label="%s\n%s\n%v"`, k, node.Operator, node.ComputedValue)
		}
		fmt.Fprint(w, "]\n")
	}
	fmt.Fprint(w, " };\n")
	for to, node := range g.Flow {
		for _, from := range node.Inputs {
			fmt.Fprintf(w, "  \"%s\" -> \"%s\";\n", from, to)
		}
	}
	return fmt.Fprint(w, "}")
}

// HasChanged if the ouput has changed with the last compute
func (n *FlowNode) HasChanged() bool { return n.changed }
