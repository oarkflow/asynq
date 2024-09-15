package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// TaskFunc defines the function signature for processing a task
type TaskFunc func(context.Context) (context.Context, error)

// Node represents a task in the DAG
type Node struct {
	Name     string
	Task     TaskFunc
	Children []*Node // Dependencies of the task
	Parents  int     // Number of dependencies (for topological sort)
}

// DAG represents the Directed Acyclic Graph
type DAG struct {
	Nodes map[string]*Node // All nodes (tasks) in the graph
	Edges int              // Number of edges in the graph
}

// NewDAG initializes an empty DAG
func NewDAG() *DAG {
	return &DAG{
		Nodes: make(map[string]*Node),
	}
}

// AddNode adds a new node (task) to the DAG
func (dag *DAG) AddNode(name string, task TaskFunc) {
	if _, exists := dag.Nodes[name]; exists {
		fmt.Printf("Node %s already exists.\n", name)
		return
	}
	dag.Nodes[name] = &Node{
		Name:     name,
		Task:     task,
		Children: []*Node{},
		Parents:  0,
	}
}

// AddEdge adds a directed edge between two nodes (tasks) indicating that 'from' task must run before 'to' task
func (dag *DAG) AddEdge(from, to string) error {
	fromNode, fromExists := dag.Nodes[from]
	toNode, toExists := dag.Nodes[to]

	if !fromExists || !toExists {
		return errors.New("either from or to node does not exist")
	}

	// Add dependency: 'to' node depends on 'from' node
	fromNode.Children = append(fromNode.Children, toNode)
	toNode.Parents++
	dag.Edges++ // Increment the number of edges in the graph
	return nil
}

// Validate checks if the DAG is valid (has at least one edge)
func (dag *DAG) Validate() error {
	if dag.Edges == 0 {
		return errors.New("DAG must have at least one edge")
	}
	return nil
}

// FlowManager manages the execution of the DAG
type FlowManager struct {
	DAG *DAG
}

// NewFlowManager creates a new flow manager
func NewFlowManager(dag *DAG) *FlowManager {
	return &FlowManager{
		DAG: dag,
	}
}

// TopologicalSort returns the nodes in a valid topological order (tasks sorted by dependencies)
func (fm *FlowManager) TopologicalSort() ([]*Node, error) {
	var result []*Node
	queue := []*Node{}

	// Add all nodes with no dependencies (parents == 0) to the queue
	for _, node := range fm.DAG.Nodes {
		if node.Parents == 0 {
			queue = append(queue, node)
		}
	}

	// Process the queue
	for len(queue) > 0 {
		// Get the first node in the queue
		current := queue[0]
		queue = queue[1:]

		// Add the node to the result (topologically sorted order)
		result = append(result, current)

		// Process its children (nodes dependent on this node)
		for _, child := range current.Children {
			child.Parents--
			if child.Parents == 0 {
				queue = append(queue, child)
			}
		}
	}

	// Check if we have a valid topological sort
	if len(result) != len(fm.DAG.Nodes) {
		return nil, errors.New("cycle detected in the graph")
	}

	return result, nil
}

// Execute runs all tasks in the correct topological order
func (fm *FlowManager) Execute(ctx context.Context) error {
	// Validate the DAG
	if err := fm.DAG.Validate(); err != nil {
		return err
	}

	// Get the topological sort order of the nodes
	order, err := fm.TopologicalSort()
	if err != nil {
		return err
	}

	// Task status tracking
	taskResults := make(map[string]context.Context)
	taskErrors := make(map[string]error)
	mu := sync.Mutex{}
	var wg sync.WaitGroup

	// Process each node in topological order
	for _, node := range order {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			fmt.Printf("Executing task: %s\n", n.Name)

			// Execute the task
			newCtx, err := n.Task(ctx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				taskErrors[n.Name] = err
				return
			}
			taskResults[n.Name] = newCtx
		}(node)
	}

	wg.Wait()

	// Check if any task failed
	if len(taskErrors) > 0 {
		for task, err := range taskErrors {
			fmt.Printf("Task %s failed: %v\n", task, err)
		}
		return errors.New("one or more tasks failed")
	}

	fmt.Println("All tasks completed successfully.")
	return nil
}

// Example task functions
func TaskA(ctx context.Context) (context.Context, error) {
	fmt.Println("Task A processing...")
	return context.WithValue(ctx, "taskA", "done"), nil
}

func TaskB(ctx context.Context) (context.Context, error) {
	fmt.Println("Task B processing...")
	return context.WithValue(ctx, "taskB", "done"), nil
}

func TaskC(ctx context.Context) (context.Context, error) {
	fmt.Println("Task C processing...")
	return context.WithValue(ctx, "taskC", "done"), nil
}

func TaskD(ctx context.Context) (context.Context, error) {
	fmt.Println("Task D processing...")
	return context.WithValue(ctx, "taskD", "done"), nil
}

func main() {
	// Initialize the DAG
	dag := NewDAG()

	// Add nodes (tasks) to the DAG
	dag.AddNode("TaskA", TaskA)
	dag.AddNode("TaskB", TaskB)
	dag.AddNode("TaskC", TaskC)
	dag.AddNode("TaskD", TaskD)

	// Add dependencies (edges) between tasks
	if err := dag.AddEdge("TaskA", "TaskB"); err != nil {
		fmt.Println("Error adding edge:", err)
	}
	if err := dag.AddEdge("TaskA", "TaskC"); err != nil {
		fmt.Println("Error adding edge:", err)
	}
	if err := dag.AddEdge("TaskB", "TaskD"); err != nil {
		fmt.Println("Error adding edge:", err)
	}
	if err := dag.AddEdge("TaskA", "TaskD"); err != nil {
		fmt.Println("Error adding edge:", err)
	}

	// Create and execute the flow manager
	fm := NewFlowManager(dag)
	ctx := context.Background()
	if err := fm.Execute(ctx); err != nil {
		fmt.Printf("Error executing DAG: %v\n", err)
	}
}
