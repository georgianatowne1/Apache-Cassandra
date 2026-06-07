package main

import (
	"fmt"
	"sync"
	"time"
)

// ConsistencyLevel represents the consistency level of the query
type ConsistencyLevel string

const (
	LocalQuorum ConsistencyLevel = "LOCAL_QUORUM"
)

// NodeStatus represents the status of a node in the cluster
type NodeStatus string

const (
	Down       NodeStatus = "DOWN"
	Recovering NodeStatus = "RECOVERING"
	Up         NodeStatus = "UP"
)

// Row represents a data row with a value and a timestamp (for last-write-wins)
type Row struct {
	Value     string
	Timestamp int64
}

// Node represents a Cassandra node
type Node struct {
	ID           string
	Status       NodeStatus
	Data         map[string]Row
	Mu           sync.RWMutex
	RecoveryTime time.Time
}

// Hint represents a pending write for a down node
type Hint struct {
	Key        string
	Row        Row
	TargetNode string
}

// Cluster represents the Cassandra cluster
type Cluster struct {
	Nodes        map[string]*Node
	Hints        map[string][]Hint // TargetNode -> Hints
	HintsMu      sync.Mutex
	WarmupPeriod time.Duration
}

func NewCluster() *Cluster {
	return &Cluster{
		Nodes: map[string]*Node{
			"A": {ID: "A", Status: Up, Data: make(map[string]Row)},
			"B": {ID: "B", Status: Up, Data: make(map[string]Row)},
			"C": {ID: "C", Status: Up, Data: make(map[string]Row)},
		},
		Hints:        make(map[string][]Hint),
		WarmupPeriod: 2 * time.Second,
	}
}

// Write writes a key-value pair to the cluster
func (c *Cluster) Write(key string, value string, cl ConsistencyLevel) bool {
	timestamp := time.Now().UnixNano()
	row := Row{Value: value, Timestamp: timestamp}

	targets := []string{"A", "B", "C"}
	successes := 0

	for _, target := range targets {
		node := c.Nodes[target]
		node.Mu.Lock()
		if node.Status == Up {
			node.Data[key] = row
			successes++
		} else {
			c.HintsMu.Lock()
			c.Hints[target] = append(c.Hints[target], Hint{Key: key, Row: row, TargetNode: target})
			c.HintsMu.Unlock()
		}
		node.Mu.Unlock()
	}

	required := 2
	return successes >= required
}

// Read reads a key from the cluster
func (c *Cluster) Read(key string, cl ConsistencyLevel) (string, bool) {
	targets := []string{"A", "B", "C"}
	var responses []Row

	sortedTargets := c.getSortedTargets(targets)

	for _, target := range sortedTargets {
		node := c.Nodes[target]
		node.Mu.RLock()
		if node.Status == Up {
			if row, exists := node.Data[key]; exists {
				responses = append(responses, row)
			} else {
				responses = append(responses, Row{Value: "", Timestamp: 0})
			}
		}
		node.Mu.RUnlock()

		if len(responses) >= 2 {
			break
		}
	}

	if len(responses) < 2 {
		return "", false
	}

	newest := responses[0]
	for _, resp := range responses[1:] {
		if resp.Timestamp > newest.Timestamp {
			newest = resp
		}
	}

	return newest.Value, true
}

// getSortedTargets implements DynamicEndpointSnitch with Warmup Penalty
func (c *Cluster) getSortedTargets(targets []string) []string {
	var preferred []string
	var warmedUp []string
	var warmingUp []string
	var down []string

	for _, target := range targets {
		node := c.Nodes[target]
		node.Mu.RLock()
		status := node.Status
		recTime := node.RecoveryTime
		node.Mu.RUnlock()

		if status == Up {
			if time.Since(recTime) < c.WarmupPeriod {
				warmingUp = append(warmingUp, target)
			} else {
				warmedUp = append(warmedUp, target)
			}
		} else {
			down = append(down, target)
		}
	}

	preferred = append(preferred, warmedUp...)
	preferred = append(preferred, warmingUp...)
	preferred = append(preferred, down...)
	return preferred
}

// RecoverNode simulates node recovery with Gossip State Delay and Hint Replay
func (c *Cluster) RecoverNode(nodeID string) {
	node := c.Nodes[nodeID]

	node.Mu.Lock()
	node.Status = Recovering
	node.Mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	c.HintsMu.Lock()
	hints := c.Hints[nodeID]
	delete(c.Hints, nodeID)
	c.HintsMu.Unlock()

	node.Mu.Lock()
	for _, hint := range hints {
		if existing, exists := node.Data[hint.Key]; !exists || hint.Row.Timestamp > existing.Timestamp {
			node.Data[hint.Key] = hint.Row
		}
	}
	node.RecoveryTime = time.Now()
	node.Status = Up
	node.Mu.Unlock()
}

func main() {
	fmt.Println("Starting Cassandra Node Recovery Simulation...")

	cluster := NewCluster()

	fmt.Println("Shutting down Node C...")
	cluster.Nodes["C"].Mu.Lock()
	cluster.Nodes["C"].Status = Down
	cluster.Nodes["C"].Mu.Unlock()

	fmt.Println("Writing 1,000 keys (v1) to active nodes...")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		cluster.Write(key, "value-v1", LocalQuorum)
	}

	fmt.Println("Updating 1,000 keys (v2) on active nodes...")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		cluster.Write(key, "value-v2", LocalQuorum)
	}

	fmt.Println("Recovering Node C (replaying hints and warming up)...")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		cluster.RecoverNode("C")
		wg.Done()
	}()

	fmt.Println("Executing concurrent reads at LOCAL_QUORUM during recovery...")
	staleReads := 0
	failedReads := 0
	var readWg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		readWg.Add(1)
		go func(id int) {
			defer readWg.Done()
			key := fmt.Sprintf("key-%d", id)
			val, ok := cluster.Read(key, LocalQuorum)
			if !ok {
				failedReads++
			} else if val != "value-v2" {
				staleReads++
			}
		}(i)
	}

	readWg.Wait()
	wg.Wait()

	fmt.Printf("Simulation Finished.\n")
	fmt.Printf("Failed Reads: %d\n", failedReads)
	fmt.Printf("Stale Reads: %d\n", staleReads)

	if staleReads == 0 && failedReads == 0 {
		fmt.Println("SUCCESS: Zero stale reads under QUORUM achieved!")
	} else {
		fmt.Println("FAILURE: Stale or failed reads detected.")
	}
}