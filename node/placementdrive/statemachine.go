package placementdrive

import (
	"encoding/json"
	"io"
	"math/rand"
	"sync"

	"github.com/lni/dragonboat/v4/statemachine"
)

type StateMachine struct {
	mu sync.Mutex

	// 本机编号
	nodeID uint64

	// Region信息
	currentID uint64
	regions   map[uint64]*RegionInfo
	// 节点信息
	nodes map[uint64]*NodeConfig
}

func NewStateMachine(regionID, nodeID uint64) statemachine.IStateMachine {
	return &StateMachine{
		nodeID:    nodeID,
		currentID: 1,

		regions: make(map[uint64]*RegionInfo),
		nodes:   make(map[uint64]*NodeConfig),
	}
}

func (sm *StateMachine) Lookup(query interface{}) (interface{}, error) {
	cmd := query.(Command)

	switch cmd.Op {
	case OpGetFreeNodes:
		return sm.GetFreeNodes(cmd.Count)
	case OpGetAllRegions:
		sm.mu.Lock()
		defer sm.mu.Unlock()
		return sm.regions, nil
	case OpLocateRegion:
		sm.mu.Lock()
		defer sm.mu.Unlock()

		for rid, region := range sm.regions {
			if region.Start == "" {
				if region.End == "" || cmd.RegionArgs.Key < region.End {
					return rid, nil
				}
			} else if region.End == "" {
				if region.Start <= cmd.RegionArgs.Key {
					return rid, nil
				}
			} else if region.Start <= cmd.RegionArgs.Key && cmd.RegionArgs.Key < region.End {
				return rid, nil
			}
		}
		return 0, ErrorNoRegion
	}
	return nil, nil
}

func (sm *StateMachine) Update(e statemachine.Entry) (statemachine.Result, error) {
	var cmd Command
	if err := json.Unmarshal(e.Cmd, &cmd); err != nil {
		return statemachine.Result{}, err
	}

	switch cmd.Op {
	case OpGetNewRegionID:
		sm.mu.Lock()
		defer sm.mu.Unlock()

		sm.currentID++
		sm.regions[sm.currentID] = &RegionInfo{
			CurrentID: 4,
		}
		return statemachine.Result{
			Value: sm.currentID,
		}, nil

	case OpGetNewNodeIDInRegion:
		sm.mu.Lock()
		defer sm.mu.Unlock()

		args := cmd.RegionArgs
		if _, ok := sm.regions[args.RegionID]; !ok {
			return statemachine.Result{}, ErrorNoRegion
		}
		sm.regions[args.RegionID].CurrentID++
		return statemachine.Result{
			Value: sm.regions[args.RegionID].CurrentID,
		}, nil

	case OpSetNodeConfig:
		sm.mu.Lock()
		defer sm.mu.Unlock()

		args := cmd.NodeConfigArgs
		sm.nodes[args.NodeID] = args.Config

		return statemachine.Result{}, nil

	case OpUpdateNodesMap:
		sm.mu.Lock()
		defer sm.mu.Unlock()

		sm.regions[cmd.RegionArgs.RegionID].NodeMap[cmd.NodeConfigArgs.Config.RaftAddr] = cmd.NodeConfigArgs.NodeID

		return statemachine.Result{}, nil
	}

	return statemachine.Result{}, nil
}

func (sm *StateMachine) SaveSnapshot(w io.Writer, sfc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	// enc := json.NewEncoder(w)

	return nil
}
func (sm *StateMachine) RecoverFromSnapshot(r io.Reader, sf []statemachine.SnapshotFile, done <-chan struct{}) error {
	// dec := json.NewDecoder(r)

	return nil
}

func (sm *StateMachine) Close() error {
	return nil
}

func (sm *StateMachine) GetFreeNodes(count int) ([]NodeConfig, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	allNodes := []*NodeConfig{}
	for _, config := range sm.nodes {
		allNodes = append(allNodes, config)
	}

	nodes := []NodeConfig{}
	for ; count > 0; count-- {
		nodes = append(nodes, *allNodes[rand.Intn(len(allNodes))])
	}
	return nodes, nil
}
