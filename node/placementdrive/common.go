package placementdrive

import "errors"

// 节点信息
type NodeConfig struct {
	RaftAddr string `json:"raft_address"`
	HttpAddr string `json:"http_address"`

	RegionCount uint64
}

// Region信息
type RegionInfo struct {
	CurrentID uint64 `json:"current_id"`
	Start     string `json:"start"`
	End       string `json:"end"`

	NodeMap map[string]uint64 `json:"nodes_map"`
}

type Operation uint64

type Command struct {
	Op Operation `json:"op"`

	Count int `json:"count"`

	RegionArgs     RegionArgs     `json:"region_args"`
	NodeConfigArgs NodeConfigArgs `json:"node_config_args"`
}

const (
	// NodeConfig 操作
	OpSetNodeConfig Operation = iota
	OpGetFreeNodes
	// Region操作
	OpGetNewRegionID
	OpGetNewNodeIDInRegion
	OpGetAllRegions
	OpUpdateNodesMap
	OpLocateRegion
	OpGetValueByRidAndKey
)

var (
	ErrorNoRegion = errors.New("ErrorNoRegion")
)

type RegionArgs struct {
	RegionID uint64 `json:"region_id"`
	Key      string `json:"key"`
}

type NodeConfigArgs struct {
	NodeID uint64      `json:"node_id"`
	Config *NodeConfig `json:"node_config"`
}
