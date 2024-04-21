package node

import (
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
)

type Node struct {
	*dragonboat.NodeHost
}

func NewNode(dataDir, raftAddr string) (*Node, error) {
	nodeHost, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		WALDir:         dataDir,
		NodeHostDir:    dataDir,
		RTTMillisecond: 200,
		RaftAddress:    raftAddr,
	})
	return &Node{
		NodeHost: nodeHost,
	}, err
}
