package main

import (
	"flag"
	"fmt"
	"gdutkv/node"
	"gdutkv/node/placementdrive"
	"log"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

var (
	raft_addresses = []string{
		"localhost:12001",
		"localhost:12002",
		"localhost:12003",
	}
	http_addresses = []string{
		"localhost:11001",
		"localhost:11002",
		"localhost:11003",
	}
)

func main() {
	var nid uint64
	flag.Uint64Var(&nid, "id", 1, "node id")
	flag.Parse()

	r := gin.Default()
	r.Use(cors.Default())

	node, err := node.NewNode(fmt.Sprintf("test/node%d", nid), raft_addresses[nid-1])
	if err != nil {
		log.Fatalf("[Err] Create Node %d Fail, Error %v\n", nid, err.Error())
	}

	pd := placementdrive.New(node, nid, http_addresses[nid-1])
	pd.Start(nid == 1)

	// 不会自动启动集群节点,需要自己来
	pdc := placementdrive.NewPDController(pd, r)
	pdc.Start()

	// node.HasNodeInfo()

	// info := node.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
	// 	SkipLogInfo: true,
	// })
	// fmt.Printf("regions: %v", info.ShardInfoList)

	r.Run(http_addresses[nid-1])
}
