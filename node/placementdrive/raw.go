package placementdrive

import (
	"context"
	"encoding/json"
	"gdutkv/node"
	"gdutkv/node/kv"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
)

// 调度器
type PlacementDriver struct {
	// 机器实例
	me *node.Node
	// 本机编号
	nodeID uint64
	// 集群session
	session *client.Session
	// http接口地址
	httpAddr string
}

const (
	MemberChangeTimeOut    = 3 * time.Second
	RegionOperationTimeOut = 3 * time.Second
)

func New(node *node.Node, nodeID uint64, httpAddr string) *PlacementDriver {
	return &PlacementDriver{
		me:       node,
		nodeID:   nodeID,
		session:  nil,
		httpAddr: httpAddr,
	}
}

func (pd *PlacementDriver) StartRegion(nid, rid uint64, initMembers map[uint64]string) error {
	config := config.Config{
		ReplicaID:          nid,
		ShardID:            rid,
		CheckQuorum:        true,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		SnapshotEntries:    512,
		CompactionOverhead: 1024,
	}

	return pd.me.StartReplica(initMembers, initMembers == nil, kv.NewMemoryKVStateMachine, config)
}

// 启动PlacementDriver服务
func (pd *PlacementDriver) Start(isLeader bool) error {
	// 默认配置文件
	config := config.Config{
		ReplicaID:          pd.nodeID,
		ShardID:            1,
		CheckQuorum:        true,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		SnapshotEntries:    512,
		CompactionOverhead: 1024,
	}

	var err error
	if isLeader {
		// 需要先将自己放入初始成员组里,然后以非加入方式启动
		err = pd.me.StartReplica(map[uint64]string{
			pd.nodeID: pd.me.RaftAddress(),
		}, false, NewStateMachine, config)
	} else {
		err = pd.me.StartReplica(nil, true, NewStateMachine, config)
	}

	if err != nil {
		return err
	}

	// 获取集群操作session
	pd.session = pd.me.GetNoOPSession(1)

	// 启动定时任务写入配置信息
	go func() {
		for {
			time.Sleep(3 * time.Second)
			pd.writeNodeConfig()
		}
	}()

	// 重启所有备份的Region

	return nil
}

func (pd *PlacementDriver) RestartAllRegion() error {
	ctx, cancel := context.WithTimeout(context.Background(), RegionOperationTimeOut)
	res, err := pd.me.SyncRead(ctx, 1, Command{
		Op: OpGetAllRegions,
	})
	cancel()

	if err != nil {
		return err
	}

	regions := res.(map[uint64]*RegionInfo)
	for regionID, region := range regions {
		pd.StartRegion(region.NodeMap[pd.me.RaftAddress()], regionID, nil)
	}

	return nil
}

func (pd *PlacementDriver) writeNodeConfig() {
	ctx, cancel := context.WithTimeout(context.Background(), RegionOperationTimeOut)
	defer cancel()

	cmd, _ := json.Marshal(Command{
		Op: OpSetNodeConfig,
		NodeConfigArgs: NodeConfigArgs{
			NodeID: pd.nodeID,
			Config: &NodeConfig{
				RaftAddr:    pd.me.RaftAddress(),
				HttpAddr:    pd.httpAddr,
				RegionCount: 1,
			},
		},
	})

	log.Println("Start Writing NodeConfig")
	_, err := pd.me.SyncPropose(ctx, pd.session, cmd)
	if err != nil {
		log.Println(err.Error())
	}
}

// 新增一个PlacementDriver到集群中
func (pd *PlacementDriver) Join(remoteID uint64, remoteAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), MemberChangeTimeOut)
	defer cancel()

	return pd.me.SyncRequestAddReplica(ctx, 1, remoteID, remoteAddr, 0)
}

// 移除一个PlacementDriver
func (pd *PlacementDriver) Leave(remoteID uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), MemberChangeTimeOut)
	defer cancel()

	return pd.me.SyncRequestDeleteReplica(ctx, 1, remoteID, 0)
}

func (pd *PlacementDriver) GetNewRegionID() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RegionOperationTimeOut)
	defer cancel()

	cmd, err := json.Marshal(Command{
		Op: OpGetNewRegionID,
	})
	if err != nil {
		return 0, err
	}

	// 从PD中获取一个新的RegionID, 作为本Region的ID
	res, err := pd.me.SyncPropose(ctx, pd.session, cmd)

	return res.Value, err
}

func (pd *PlacementDriver) GetFreeNodes(count int) ([]NodeConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RegionOperationTimeOut)
	defer cancel()

	// 从PD中获取一个新的RegionID, 作为本Region的ID
	nodes, err := pd.me.SyncRead(ctx, 1, Command{
		Op:    OpGetFreeNodes,
		Count: count,
	})

	return nodes.([]NodeConfig), err
}

func (pd *PlacementDriver) GetMemberShip(rid uint64) (*dragonboat.Membership, error) {
	ctx, cancel := context.WithTimeout(context.Background(), MemberChangeTimeOut)
	defer cancel()

	return pd.me.SyncGetShardMembership(ctx, rid)
}

func (pd *PlacementDriver) UpdateNodesMap(rid, nid uint64, addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), MemberChangeTimeOut)
	defer cancel()

	cmd, err := json.Marshal(Command{
		Op: OpGetNewRegionID,
		RegionArgs: RegionArgs{
			RegionID: rid,
		},
		NodeConfigArgs: NodeConfigArgs{
			NodeID: nid,
			Config: &NodeConfig{
				RaftAddr: addr,
			},
		},
	})
	if err != nil {
		return err
	}

	_, err = pd.me.SyncPropose(ctx, pd.session, cmd)
	return err
}

func (pd *PlacementDriver) JoinRegionNode(rid, nid uint64, addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), MemberChangeTimeOut)
	defer cancel()

	return pd.me.SyncRequestAddReplica(ctx, rid, nid, addr, 0)
}

func (pd *PlacementDriver) LeaveRegionNode(rid, nid uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), MemberChangeTimeOut)
	defer cancel()

	return pd.me.SyncRequestDeleteReplica(ctx, rid, nid, 0)
}

// 创建一个新的Region, 返回RegionID
func (pd *PlacementDriver) CreateRegion() (uint64, error) {
	rid, err := pd.GetNewRegionID()
	if err != nil {
		return rid, err
	}

	nodes, err := pd.GetFreeNodes(3)
	if err != nil {
		return rid, err
	}

	validNodes := make(map[string]uint64)
	for id, node := range nodes {
		validNodes[node.RaftAddr] = uint64(id + 1)
	}

	initMembers := make(map[uint64]string)
	for addr, id := range validNodes {
		initMembers[id] = addr
	}
	if _, ok := validNodes[pd.me.RaftAddress()]; !ok {
		initMembers[4] = pd.me.RaftAddress()
	}

	// 先从本机启动一个RaftGroup,其中包含三个节点, 然后通过LeaderTransfer转让给第一个节点
	err = pd.StartRegion(4, rid, initMembers)
	if err != nil {
		return rid, err
	}

	// 向所有node节点发送http请求告知以join方式启动
	postData := make(url.Values)
	postData.Add("rid", strconv.FormatInt(int64(rid), 10))
	postData.Add("nid", "")
	for id, addr := range initMembers {
		if addr != pd.me.RaftAddress() {
			continue
		}
		url := nodes[id-1].HttpAddr + "/region/create"
		postData.Set("nid", strconv.FormatUint(id, 10))
		log.Printf("Send Request to %s - [%d, %s]\n", url, id, addr)
		http.PostForm(url, postData)
	}

	if _, ok := validNodes[pd.me.RaftAddress()]; !ok {
		// 等待集群建立成功,我不是随机出来集群的一员
		go func() {
			for {
				if err := pd.LeaveRegionNode(rid, 4); err == nil {
					pd.me.RemoveData(rid, 4)
					break
				}
			}
		}()
	}

	return rid, err
}

func (pd *PlacementDriver) LocateRegionByKey(key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), MemberChangeTimeOut)
	defer cancel()

	res, err := pd.me.SyncRead(ctx, 1, Command{
		Op: OpLocateRegion,
		RegionArgs: RegionArgs{
			Key: key,
		},
	})
	if err != nil {
		return 0, err
	}

	return res.(uint64), nil
}
