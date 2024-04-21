package kv

import (
	"encoding/json"
	"errors"
	"io"
	"sync"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

type MemoryKVStateMachine struct {
	NodeID   uint64
	RegionID uint64

	mu    sync.Mutex
	datas map[string]string
}

type CommandOp uint64

const (
	OpGet CommandOp = iota
	OpSet
	OpDelete
	OpList
)

type Command struct {
	Key      string    `json:"key"`
	Value    string    `json:"value"`
	Operator CommandOp `json:"operator"`
}

var ErrorNoKey = errors.New("ErrorNoKey")
var ErrorOp = errors.New("ErrorNoOp")

func NewMemoryKVStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	return &MemoryKVStateMachine{
		NodeID:   replicaID,
		RegionID: shardID,
		datas:    make(map[string]string),
	}
}

func (s *MemoryKVStateMachine) Lookup(query interface{}) (interface{}, error) {
	var command Command
	err := json.Unmarshal(query.([]byte), &command)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch command.Operator {
	case OpGet:
		if val, ok := s.datas[command.Key]; !ok {
			return nil, ErrorNoKey
		} else {
			return val, nil
		}
	case OpList:
		return s.datas, nil
	}
	return nil, ErrorOp
}

func (s *MemoryKVStateMachine) Update(e sm.Entry) (sm.Result, error) {
	var command Command
	err := json.Unmarshal(e.Cmd, &command)
	if err != nil {
		return sm.Result{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch command.Operator {
	case OpSet:
		oldValue := s.datas[command.Key]
		s.datas[command.Key] = command.Value

		return sm.Result{
			Data: []byte(oldValue),
		}, nil
	case OpDelete:
		if oldValue, ok := s.datas[command.Key]; !ok {
			return sm.Result{}, ErrorNoKey
		} else {
			delete(s.datas, command.Key)
			return sm.Result{
				Data: []byte(oldValue),
			}, nil
		}
	}
	return sm.Result{}, ErrorOp
}

func (s *MemoryKVStateMachine) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	datas, _ := json.Marshal(s.datas)
	_, err := w.Write(datas)
	return err
}

func (s *MemoryKVStateMachine) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var datas map[string]string
	json.Unmarshal(data, &datas)
	s.datas = datas
	return nil
}

func (s *MemoryKVStateMachine) Close() error { return nil }
