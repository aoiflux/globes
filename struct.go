package main

import (
	"github.com/dgraph-io/badger/v3"
)

type FileNode struct {
	FHash     []byte
	Names     []string `json:"names"`
	Completed bool     `json:"completed"`
}

type ChonkNode struct {
	CKey  []byte
	CData []byte
}

type RelationEdge struct {
	FHash []byte
	Index int64
	CHash []byte
}

type ThreadIO struct {
	Index     int64
	FHash     []byte
	LostChunk []byte
	DB        *badger.DB
	Err       chan error
}
