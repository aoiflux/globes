package main

import (
	"errors"
	"runtime"
)

const (
	B  int64 = 1
	KB       = B << 10
	MB       = KB << 10
	GB       = MB << 10
	TB       = GB << 10
)

const (
	CacheLimit = GB
	ChonkSize  = 256 * KB
)

var MaxThreadCount = 2 * runtime.NumCPU()

const (
	FileNamespace      = "file:"
	RelationNapespace  = "rel:"
	ChonkNamespace     = "cnk:"
	HashIndexSeperator = "|"
)

var (
	ErrIncompleteFile = errors.New("incomplete file")
)
