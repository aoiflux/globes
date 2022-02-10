package main

import (
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v3"
)

func restore(fhash []byte, dstfile *os.File, db *badger.DB) error {
	fnode, err := getFileNode(fhash, db)
	if err != nil {
		return err
	}
	if !fnode.Completed {
		return ErrIncompleteFile
	}

	fmt.Println("Restoring File: ")
	for restoreIndex := int64(0); ; restoreIndex += ChonkSize {
		chash, err := getRelationEdge(fhash, restoreIndex, db)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err != nil && err == badger.ErrKeyNotFound {
			break
		}

		data, err := getChunkNode(chash, db)
		if err != nil {
			return err
		}

		_, err = dstfile.Write(data)
		if err != nil {
			return err
		}
	}

	return nil
}
