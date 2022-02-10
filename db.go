package main

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v3"
)

func connect(datadir string) (*badger.DB, error) {
	opts := badger.DefaultOptions(datadir)
	opts.IndexCacheSize = CacheLimit
	opts.SyncWrites = true
	opts.NumGoroutines = MaxThreadCount
	opts.BlockCacheSize = CacheLimit
	return badger.Open(opts)
}

func getFileNode(fhash []byte, db *badger.DB) (FileNode, error) {
	var fnode FileNode

	keystring := fmt.Sprintf("%s%v", FileNamespace, fhash)
	key := []byte(keystring)

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &fnode)
		})

		return err
	})

	fnode.FHash = fhash
	return fnode, err
}
func setNamesFileNode(fnode FileNode, db *badger.DB) error {
	keystring := fmt.Sprintf("%s%v", FileNamespace, fnode.FHash)
	key := []byte(keystring)

	fdata, err := json.Marshal(fnode)
	if err != nil {
		return err
	}

	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, fdata)
	})
}
func setCompletedFileNode(fhash []byte, db *badger.DB) error {
	keystring := fmt.Sprintf("%s%v", FileNamespace, fhash)
	key := []byte(keystring)

	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			var fnode FileNode

			err := json.Unmarshal(val, &fnode)
			if err != nil {
				return err
			}

			fnode.Completed = true
			fdata, err := json.Marshal(fnode)
			if err != nil {
				return err
			}

			return txn.Set(key, fdata)
		})

		return err
	})

	return err
}

func getChunkNode(chash []byte, db *badger.DB) ([]byte, error) {
	keystring := fmt.Sprintf("%s%v", ChonkNamespace, chash)
	key := []byte(keystring)

	var data []byte

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			data, err = item.ValueCopy(val)
			return err
		})

		return err
	})

	return data, err
}
func setChunkNode(cnode ChonkNode, db *badger.DB) error {
	keystring := fmt.Sprintf("%s%v", ChonkNamespace, cnode.CKey)
	key := []byte(keystring)
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, cnode.CData)
	})
}

func getRelationEdge(fhash []byte, index int64, db *badger.DB) ([]byte, error) {
	keystring := fmt.Sprintf("%s%v%s%d", RelationNapespace, fhash, HashIndexSeperator, index)
	key := []byte(keystring)

	var chash []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			chash, err = item.ValueCopy(val)
			return err
		})

		return err
	})

	return chash, err
}
func setRelationEdge(rel RelationEdge, db *badger.DB) error {
	keystring := fmt.Sprintf("%s%v%s%d", RelationNapespace, rel.FHash, HashIndexSeperator, rel.Index)
	key := []byte(keystring)

	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, rel.CHash)
	})
}
