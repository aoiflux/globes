package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	progressbar "github.com/schollz/progressbar/v3"
)

func storesingle(srchash []byte, size int64, srcFile *os.File, db *badger.DB) error {
	fnode, err := fileNodePreflight(srchash, srcFile, db)
	if err != nil {
		return err
	}
	if fnode.Completed {
		return nil
	}

	var buffsize int64
	bar := progressbar.DefaultBytes(size)
	fmt.Println("Saving File")

	for storeIndex := int64(0); ; storeIndex += ChonkSize {
		if storeIndex > size {
			break
		}

		if size-storeIndex <= ChonkSize {
			buffsize = size - storeIndex
		} else {
			buffsize = ChonkSize
		}

		lostchunk := make([]byte, buffsize)
		_, err = srcFile.Read(lostchunk)
		if err != nil && err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		lostChunkHash := getchunkhash(lostchunk)

		_, chunkErr := getChunkNode(lostChunkHash, db)
		if chunkErr != nil && chunkErr != badger.ErrKeyNotFound {
			return chunkErr
		}
		if chunkErr != nil && chunkErr == badger.ErrKeyNotFound {
			var chunk ChonkNode
			chunk.CKey = lostChunkHash
			chunk.CData = lostchunk
			err := setChunkNode(chunk, db)
			if err != nil {
				return err
			}
		}

		var relErr error
		if chunkErr == nil {
			_, relErr = getRelationEdge(srchash, storeIndex, db)
		}
		if relErr != nil && relErr != badger.ErrKeyNotFound {
			return err
		}
		if relErr == nil && chunkErr == nil {
			bar.Add64(buffsize)
			continue
		}

		var rel RelationEdge
		rel.CHash = lostChunkHash
		rel.FHash = srchash
		rel.Index = storeIndex
		err := setRelationEdge(rel, db)
		if err != nil {
			return err
		}
		bar.Add64(buffsize)
	}
	bar.Finish()

	return nil
}

func store(srchash []byte, size int64, srcFile *os.File, db *badger.DB) error {
	fnode, err := fileNodePreflight(srchash, srcFile, db)
	if err != nil {
		return err
	}
	if fnode.Completed {
		return nil
	}

	var active int
	var buffsize int64
	bar := progressbar.DefaultBytes(size)
	fmt.Println("Saving File")

	var tio ThreadIO
	tio.FHash = srchash
	tio.DB = db
	tio.Err = make(chan error, MaxThreadCount)

	for storeIndex := int64(0); ; storeIndex += ChonkSize {
		if storeIndex > size {
			break
		}
		tio.Index = storeIndex

		if size-storeIndex <= ChonkSize {
			buffsize = size - storeIndex
		} else {
			buffsize = ChonkSize
		}

		lostchunk := make([]byte, buffsize)
		_, err = srcFile.Read(lostchunk)
		if err != nil && err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		tio.LostChunk = lostchunk

		go storeWorker(tio)
		active++

		if active > MaxThreadCount {
			err := <-tio.Err
			if err != nil {
				return err
			}
			active--
			bar.Add64(buffsize)
		}
	}

	for active > 0 {
		err := <-tio.Err
		if err != nil {
			return err
		}
		active--
		bar.Add64(ChonkSize)
	}

	bar.Add64(ChonkSize)
	bar.Finish()
	return nil
}

func storeWorker(tio ThreadIO) {
	lostChunkHash := getchunkhash(tio.LostChunk)

	_, chunkErr := getChunkNode(lostChunkHash, tio.DB)
	if chunkErr != nil && chunkErr != badger.ErrKeyNotFound {
		tio.Err <- chunkErr
	}
	if chunkErr != nil && chunkErr == badger.ErrKeyNotFound {
		var chunk ChonkNode
		chunk.CKey = lostChunkHash
		chunk.CData = tio.LostChunk
		err := setChunkNode(chunk, tio.DB)
		if err != nil {
			tio.Err <- err
		}
	}

	var relErr error
	if chunkErr == nil {
		_, relErr = getRelationEdge(tio.FHash, tio.Index, tio.DB)
	}
	if relErr != nil && relErr != badger.ErrKeyNotFound {
		tio.Err <- relErr
	}
	if relErr == nil && chunkErr == nil {
		tio.Err <- nil
	}

	var rel RelationEdge
	rel.CHash = lostChunkHash
	rel.FHash = tio.FHash
	rel.Index = tio.Index
	tio.Err <- setRelationEdge(rel, tio.DB)
}

func fileNodePreflight(hash []byte, srcFile *os.File, db *badger.DB) (FileNode, error) {
	var fnode FileNode
	var err error
	srcFileName := filepath.Base(srcFile.Name())

	fnode, err = getFileNode(hash, db)
	if err != nil && err != badger.ErrKeyNotFound {
		return fnode, err
	}

	if err != nil && err == badger.ErrKeyNotFound {
		fnode.FHash = hash
		fnode.Names = []string{srcFileName}
		err = setNamesFileNode(fnode, db)
		return fnode, err
	}

	if !fnode.Completed || contains(srcFileName, fnode.Names) {
		return fnode, err
	}

	fnode.FHash = hash
	fnode.Names = append(fnode.Names, srcFileName)
	err = setNamesFileNode(fnode, db)
	return fnode, err
}

func contains(search string, space []string) bool {
	for _, val := range space {
		if search == val {
			return true
		}
	}
	return false
}
