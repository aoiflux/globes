package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/dgraph-io/badger/v3"
	"golang.org/x/crypto/sha3"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("need db path, src and dst file paths")
		return
	}

	datapath := os.Args[1]
	srcpath := os.Args[2]
	dstpath := os.Args[3]

	fmt.Printf("Starting global store...\n\n")

	db, err := connect(datapath)
	handle(err, db)

	size, srcFile, err := openFileR(srcpath)
	handle(err, db)
	defer srcFile.Close()

	srcHash, err := getfilehash(srcFile)
	handle(err, db)

	start := time.Now()
	err = storesingle(srcHash, size, srcFile, db)
	handle(err, db)
	err = setCompletedFileNode(srcHash, db)
	handle(err, db)
	fmt.Printf("Store Time: %s\n\n", time.Since(start))

	dstFile, err := os.OpenFile(dstpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	handle(err, db)
	defer dstFile.Close()

	start = time.Now()
	err = restore(srcHash, dstFile, db)
	handle(err, db)
	fmt.Println("Restore Time: ", time.Since(start))

	dstHash, err := getfilehash(dstFile)
	handle(err, db)

	if diff := bytes.Compare(srcHash, dstHash); diff == 0 {
		fmt.Printf("\nFiles Match!\n\n\n")
	}

	db.Close()
}

func openFileR(path string) (int64, *os.File, error) {
	srcInfo, err := os.Stat(path)
	if err != nil {
		return 0, nil, err
	}

	srcFile, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}

	return srcInfo.Size(), srcFile, nil
}

func getfilehash(fileHandle *os.File) ([]byte, error) {
	info, err := os.Stat(fileHandle.Name())
	if err != nil {
		return nil, err
	}

	fmt.Println("Generating SHA3-256 hash for file: ", fileHandle.Name())
	start := time.Now()
	fileHandle.Seek(0, io.SeekStart)

	hasher := sha3.New256()
	bar := pb.Full.Start64(info.Size())

	barReader := bar.NewProxyReader(fileHandle)
	_, err = io.Copy(hasher, barReader)
	if err != nil {
		return nil, err
	}
	hash := hasher.Sum(nil)

	bar.Finish()
	fmt.Printf("Operation completed in: %s\n\n", time.Since(start))
	fileHandle.Seek(0, io.SeekStart)
	return hash, nil
}

func getchunkhash(data []byte) []byte {
	hasher := sha3.New512()
	reader := bytes.NewReader(data)
	io.Copy(hasher, reader)
	return hasher.Sum(nil)
}

func handle(err error, db *badger.DB) {
	if err != nil {

		if db != nil {
			db.Close()
		}

		fmt.Printf("\n\n%v\n\n", err)
		os.Exit(1)
	}
}
