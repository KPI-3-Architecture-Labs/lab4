package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const bufSize = 8192

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashInd map[string]int64

type FileSegment struct {
	index   hashInd
	outPath string
	mutex   sync.RWMutex
}

type Db struct {
	out         *os.File
	outOffset   int64
	dir         string
	segmentSize int64
	totalNumber int
	segments    []*FileSegment
	indexMutex  sync.RWMutex
}

func (s *FileSegment) getValue(position int64) (string, error) {
	file, err := os.Open(s.outPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	newReader := bufio.NewReader(file)
	_, err = newReader.Discard(int(position))
	if err != nil {
		return "", err
	}

	value, err := readValue(newReader)
	if err != nil {
		return "", err
	}

	return value, nil
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segments:    make([]*FileSegment, 0),
		dir:         dir,
		segmentSize: segmentSize,
	}

	err := db.newSegment()
	if err != nil {
		return nil, err
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	return db, nil
}

func (db *Db) newSegment() error {
	outFile := fmt.Sprintf("%s%d", outFileName, db.totalNumber)
	outPath := filepath.Join(db.dir, outFile)
	db.totalNumber++

	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0o600)

	if err != nil {
		return err
	}

	newFileSegment := &FileSegment{
		outPath: outPath,
		index:   make(hashInd),
	}

	db.out.Close()
	db.out = f
	db.outOffset = 0

	db.segments = append(db.segments, newFileSegment)

	if len(db.segments) >= 3 {
		db.consolidateSegments()
	}
	return nil
}

func (db *Db) consolidateSegments() {
	go func() {
		outFile := fmt.Sprintf("%s%d", outFileName, db.totalNumber)
		outPath := filepath.Join(db.dir, outFile)
		db.totalNumber++

		newSegment := &FileSegment{
			outPath: outPath,
			index:   make(hashInd),
		}
		var offset int64

		f, err := os.OpenFile(outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return
		}
		defer f.Close()

		lastIndex := len(db.segments) - 2

		for i := 0; i <= lastIndex; i++ {

			s := db.segments[i]

			for key, index := range s.index {
				if i < lastIndex {
					cloneFlag := false
					for _, segment := range db.segments[i+1 : lastIndex+1] {
						if _, ok := segment.index[key]; ok {
							cloneFlag = true
							break
						}
					}
					if cloneFlag {
						continue
					}
				}

				value, _ := s.getValue(index)

				entry := entry{
					key:   key,
					value: value,
				}

				n, err := f.Write(entry.Encode())

				if err == nil {
					newSegment.index[key] = offset
					offset += int64(n)
				}
			}
		}

		db.segments = []*FileSegment{newSegment, db.segments[len(db.segments)-1]}
	}()
}

func (db *Db) recover() error {
	var err error
	var buf [bufSize]byte

	in := bufio.NewReaderSize(db.out, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)

		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}

		n, err = in.Read(data)

		if err == nil {
			var e entry
			e.Decode(data)
			db.segments[len(db.segments)-1].index[e.key] = db.outOffset
			db.outOffset += int64(n)
			db.indexMutex.Lock()
			db.indexMutex.Unlock()
		}
	}

	return err
}

func (db *Db) Get(key string) (string, error) {
	db.indexMutex.RLock()
	defer db.indexMutex.RUnlock()

	var (
		segment *FileSegment
		pos     int64
		ok      bool
	)

	for i := range db.segments {
		segment = db.segments[len(db.segments)-i-1]
		segment.mutex.RLock()

		pos, ok = segment.index[key]

		segment.mutex.RUnlock()
		if ok {
			break
		}
	}

	if !ok {
		return "", ErrNotFound
	}

	value, err := segment.getValue(pos)
	if err != nil {
		return "", err
	}

	return value, nil
}

func (db *Db) Put(key, value string) error {
	entry := entry{
		key:   key,
		value: value,
	}

	db.indexMutex.Lock()
	defer db.indexMutex.Unlock()

	encodedEntry := entry.Encode()
	size := int64(len(encodedEntry))

	stat, err := db.out.Stat()
	if err != nil {
		return err
	}

	if stat.Size()+size > db.segmentSize {
		err := db.newSegment()
		if err != nil {
			return err
		}
	}

	n, err := db.out.Write(encodedEntry)
	if err != nil {
		return err
	}

	db.segments[len(db.segments)-1].index[entry.key] = db.outOffset
	db.segments[len(db.segments)-1].mutex.Lock()
	db.segments[len(db.segments)-1].mutex.Unlock()
	db.outOffset += int64(n)

	return nil
}

func (db *Db) Close() { db.out.Close() }
