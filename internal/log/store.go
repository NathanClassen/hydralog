package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

//	this is the number of bytes that will be used to record the
//		length of the record each time a new record is written
const lenWidth = 8

type store struct {
	File *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// creates a new store from file, getting the size of the store
//
//	via os.Stat, and setting a writer for the file
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

//	writes a new record to the store. Writes to the buffered writer
//		rather than directly to the file to reduce system calls and
//		improve performance
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	//	lock the store to avoid collisions and inconsistent data
	s.mu.Lock()
	defer s.mu.Unlock()
	
	//	at every append, the position will be equal to the current
	//		 size of the store-the latest place to write a record
	pos = s.size

	//	begin writing to the buf (Writer)
	//	in preparation to write the new record, we first write the
	//		length of the record to be written-this will allow us
	//		to read precisely the correct number of bytes when
	//		reading the record
	//	this length is written in binary encording
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	//	write the content of the record and return the number of 
	//		bytes written, i.e. the length of the record
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	//	length of record just written + number of bytes used to 
	//		record the records length. This is the length of one
	//		complete entry...
	w += lenWidth

	//	...ergo, the size of the store is now increased by `w`
	s.size += uint64(w)

	//	return the length of the entry just made and the position
	//		of the entry in the store
	return uint64(w), pos, nil
}

//	reads a record from the store
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//	since we will be reading from the file, we need to write any 
	//		buffered data to the file to ensure the complete store
	//		is available for reading
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	//	we know where the record entry starts, and that every entry 
	//		begins with a number entry telling us how long the actual
	//		record is and thus how many bytes need to be read. So we
	//		create a slice to hold that number entry-it's of len `lenWidth`
	//		because that's how many bytes we use to store the record len
	size := make([]byte, lenWidth)
	//	read in the length entry
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	//	now that we know the length of the record, create a slice to 
	//		hold it
	b := make([]byte, enc.Uint64(size))

	//	read the record of length len(b) into b. We start reading at
	//		pos+lenWidth because pos is where the record entry begins;
	//		it begins with a length indicator of length lenWidth. So the
	//		record itself begins at pos+lenWidth
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	//	return the record
	return b, nil
}

//	implement the ReadAt interface
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

//	persist any buffered data and then close the store file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}

func (s *store) Name() string {
	return s.File.Name()
}