package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		// this is a buffered writer for the file
		// TODO: research the concept of buffers in general as well as the purpose and function of a buffered writer such as this
		buf: bufio.NewWriter(f),
	}, nil
}

// ReadAt makes store implement the io.ReaderAt interface
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close persists any buffered data to disk before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}

// Append appends a new record to the log
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// get current size; this will be equal to the position of the new data, as it will be appended
	pos = s.size
	// first write the length of the new record so that we know how many bytes to read upon reading the record
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// write the actual data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// add 8 to the number of bytes written that gets returned; possibly to account for the length we wrote first
	w += lenWidth
	// update the store size with the new length
	s.size += uint64(w)
	// return number of bytes written, the position of this appended record and no error
	return uint64(w), pos, nil
}

// Read reads a record from the log
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Flush writes any buffered data to the underlying io.Writer; in this case s.File
	// we do this in case there is any record(s) appended but yet unwritten to the store
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// create slice to hold the size of the record to be read
	size := make([]byte, lenWidth)
	// read the size of the record to be read, into the `size` slice
	// (remember, the first chunk of data per append, will be lenWidth in size and will hold the size of the subsequent record)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// create a slice to hold the record to be read
	b := make([]byte, enc.Uint64(size))
	// read the record into the slice; we know how much to read (i.e. how large the record is) by virtue of the `size` slice
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	// return the record
	return b, nil
}
