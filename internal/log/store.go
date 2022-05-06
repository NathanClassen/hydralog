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

/*
	lenWidth is the allotted number of bytes to use when recording the length of a particular record entry
		specifying the length explicitly lets us know exactly how much data corresponds to the length of a record
		(so metadata about the record) vs, how much is the actual record
*/
const (
	lenWidth = 8
)

/*
	consider that a Log is the highest level model in this service
	a Log then consists of segments
	a segment then wraps two other data models: an index and a store
	the Store is the log part of the Log; it holds all of the records that are sent to the Log


	store wraps a File which will have records written to it, this is the logs of our Log so a Log has a store

	store has a mutex which is used to prevent collisions when writing; we do not 'assign' the mutex in the constructor
		because the zero value for a mutex is an unlocked mutex

	store has a bufio.Writer which implements buffering for the File. Data is written to the buffer and under certain
		conditions, that data is persisted to the underlying io.Writer (in this case a File) via Flush

	store has a size which corresponds to the amount of data in the file which the number of records can be
		extrapolated from--as well as at what byte the last record falls
	this size is incremented everytime a new record is appended to the store file
*/
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a new store passing in a pointer to a File which shall be used as the basis of the store
func newStore(f *os.File) (*store, error) {
	// get metadata information about the File
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	/*
		here we set store.size to be equal to the current size of the File
		when appending a Record to the File, it will start right at the end of the existing data. So for an empty
			file (size 0), the new record will be at position 0. With a File that has, say 160 bytes of data aleady
			written, the position of the new record will be at position 160 (the end of the file before the record was
			written)
	*/
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		// create the buffer for the File
		// TODO: research the concept of buffers in general as well as the purpose and function of a buffered writer such as this
		buf: bufio.NewWriter(f),
	}, nil
}

/*
	ReadAt makes store implement the io.ReaderAt interface and so we can pass an instance of store to any function
		that takes an io.ReaderAt as an argument
*/
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	// lock the store before reading and unlock at end of procedure via defer
	s.mu.Lock()
	defer s.mu.Unlock()
	// any data in the buffer is Flushed to the File so that our data is complete before reading
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	//	starting at position `off`, read the store File
	//	reads len(p) bytes into p, and returns the number of bytes read (assuming no error, will be equal to len(p))
	return s.File.ReadAt(p, off)
}

// Close persists any buffered data to disk before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// before closing, make sure any "in progress" writes are completely written to the store File
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	// close the File; it can no longer be used for I/O ops unless opened again
	return s.File.Close()
}

// Append appends a new record to the log
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	/*
		get current size; this will be equal to the position of the new data, as it will be appended
		this value is returned and subsequently used to write a new entry to the index file; it is the "position" value
			of the index entry
	*/
	pos = s.size
	/*
		first write the length of this new record so that we know how many bytes to read upon reading the record
		this is the first part of a record entry in the store file. The index is used to find the location of this
			entry, and the first thing read in the entry is this, the length of the entry
	*/
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	/*
		write the actual Record data
		this Record will immediately follow the length of the record. So we write:
			(len(record))(record)
		note that the length of the record (len(record)) will take 8 bytes; thus we track the number of bytes used to
			record Record lengths with the variable lenWidth
	*/
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// add 8 to the number of bytes written that gets returned to account for the length we wrote first
	w += lenWidth
	/*
		update the store size with the new length
		this will be the length the next time a record is added to the log and thus, the position of that record
	*/
	s.size += uint64(w)
	// return number of bytes written, the position of this appended record and no error
	return uint64(w), pos, nil
}

// Read reads a record from the log
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	/*
		Flush writes any buffered data, still held in the buffer, to the underlying io.Writer; in this case s.File
		we do this in case there is any record(s) appended but yet unwritten to the store, because we want to make sure
			that we read from the store in a consistent state
	*/
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	/*
		create byte slice of the proper size to hold the `size` of the record to be read
		the size of the slice is lenWidth or 8
		again we use this size because the numbers that we write before each record to track the size of the record,
			take up 8 bytes. So to read the first 8 bytes of data is to read a number value which will tell us how many
			subsequent bytes of data to read in order to read the entire record
	*/
	size := make([]byte, lenWidth)

	/*
		read the size_of_the_record to be read, into the `size` slice
		of course we begin reading at `pos` (position)
	*/
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	/*
		create a slice to hold the record to be read
		at this point, `size` byte slice holds a number which is the number of bytes that make up the actual
			record data. We create s slice, `b` of that size so that we can read all of the Record data into it

	*/
	b := make([]byte, enc.Uint64(size))

	/*
		we read len(b) number of bytes into b, and thus read in the whole record
		we start reading for the record data at at pos+lenWidth because that is equal to:
			"where does the entry start (entry being len(recordItself)recordItself), and then skip lenWidth, which is
			8 bytes, to avoid reading the lengthOfRecord portion of the entry"
	*/
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	// b is a slice of bytes which contains the record data... send it!
	return b, nil
}
