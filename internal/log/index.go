package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth		= offWidth + posWidth
)

//	index file for record lookup
//		underlying file
//		in-memory map of file
//		current size
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	//	here we grow the file to max size for index files
	//		this is done before creating in-mem representation so
	//		the whole index will be available in memory rather than
	//		the current size only
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

//	Close the index and make it ready for for a service restart
func (i *index) Close() error {
	//	flush data in mmap memory region to the device
	//		MS_SYNC: perform flush syncronously
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	//	data in mmap has been flushed to file, now file will be flushed to stable
	//		storage
	if err := i.file.Sync(); err != nil {
		return err
	}

	//	truncate file back to actual size as writes have problaby
	//		been made since opening and mmapping
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

//	Read takes an offset (record number essentially; zero indexed) and returns the offset
//		and position from the index
func (i *index) Read(offset int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	//	-1 to get last record
	if offset == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(offset)
	}

	pos = uint64(out) * entWidth

	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	//	gets the offset number from the index
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	//	gets the posisition of the record in the store
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

//	Write appends a new entry and updates the size of the index
func (i *index) Write(offset uint32, pos uint64) error {
	//	check whether given a new entry the file will grow beyond the size of the mmap
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	//	encode offset and position and append to mmap
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// update size of index 
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}