package log

import (
	"fmt"
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	// to hold the offset of a record in the store file
	offWidth uint64 = 4
	// to hold the position of a record in the store file
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

/*
	consider that a Log is the highest level model in this service
	a Log then consists of segments
	a segment then wraps two other data models: an index and a store
	the index is closely associated with the store as it keeps track of the location of every record that is written
		into the store

	index wraps a File which will have location data about every record written

	index has a memory map, which is a byte slice representation of a File
	"The speed at which application instructions are processed on a system is proportionate to the number of access
		operations required to obtain data outside of program-addressable memory. The system provides two methods for
		reducing the transactional overhead associated with these external read and write operations. You can map file
		data into the process address space." (https://www.ibm.com/docs/en/aix/7.2?topic=memory-understanding-mapping)
	So this memory map essentially bring an os.File into memory and lets us write/read to/from it

	index has a size which corresponds to the number of record entries that are indexed in the underlying file

*/
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

/*
	newIndex creates a new index object, passing in two arguments
	f is a pointer to an os.File which will serve as the basis for the index
	c Config is a configuration object that tracks a few things:
		MaxStoreBytes is a uint64 that tells the Log how many bytes should be written to a single store file of the Log
		MaxIndexBytes is a uint64 that tells the Log how many bytes should be written to a single index file of the Log
		InititalOffset is the base offset for a Segment. Being that a Lof consists of many Segments, which consist of
			many indexes, and that the way we look up records is by their number (first recorded record is 1, the next
			is 2, etc), and that is  what we call offset, we need to know what is the first offset for an index in a
			given Segment. e.g. when a Log is created, it will have one segment, and therefore one index and store file.
			The first records to be recorded will be indexed at offests 0, 1, 2, 3, etc. When the index gets to the size
			for which beyond it will not be efficient to search through, then we need to create a new index, and
			therefore a new segment. This next index will pickup rtecording logs, and because it is the same overall
			Log, it needs to continue counting Records where the first index left off. Say we configure the
			MaxIndexBytes such that an index can hold 100 entries. That would mean that the index records entries at
			offsets 0 to 99. The new index then, will record entries at offsets 100 to 199. So... the initial offset of
			the new index will be 100
*/
func newIndex(f *os.File, c Config) (*index, error) {
	// create a new index
	idx := &index{
		file: f,
	}
	// obtain file information for f
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// here we set the `size` of the index; the truncation below actually grows the file to the max size allowed
	idx.size = uint64(fi.Size())
	/*
		here we use Truncate to set the size of the file f, to be the MaxIndexBytes, which is set in the Config
		we grow the file to the max size allowed, and we then memory map it. If it was not grown before memory mapping
			it would be stuck at whatever size it was when we mem-map it (probably too small a size) as we cannot resize
			once the mem-map has been created
	*/
	if err = os.Truncate(
		f.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	/*
		create the memory map; what is this?
		initial questions were: Does it just allow faster reads? Does it work almost like a cache of a file? and the
			answers are yes plus more and yes
	*/

	if idx.mmap, err = gommap.Map(
		// fd returns a file descriptor referencing the open file
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	/*
		we now have an index which has:
			an associated File which will have index entries written to it
			a memory map of that file
			and a size which is the size of that file at the point of the creation of this index
	*/
	return idx, nil
}

// Name returns the name of the index.File
func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	// flush changes in the mapped region to the file or device; file in this case
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	/*
		commit the in-memory file to stable storage
			we have already flushed the memory map, but this call ensures that general in-memory data is persisted to
			disk
	*/
	if err := i.file.Sync(); err != nil {
		return err
	}
	/*
		this call to Truncate cuts the file back down to size of index
		its size is made equal to how much data it currently holds, this way, the last entry comprises the last bytes
			in the file. This allows us to know where the last entry is, and to easily find it and its offset.
		Before truncating back down to this least needed size, there was some unknown amount of blank space
			due to the initial truncation
	*/
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	// close file; can no longer be used for I/O
	return i.file.Close()
}

/*
	Read receives `in` a record offset (or number) and will return:
		`out` which is largely ignored, it is just the offset number of the record
		`pos` this is the position of the record in the store file; so it can be used tko calculate the exact location
			of a record
	remember that the offset corresponds to the record entry number. So the firtst record recorded is 0 offset, then 1,
		and then 2
*/
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// if the index has no entries (size 0) return an end of file error as we are looking for a record in an empty file
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// still unclear what the point of checking this specific condition is
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	/*
		here we multiply the offset of the record that we are looking for by the length of an entire index entry
		this ensures that we start reading at the correct byte in the mem-map
		e.g. if we want the first record, we Read with offset 0 and this becomes pos = 0
			therefore, when we read for the actual position portion of the entry below, we start at:
				0+offWidth (which accounts for the offset portion of the entry)  and then we read to 0+entWidth...
			entWidth is the entire length of an entry, an entry comprising an offset number (len 4) and then a position
				number (len 8)
				So, we read from the end of the offset number 0+4 (which is the beginning of the position number) to
					the end of the position number 0+12
	*/
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

/*
	Write enters a new entry into the Index
	an entry in the index consists of an offset (record number) and a position
*/
func (i *index) Write(off uint32, pos uint64) error {
	/*
		first we check the status of the index
		if it is too small to hold another record then we return an End of File error
	*/
	if uint64(len(i.mmap)) < i.size+entWidth {
		fmt.Println("going to return eof")
		return io.EOF
	}
	/*
		the first step in entering a new record in the index is to record the offset
		we select a portion of the mem-mapped file (essentially a byte slice) equivalent to the length needed to record
			an offset. The offset number requires 4 bytes, so we say that from current size + 4 bytes, is equal to our
				offset that we want to record. So... "start at the current size, or the end of the last entry, and take
				the next 4 bytes, and use that space to record the offset number"
	*/
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	/*
		we then record the position, and this is the position of the corresponding record within the store file. So we
			map record entries in the index according to the offset of the record (or the number of the record: 1st,
			2nd, 3rd, etc)
		we say essentially "find the end of the offset number that we literally just recorded, and from there, grab 8
			more bytes (entWidth - the already accounted for offWidth of 4 bytes) and set that space to be the position
			of the record
	*/
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// increment size by what we have just added: and offset of offWidth length, and a pos of posWidth length, i.e.
	//    entwidth. This is where the next entry will begin
	i.size += entWidth
	return nil
}
