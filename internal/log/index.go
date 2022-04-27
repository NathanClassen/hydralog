package log

import (
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

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex: creates an index for a given file
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
	// truncate the size of the file f, to be the MaxIndexBytes of Segment
	// we grow the file to the max size allowed, and we then memory map it. If it was not grown before memory mapping
	//   it would be stuck at whatever size it was when we mem-map it (probably too small a size) as we cannot resize
	//   once the mem-map has been created
	if err = os.Truncate(
		f.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	/* create the memory map; what is this? Does it just allow faster reads? Does it work almost like a cache of a file?
	     page 36 in the book seems to indicate that a memory map of a file is a slice of bytes; that is, we translate
		   the entire contents of the file into a byte slice. Maybe that looks like:

		e.g.

			Hello,

				Hello world!

			Bye now

	     memory mapped version:

		[
			"H","e","l","l","o",",",
			"\n","\n","\t","H","e","l","l","o"," ","w","o","r","l","d","!",
			"\n","\n","B","y","e"," ","n","o","w",
		]
	*/

	if idx.mmap, err = gommap.Map(
		// fd returns a file descriptor referencing the open file
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	// return newly created index struct
	return idx, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	// checks if index is empty
	if in == -1 {
		// (i.size / entWidth) should give number of entries as entWidth is the length of 1 entry
		// if index (and therefore log is empty, this is (0 / 12) - 1 = -1
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// we set the location in the file from current size (end of file) to size of offsets (4) to be a new offset
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// we set the location in the file from the just set offset to posWidth (8) to be the pos
	// we get the location for the pos by taking the space from size+offWidth (which gives us the end of the just set
	//    offset) up to size+entwidth (which is size at this write time + offWidth (which gets us to end of just set
	//    offset) + posWidth (which gets us 8 bytes from the just set offset))
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// increment size by what we have just added: and offset of offWidth length, and a pos of posWidth length, i.e.
	//    entwidth. This is where the next entry will begin
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	// flush changes in the mapped region to 'device' ?
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// commit the in-memory file changes to stable storage
	if err := i.file.Sync(); err != nil {
		return err
	}
	// truncate file to size of index
	// we truncate it back down, to be only the size it needs to be given how much data it currently holds
	//    this way, the last entry comprises the last bytes in the file. This allows us to know where the last entry
	//	  is, and to easily find it and its offset. Before truncating back down to this least needed size, there was
	//    some unknown amount of blank space due to the initial truncation
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	// close file; can no longer be used for I/O
	return i.file.Close()
}
