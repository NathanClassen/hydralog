package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/NathanClassen/hydralog/api/v1"
	"google.golang.org/protobuf/proto"
)

//	Segement is an abstraction over a store and an index
type segment struct {
	//	the baseOffset of each segment is the offset of the first
	//		entry in that segments index. E.g., if the log has 2 segments
	//		the first having entires 1 - 5 and the second entries 6 - 10, 
	//		then the base offset for the second is 6. The entries for each
	//		segment are relative to the baseOffset. So fetching the 6th will 
	//		record from the log will Read(6) from the second segment. So that 
	//		segement will return the entry at 6 - baseOffset, I.E. it's 0th entry.
	store *store
	index *index
	baseOffset, nextOffset uint64
	config Config
}

//	Return a pointer to a segement
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	//	Create segement; records will begin at baseOffset record
	s := &segment{
		baseOffset: baseOffset,
		config: c,
	}

	var err error
	//	open or create file baseOffset.store to function as store file
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	//	create store out of store file
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	//	open or create baseOffset.index file
	//	why no append flag for the index file?
	//		O_APPEND is not used because writes will be made to the mmap and then the
	//		entire contents will be written to the file
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}
	//	create index out of index file
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	//	check to see if the index already has entries, if not, then
	//		the nextOffset should be the baseOffset
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		//	if so, the nextOffset is the base + the latest offset + 1
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// obtain next offset for segment and set on record
	cur := s.nextOffset
	record.Offset = cur
	//	marshall record into pb
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	//	append the record to the segment store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	//	write the index for the record
	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	//	update the next offset on the segment
	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(offset uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(offset - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
