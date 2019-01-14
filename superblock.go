package stud

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/coyove/common/lru"
	mmap "github.com/edsrzf/mmap-go"
)

var superBlockMagic = [4]byte{'z', 'z', 'z', '0'}

const (
	superBlockSize = 80
	nodeBlockSize  = 16 + maxItems*itemSize + maxChildren*8

	// normally an insert op won't affect more than 8 nodes, if it does, we have to save snapshot to an external file
	snapshotSize = 4 + superBlockSize + 8*nodeBlockSize + 16 // <32K
)

type SuperBlock struct {
	magic        [4]byte
	endian       byte
	reserved     [7]byte
	mmapSize     int32
	mmapSizeUsed int32
	createdAt    uint32
	size         int64
	count        uint64
	tailptr      int64
	salt         [16]byte
	rootNode     int64
	superHash    uint64

	_fd, _fdl   *os.File
	_mmap       mmap.MMap
	_cacheFds   chan *os.File
	_cacheBytes *lru.Cache
	_filename   string
	_root       *nodeBlock
	_lock       sync.RWMutex
	_closed     bool

	// snapshots store the "stable" states of SuperBlock (and nodeBlock)
	// when dirty nodes are about to sync, new states will go to pending snapshots,
	// only when all dirty nodes are updated without errors, pending snapshots become stable snapshots
	// if any error happened, all nodes revert back to last snapshots.
	_dirtyNodes        map[*nodeBlock]bool
	_snapshot          [superBlockSize]byte
	_snapshotPending   [superBlockSize]byte
	_snapshotChPending map[*nodeBlock][nodeBlockSize]byte
}

func (b *SuperBlock) newNode() *nodeBlock {
	return &nodeBlock{
		magic:  nodeMagic,
		_super: b,
	}
}

func (b *SuperBlock) revertToLastSnapshot() {
	*(*[superBlockSize]byte)(unsafe.Pointer(b)) = b._snapshot
	b._root = nil
}

func (b *SuperBlock) sync() error {
	h := fnv.New64()
	blockHdr := *(*[superBlockSize]byte)(unsafe.Pointer(b))
	h.Write(blockHdr[:superBlockSize-8])
	b.superHash = h.Sum64()
	blockHdr = *(*[superBlockSize]byte)(unsafe.Pointer(b))

	var err error
	if b._mmap == nil {
		b._fd.Seek(0, 0)
		if _, err := b._fd.Write(blockHdr[:]); err != nil {
			return err
		}
		err = b._fd.Sync()
	} else {
		copy(b._mmap, blockHdr[:])
		//err = b._mmap.Flush()
	}
	if err == nil {
		b._snapshotPending = blockHdr
	}
	return err
}

// Count returns the count of keys stored
func (b *SuperBlock) Count() int { return int(b.count) }

// Size returns the total size of all data stored in bytes
func (b *SuperBlock) Size() int64 { return b.size }

// FileSize returns the actual size of stud in bytes
func (b *SuperBlock) FileSize() int64 { e, _ := b._fd.Seek(0, 2); return e }

// Tail returns the tail pointer position
func (b *SuperBlock) Tail() int64 { return b.tailptr }

// Close closes stud
func (b *SuperBlock) Close() error {
	b._lock.Lock()
	defer b._lock.Unlock()

	if b._closed {
		return nil
	}
	b._closed = true
	if err := b._mmap.Unlock(); err != nil {
		return err
	}
	if err := b._mmap.Unmap(); err != nil {
		return err
	}
	if err := b._fd.Close(); err != nil {
		return err
	}
	if err := b._fdl.Close(); err != nil {
		return err
	}
	if err := os.Remove(b._filename + ".lock"); err != nil {
		return err
	}

CLOSE_CACHE:
	for {
		select {
		case f := <-b._cacheFds:
			if err := f.Close(); err != nil {
				return err
			}
		default:
			break CLOSE_CACHE
		}
	}
	close(b._cacheFds)
	return nil
}

// Walk interates all data stored in stud
// If filter function returns false, then callback function will not be called (no Stream will open)
// After callback, the Stream will be closed automatically
func (sb *SuperBlock) Walk(filter func(Metadata) bool, callback func(string, *Stream) error) error {
	sb._lock.RLock()
	defer sb._lock.RUnlock()

	var err error
	if sb.rootNode == 0 && sb._root == nil {
		return nil
	}

	if sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return err
		}
	}

	return sb._root.iterate(filter, callback, 0)
}

// syncDirties shall only be called by Create()/Flag()
func (sb *SuperBlock) syncDirties() error {
	if sb._root == nil || len(sb._dirtyNodes) == 0 {
		// nothing in the tree
		return nil
	}

	buf := bytes.Buffer{}
	buf.Write(sb._snapshot[:])

	for node := range sb._dirtyNodes {
		if node.offset == 0 {
			continue
		}
		buf.Write(node._snapshot[:])
	}

	ext := false
	h := fnv.New128()
	h.Write(buf.Bytes())
	buf.Write(h.Sum(nil))

	binary.BigEndian.PutUint32(sb._mmap[superBlockSize:], uint32(buf.Len()))

	if testCase2 {
		// normally we won't have a fatal error here,
		// let's simulate that copy(sb._mmap[superBlockSize+4:], buf.Bytes()) fails
		panic(testError)
	}

	if buf.Len() <= snapshotSize {
		copy(sb._mmap[superBlockSize+4:], buf.Bytes())
	} else {
		ext = true
		if err := ioutil.WriteFile(sb._filename+".snapshot", buf.Bytes(), 0666); err != nil {
			binary.BigEndian.PutUint32(sb._mmap[superBlockSize:], 0)
			return err
		}
	}

	// we have done writing the master snapshot (WAL)
	// if the above code failed, we are fine because we will directly revertDirties
	// from now on we are entering the critical area,
	// if the belowed code failed, we have to panic,
	// users can recover it, but SuperBlock must not be used anymore.

	nodes := make([]*nodeBlock, 0, len(sb._dirtyNodes))
	for len(sb._dirtyNodes) > 0 {
		for node := range sb._dirtyNodes {
			if !node.areChildrenSynced() {
				continue
			}

			if err := node.sync(); err != nil {
				panic(err)
			}

			nodes = append(nodes, node)
			delete(sb._dirtyNodes, node)
		}
	}

	sb.rootNode = sb._root.offset
	var err error
	if testCase3 || testCase4 {
		err = testError
	} else {
		err = sb.sync()
	}

	if err != nil {
		panic(err)
	}

	// all clear, let's commit the pending snapshots
	for _, node := range nodes {
		node._snapshot = sb._snapshotChPending[node]
		delete(sb._snapshotChPending, node)
	}

	// after that the pending snapshots should be emptied
	if len(sb._snapshotChPending) != 0 {
		panic("shouldn't happen")
	}
	sb._snapshot = sb._snapshotPending
	binary.BigEndian.PutUint64(sb._mmap[superBlockSize:], 0)
	if ext {
		os.Remove(sb._filename + ".snapshot")
	}

	return nil
}

func (sb *SuperBlock) revertDirties() {
	for node := range sb._dirtyNodes {
		node.revertToLastSnapshot()
		delete(sb._dirtyNodes, node)
	}
	sb.revertToLastSnapshot()
}

func (sb *SuperBlock) writeMetadata(key uint128, keystr string, r io.Reader) (Metadata, error) {
	if testCase1 {
		return Metadata{}, testError
	}

	v, err := sb._fd.Seek(sb.tailptr, os.SEEK_SET)
	if err != nil {
		return Metadata{}, err
	}

	var keylen uint16 = uint16(len(keystr))
	if keylen > 8 {
		if _, err := io.Copy(sb._fd, strings.NewReader(keystr)); err != nil {
			return Metadata{}, err
		}
	}

	buf := make([]byte, 32*1024)
	written := int64(0)
	h := crc32.NewIEEE()
	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := sb._fd.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
				h.Write(buf[0:nr])
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
		if testCase5 {
			return Metadata{}, testError
		}
	}
	if err != nil {
		return Metadata{}, err
	}

	p := Metadata{
		key:    key,
		tstamp: uint32(time.Now().Unix()),
		offset: v,
		crc32:  h.Sum32(),
	}

	p.setKeyLen(keylen)
	p.setBufLen(written)

	sb.size += written
	sb.tailptr += written
	if keylen > 8 {
		sb.tailptr += int64(keylen)
	}
	return p, nil
}

func (sb *SuperBlock) Add(key string, value []byte) (err error) {
	return sb.Create(key, bytes.NewReader(value))
}

// Create creates the key and copies the content from r into it
// The max length of the key is 65535, and if it already existed, ErrKeyExisted will be returned
// The max size of the data is 256TB
// Uses of keys shorter than 8 bytes (including 8) are recommended
// This method may panic. If you recover, SueprBlock shall not be used any more
func (sb *SuperBlock) Create(key string, r io.Reader) (err error) {
	sb._lock.Lock()
	defer sb._lock.Unlock()

	if len(key) >= 65536 {
		return ErrKeyTooLong
	}
	if r == nil {
		return ErrKeyNilReader
	}

	defer func() {
		if err != nil {
			sb.revertDirties()
		}
	}()

	if sb.rootNode == 0 && sb._root == nil {
		p, err := sb.writeMetadata(sb.hashString(key), key, r)
		if err != nil {
			return err
		}

		sb._root = sb.newNode()
		sb._root.itemsSize = 1
		sb._root.items[0] = p
		sb._root.markDirty()
		goto SYNC
	}

	if sb.rootNode != 0 && sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return err
		}
	}

	if sb._root.itemsSize >= maxItems {
		item2, second := sb._root.split(maxItems / 2)
		oldroot := sb._root
		sb._root = sb.newNode()
		sb._root.appendItems(item2)
		sb._root.appendChildren(oldroot, second)
		sb._root.markDirty()
	}

	if err := sb._root.insert(sb.hashString(key), key, r); err != nil {
		return err
	}

SYNC:
	sb.count++
	if err := sb.syncDirties(); err != nil {
		return err
	}

	return nil
}

// Flag flags the key using the callback function
// This method may panic. If you recover, SueprBlock shall not be used any more
func (sb *SuperBlock) Flag(key string, callback func(oldFlag uint64) (newFlag uint64)) (uint64, error) {
	sb._lock.Lock()
	defer sb._lock.Unlock()

	var err error
	if sb.rootNode == 0 && sb._root == nil {
		return 0, ErrKeyNotFound
	}

	if sb.rootNode != 0 && sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return 0, err
		}
	}

	node, err := sb._root.getOrFlag(sb.hashString(key), callback)
	if err != nil {
		return 0, err
	}

	if err := sb.syncDirties(); err != nil {
		return 0, err
	}

	return node.flag, nil
}

func (sb *SuperBlock) loadNodeBlock(offset int64) (*nodeBlock, error) {
	var err error
	var nodeHdr [nodeBlockSize]byte

	if offset < int64(sb.mmapSize) {
		copy(nodeHdr[:], sb._mmap[offset:])
	} else {
		_, err = sb._fd.Seek(offset, 0)
		if err != nil {
			return nil, err
		}

		if _, err := io.ReadAtLeast(sb._fd, nodeHdr[:], nodeBlockSize); err != nil {
			return nil, err
		}
	}

	n := &nodeBlock{_super: sb}
	*(*[nodeBlockSize]byte)(unsafe.Pointer(n)) = nodeHdr
	if n.magic != nodeMagic {
		return nil, ErrWrongMagic
	}

	n._snapshot = nodeHdr
	return n, nil
}
