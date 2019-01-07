package stud

import (
	"hash/crc32"
	"io/ioutil"
	"os"
)

// Open opens the Stream of the key
// if it is not found, error would be ErrKeyNotFound
// if it is found, Stream must be closed after used
// Don't write "_, err := sb.Open()"
func (sb *SuperBlock) Open(key string) (*Stream, error) {
	sb._lock.RLock()
	defer sb._lock.RUnlock()

	load := func(node Metadata) (*Stream, error) {
		d := &Stream{_super: sb}
		if err := d.open(); err != nil {
			return nil, err
		}

		if _, err := d._fd.Seek(node.offset, 0); err != nil {
			return nil, err
		}

		if node.KeyLen() > 8 {
			ln := int64(node.KeyLen())
			if _, err := d._fd.Seek(ln, os.SEEK_CUR); err != nil {
				return nil, err
			}
		}

		d.h = crc32.NewIEEE()
		d.Metadata = node
		d.remaining = int(node.BufLen())
		return d, nil
	}

	var err error
	if sb.rootNode == 0 && sb._root == nil {
		return nil, ErrKeyNotFound
	}

	if sb.rootNode != 0 && sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return nil, err
		}
	}

	node, err := sb._root.getOrFlag(sb.hashString(key), nil)
	if err != nil {
		return nil, err
	}

	//	sb._cache.Add(key, node)
	return load(node)
}

// Get returns bytes slice directly
// Call it only when you know that the result is small enough to fit in memory because they will be cached
// to improve speed
func (sb *SuperBlock) Get(key string) ([]byte, error) {
	if sb._cacheBytes != nil {
		if v, ok := sb._cacheBytes.Get(key); ok {
			return v.([]byte), nil
		}
	}

	s, err := sb.Open(key)
	if err != nil {
		return nil, err
	}

	buf, err := ioutil.ReadAll(s)
	if err != nil {
		return nil, err
	}

	if sb._cacheBytes != nil {
		sb._cacheBytes.AddWeight(key, buf, int64(len(buf)))
	}

	return buf, nil
}
