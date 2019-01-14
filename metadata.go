package stud

import (
	"fmt"
	"unsafe"
)

const itemSize = 48

type Metadata struct {
	key    uint128
	next   int64 // linked list
	offset int64
	size   uint64 // 16bit key length + 48bit data length
	crc32  uint32
	flag   uint32
}

func (m *Metadata) Pos() (int64, int64) {
	if m.KeyLen() > 8 {
		return m.offset + int64(m.KeyLen()), m.Len()
	}
	return m.offset, m.Len()
}

func (m *Metadata) ShortName() string {
	ln := m.KeyLen()
	x := *(*[16]byte)(unsafe.Pointer(&m.key))
	if ln > 8 {
		// the key is stored elsewhere, for performance reason we won't read them
		return fmt.Sprintf("%x", x)
	}
	keybuf := make([]byte, ln)
	copy(keybuf, x[:ln])
	return string(keybuf)
}

func (m *Metadata) KeyLen() uint16 { return uint16(m.size >> 48) }

func (m *Metadata) Len() int64 { return int64(m.size & 0x0000ffffffffffff) }

func (m *Metadata) Flag() uint32 { return m.flag }

func (m *Metadata) Crc32() uint32 { return m.crc32 }

func (m *Metadata) setKeyLen(ln uint16) {
	m.size &= 0x0000ffffffffffff
	m.size |= uint64(ln) << 48
}

func (m *Metadata) setBufLen(ln int64) {
	m.size &= 0xffff000000000000
	m.size |= uint64(ln) & 0x0000ffffffffffff
}
