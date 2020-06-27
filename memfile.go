package stud

type memFile struct {
	buf    []byte
	cursor int64
}

func (f *memFile) Seek(pos int64, w int) (int64, error) {
	switch w {
	case 0:
		f.cursor = pos
	case 1:
		f.cursor += pos
	case 2:
		f.cursor = int64(len(f.buf)) + pos
	}
	return f.cursor, nil
}

func (f *memFile) WriteAt(p []byte, pos int64) (int, error) {
	f.Seek(pos, 0)
	return f.Write(p)
}

func (f *memFile) Write(p []byte) (int, error) {
	f.cursor += int64(len(p))
	return copy(f.buf[f.cursor-int64(len(p)):], p), nil
}

func (f *memFile) ReadAt(p []byte, pos int64) (int, error) {
	f.Seek(pos, 0)
	return f.Read(p)
}

func (f *memFile) Read(p []byte) (int, error) {
	f.cursor += int64(len(p))
	return copy(p, f.buf[f.cursor-int64(len(p)):]), nil
}

func (f *memFile) Sync() error { return nil }

func (f *memFile) Close() error { return nil }
