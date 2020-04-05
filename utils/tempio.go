package utils

import (
	"io"
	"os"
)

type tempFileStruct struct {
	f *os.File
}

func (t tempFileStruct) Read(p []byte) (n int, err error) {
	return t.f.Read(p)
}

func (t tempFileStruct) Close() error {
	defer os.Remove(t.f.Name())
	return t.f.Close()
}

func NewTempFileReadCloser(f *os.File) io.ReadCloser {
	return tempFileStruct{f}
}
