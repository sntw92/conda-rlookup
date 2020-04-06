package helpers

import (
	"conda-rlookup/utils"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

type LocalFileSource struct {
	TempDir              string
	RepodataLockFilename string

	SourceDir string
}

func (l *LocalFileSource) Init() error {
	logger := GetAppLogger()

	if l == nil {
		return logger.ErrorPrintf("called on a nil struct!")
	}

	if err := os.MkdirAll(l.TempDir, 0755); err != nil {
		return logger.ErrorPrintf("could not create tempdir %s: %s", l.TempDir, err.Error())
	}
	return nil
}

func (l *LocalFileSource) GetFileReadCloser(relativeFilepath string) (io.ReadCloser, error) {
	logger := GetAppLogger()

	if l == nil {
		return nil, logger.ErrorPrintf("called on a nil struct!")
	}

	var err error
	baseFilename := filepath.Base(relativeFilepath)
	parentDir := filepath.Dir(relativeFilepath)

	if baseFilename == "repodata.json" {
		// Create a temporary file for writing repodata to
		tmpFile, err := ioutil.TempFile(l.TempDir, ".tmp.repodata.json.*")
		if err != nil {
			return nil, logger.ErrorPrintf("could not create tempfile for copying repodata.json: %s", err.Error())
		}

		// cleanup the temporary file on errors
		cleanupTempFile := true
		defer func() {
			if cleanupTempFile {
				tmpFilename := tmpFile.Name()
				tmpFile.Close()
				os.Remove(tmpFilename)
			}
		}()

		if l.RepodataLockFilename != "" { // Take locks for copying to TempDir
			lockFilename := filepath.Join(l.SourceDir, parentDir, l.RepodataLockFilename)
			filelock := flock.New(lockFilename)

			logger.Printf("[DEBUG] Trying to acquire lock on file: %s", lockFilename)
			lockCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			locked, err := filelock.TryLockContext(lockCtx, 678*time.Millisecond)
			if err != nil {
				return nil, logger.ErrorPrintf("could not acquire lock on lockfile %s for reading %s: %s",
					lockFilename, relativeFilepath, err.Error())
			}

			if !locked {
				return nil, logger.ErrorPrintf("could not acquire lock on lockfile %s for reading %s",
					lockFilename, relativeFilepath)
			}
			//nolint:errcheck
			defer filelock.Unlock()
			logger.Printf("[DEBUG] Acquired lock on file: %s", lockFilename)
		}

		repodataFilename := filepath.Join(l.SourceDir, relativeFilepath)
		repodataFile, err := os.OpenFile(repodataFilename, os.O_RDONLY, 0755)
		if err != nil {
			return nil, logger.ErrorPrintf("could not open file %s for reading: %s", repodataFilename, err.Error())
		}
		if _, err = io.Copy(tmpFile, repodataFile); err != nil {
			return nil, logger.ErrorPrintf("could not copy repodata.json to tempfile %s: %s", tmpFile.Name(), err.Error())
		}
		if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
			return nil, logger.ErrorPrintf("could not rewind file %s: %s", tmpFile.Name(), err.Error())
		}

		cleanupTempFile = false
		return utils.NewTempFileReadCloser(tmpFile), nil
	}

	targetFilename := filepath.Join(l.SourceDir, relativeFilepath)
	f, err := os.OpenFile(targetFilename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, logger.ErrorPrintf("could not open file %s for reading: %s", targetFilename, err.Error())
	}

	return f, nil
}
