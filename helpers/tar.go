package helpers

import (
	"archive/tar"
	"compress/bzip2"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

// TarBz2ExtractFilesAndGetSha256sum reads a tar.bz2 stream and tries extracting a set of "allowed-files"
// from the archive into destDir while also trying to calculate the Sha256sum of the stream.
// The sha256sum is returned as a hex-encoded string, along with error, if any.
// destDir is created if it does not already exist.
// If there are no errors, tarReader is guaranteed to be read till EOF.
// In case of errors, the state of destDir is unknown.
func TarBz2ExtractFilesAndGetSha256sum(tarReader io.Reader, destDir string, allowedFiles []string) (string, error) {
	logger := GetAppLogger()

	fileIsAllowed := make(map[string]bool)
	for _, flname := range allowedFiles {
		fileIsAllowed[filepath.Join(destDir, flname)] = true
	}

	hasher := sha256.New()
	teeReader := io.TeeReader(tarReader, hasher)

	bz2Decomp := bzip2.NewReader(teeReader)
	tr := tar.NewReader(bz2Decomp)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", logger.ErrorPrintf("failed reading archive: %s", err)
		}

		// if the header is nil, just skip it (not sure how this happens)
		if header == nil {
			continue
		}

		// the target location where the dir/file should be created
		target := filepath.Join(destDir, header.Name)
		if !fileIsAllowed[target] {
			continue
		}
		logger.Printf("[DEBUG] Extracting file %s\n", target)

		// the following switch could also be done using fi.Mode(), not sure if there
		// a benefit of using one vs. the other.
		// fi := header.FileInfo()

		// check the file type
		if header.Typeflag == tar.TypeReg {
			parentDir := filepath.Dir(target)
			if _, err := os.Stat(parentDir); err != nil {
				if err := os.MkdirAll(parentDir, 0755); err != nil {
					return "", logger.ErrorPrintf("could not create dir %s: %s", parentDir, err.Error())
				}
			}

			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return "", logger.ErrorPrintf("could not create file %s: %s", target, err.Error())
			}

			// copy over contents
			if _, err := io.Copy(f, tr); err != nil {
				return "", logger.ErrorPrintf("could not write to file %s: %s", target, err.Error())
			}

			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			f.Close()
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
