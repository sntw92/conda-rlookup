package helpers

import (
	"archive/tar"
	"compress/bzip2"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// TarBz2ExtractFilesAndGetChecksum reads a tar.bz2 stream and tries extracting a set of "allowed-files"
// from the archive into destDir while also trying to calculate the checksum (sha256 or md5) of the stream.
// The checksum is returned as a hex-encoded string, along with error, if any.
// destDir is created if it does not already exist.
// If there are no errors, srcReader is guaranteed to be read till EOF.
// In case of errors, the state of destDir is unknown.
func TarBz2ExtractFilesAndGetChecksum(srcReader io.Reader, destDir string, allowedFiles []string, checksumType string) (string, error) {
	logger := GetAppLogger()

	fileIsAllowed := make(map[string]bool)
	for _, flname := range allowedFiles {
		fileIsAllowed[filepath.Join(destDir, flname)] = true
	}

	var hasher hash.Hash
	switch strings.ToLower(checksumType) {
	case "md5", "md5sum":
		hasher = md5.New()
	case "sha256", "sha", "shasum", "sha256sum":
		hasher = sha256.New()
	default:
		return "", fmt.Errorf("Unknown checksum type %s: must be one of {sha256, md5}", checksumType)
	}

	// Legend:
	//   NAME provides an io.Reader interface: (NAME)=>----
	//   NAME provides an io.Writer interface: ----=>(NAME)
	// Description:
	//   The graph below shows the data flow: As we read from (tr) in the process of extracting files
	//   the decompression happens in (bz2Decomp), the (teeReader) provides this data to (bz2Decomp)
	//   by reading from (tarReader) and writing the same content into (hasher). (tarReader) could have
	//   a disk-file or a network socket (underlying http, for example) as the backing source.
	//   At the very end we must "drain" all of (tarReader) into (hasher) by pulling on (teeReader) so
	//   that we calculate the checksum accurately.
	// Graph:
	//   (tarReader)=>----(teeReader)=>------(bz2Decomp)=>-----(tr)=>---
	//                         |
	//                         +---=>(hasher)
	teeReader := io.TeeReader(srcReader, hasher)
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

	// This is necessary to drain the ENTIRE tarbz2 file into the hasher
	// so that the correct checksum is calculated
	if _, err := io.Copy(ioutil.Discard, teeReader); err != nil {
		return "", logger.ErrorPrintf("Unable to read file fully for hashing: %s", err.Error())
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}
