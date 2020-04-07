package indexer

import (
	"conda-rlookup/domain"
	"conda-rlookup/helpers"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/google/renameio"
)

// IndexSubdir is used for indexing a conda repository subdir i.e. generating a cache of some files in info/
// directory of packages, creating a metadata file for the reverse lookup index, and keeping a history of
// this indexing in a repodata history file that is used to make incremental updates possible without needing
// to re-index everything.
// The working directory is assumed to be prefixDir. In this subtree, the RelativeLocation of the subdir
// is used further to segment the cache. svrName is the server-name that is prepended to the "id" that is
// populated in the metadata, and src could be either a local or a remote file source for fetching repodata files
// and packages files.
func IndexSubdir(s domain.Subdir, prefixDir string, svrName string, src domain.CondaChannelFileSource) error {
	logger := helpers.GetAppLogger()

	// Create Working directory, if required
	workDir := filepath.Join(prefixDir, s.RelativeLocation)
	err := os.MkdirAll(workDir, 0755)
	if err != nil {
		return logger.ErrorPrintf("could not create workdir at %s for conda-channel-subdir: %s",
			workDir, err.Error())
	}

	//TODO: Make historic repodata filename configurable
	histRepodataFilename := filepath.Join(workDir, "repodata.json.history")
	curKafkadocsFilename := filepath.Join(workDir, "kafkadocs.json")

	// repodataTempFile is used for writing the incremental updates to history file.
	// Once the indexing is complete this file is simply rename to the repodata histroy file.
	// In case of fatal errors, the original repodata-history file is left as is and this file is purged.
	repodataTempFile, err := renameio.TempFile("", histRepodataFilename)
	if err != nil {
		return logger.ErrorPrintf("could not open repodata temp file: %s", err.Error())
	}
	//nolint:errcheck
	defer repodataTempFile.Cleanup()

	kafkadocsTempFile, err := renameio.TempFile("", curKafkadocsFilename)
	if err != nil {
		return logger.ErrorPrintf("could not open kafkadocs temp file: %s", err.Error())
	}
	//nolint:errcheck
	defer kafkadocsTempFile.Cleanup()

	// Get the historic repodata
	histRepodata, err := readInRepodataFile(histRepodataFilename)
	if err != nil {
		return logger.ErrorPrintf("could not read in historic repodata file %s: %s", histRepodataFilename, err.Error())
	}

	// Get the current repodata reader file
	curRepodataLocation := filepath.Join(s.RelativeLocation, "repodata.json")
	curRepodata, err := readInRepodataFromSource(curRepodataLocation, src)
	if err != nil {
		return logger.ErrorPrintf("could not read in current repodata %s: %s", curRepodataLocation, err.Error())
	}

	curKafkadocs, err := readInKafkadocsFile(curKafkadocsFilename)
	if err != nil {
		return logger.ErrorPrintf("could not read in kafkadocs file %s: %s", curKafkadocsFilename, err.Error())
	}

	// Start with a black success state; add no-ops and successful updates as we progress
	successRepodata := domain.CondaRepodata{Packages: make(map[string]domain.CondaPackage)}

	// Statistics
	var nOldPackages, nCurPackages, nSkipped, nUpdated, nDeleted, nFailed, nUpToDate int
	nOldPackages = len(histRepodata.Packages)
	nCurPackages = len(curRepodata.Packages)

	// Do updates on current - historic
	for name, pkg := range curRepodata.Packages {
		var oldpkgSha, newpkgSha string
		var updateRequired bool
		var ok bool

		// TODO: Add ways to handle packages that do not specify a SHA256sum?
		if newpkgSha, ok = pkg["sha256"].(string); !ok {
			log.Printf("[ERROR] Could not get SHA256sum for %s", name)
			nSkipped += 1
			continue
		}

		if oldpkg, ok := histRepodata.Packages[name]; ok {
			if oldpkgSha, ok = oldpkg["sha256"].(string); !ok {
				log.Printf("[ERROR] Could not get historical SHA256sum for %s", name)
				updateRequired = true // Older one doesn't have sha256sum, newer one does. Got to update!
			} else if oldpkgSha != newpkgSha {
				updateRequired = true // The sha256sum got chaged. Package override requires update!
			}
		} else {
			updateRequired = true // New package was added. Must add it in our index!
		}

		if updateRequired {
			nFailed += 1 // Assume it'll fail. If we reach the end and it doesn't decrement the count then

			pkgFilename := filepath.Join(s.RelativeLocation, name)
			logger.Printf("[INFO] Updating package: %s", pkgFilename)
			newTarFile, err := src.GetFileReadCloser(pkgFilename)
			if err != nil {
				log.Printf("[ERROR] Could not fetch package %s: %s", pkgFilename, err.Error())
				continue
			}
			tarFileDir := filepath.Join(workDir, name)
			id := filepath.Join(svrName, s.RelativeLocation, name)
			metadataSha256, err := extractPackageAndGenerateMetadataDocument(newTarFile, tarFileDir, id, newpkgSha, pkg, s.ExtraData)
			newTarFile.Close()
			if err != nil {
				log.Printf("[ERROR] Could not fetch and extract metadata for %s: %s", name, err.Error())
				continue
			}
			curKafkadocs.Docs[id] = domain.KafkadocEntry{
				Path:   filepath.Join(name, "metadata.json"),
				Sha256: metadataSha256,
			}
			logger.Printf("[INFO] Successfully Updated package: %s", pkgFilename)
			nUpdated += 1
			nFailed -= 1
			successRepodata.Packages[name] = pkg
		} else {
			logger.Printf("[INFO] Package %s is already up-to-date", filepath.Join(s.RelativeLocation, name))
			nUpToDate += 1
			successRepodata.Packages[name] = pkg
		}
	}

	// Delete files in: historic - current
	for name := range histRepodata.Packages {
		if _, ok := curRepodata.Packages[name]; !ok {
			pkgLocationDir := filepath.Join(workDir, s.RelativeLocation, name)
			if _, err = os.Stat(pkgLocationDir); !os.IsNotExist(err) {
				os.RemoveAll(pkgLocationDir)
			}
			nDeleted += 1
			logger.Printf("[INFO] Deleting package: %s", filepath.Join(s.RelativeLocation, name))

			id := filepath.Join(svrName, s.RelativeLocation, name)
			curKafkadocs.Docs[id] = domain.KafkadocEntry{
				Path:   "",
				Sha256: "",
			}
		}
	}

	if err = json.NewEncoder(repodataTempFile).Encode(successRepodata); err != nil {
		return logger.ErrorPrintf("could not write success data to new history file: %s", err.Error())
	}

	if err = repodataTempFile.CloseAtomicallyReplace(); err != nil {
		return logger.ErrorPrintf("could not update histrorical repodata file: %s", err.Error())
	}

	logger.Printf("[INFO] Summary for %s: (Old -> New) = (%d -> %d), Updated = %d, Deleted = %d, Failed = %d, Skipped = %d, Up-to-date = %d",
		s.RelativeLocation, nOldPackages, nCurPackages, nUpdated, nDeleted, nFailed, nSkipped, nUpToDate)

	if err = json.NewEncoder(kafkadocsTempFile).Encode(curKafkadocs); err != nil {
		return logger.ErrorPrintf("could not write to current kafkadocs file: %s", err.Error())
	}

	if err = kafkadocsTempFile.CloseAtomicallyReplace(); err != nil {
		return logger.ErrorPrintf("could not update current kafkadocs file: %s", err.Error())
	}

	return nil
}

func extractPackageAndGenerateMetadataDocument(r io.Reader, prefixDir string, id string,
	expectedSha256sum string, repodata domain.CondaPackage, extraData map[string]interface{}) (string, error) {
	logger := helpers.GetAppLogger()
	allowedFiles := []string{
		"info/about.json",
		"info/index.json",
		"info/files",
		"info/paths.json",
	}
	actualSha256sum, err := helpers.TarBz2ExtractFilesAndGetSha256sum(r, prefixDir, allowedFiles)
	if err != nil {
		return "", logger.ErrorPrintf("could not extract package: %s", err.Error())
	}
	if expectedSha256sum != "" && actualSha256sum != expectedSha256sum {
		return "", logger.ErrorPrintf("sha256sum mismatch: actual %s vs expected %s", actualSha256sum, expectedSha256sum)
	}

	// Generate MetadataDocument
	res := make(map[string]interface{})
	for k, v := range repodata {
		res[k] = v
	}
	if len(extraData) > 0 {
		for k, v := range extraData {
			res[k] = v
		}
	}
	res["id"] = id

	pathsJsonFilename := filepath.Join(prefixDir, "info/paths.json")
	pathsJson, pathsErr := readJsonFromFile(pathsJsonFilename)

	filesFilename := filepath.Join(prefixDir, "info/files")
	filesJson, filesErr := readLinesIntoJsonArray(filesFilename, "files")

	if filesErr != nil {
		if pathsErr == nil {
			if pathsData, ok := pathsJson["paths"]; ok {
				if pathsArr, ok := pathsData.([]interface{}); ok {
					filesJson = make(map[string]interface{})
					filesJson["files"] = arrayOfObjectsToArrayOfStrings(pathsArr, "_path")
					filesErr = nil
				}
			}
		} else {
			return "", logger.ErrorPrintf("could not parse both of info/files and info/paths.json")
		}
	}

	aboutJsonFilename := filepath.Join(prefixDir, "info/about.json")
	aboutJson, aboutErr := readJsonFromFile(aboutJsonFilename)

	// Convert root_pkgs to an array of strings
	if aboutJsonRootPkgs, ok := aboutJson["root_pkgs"]; ok {
		if aboutJsonRootPkgsArr, ok := aboutJsonRootPkgs.([]interface{}); ok {
			aboutJson["root_pkgs"] = arrayOfObjectsToArrayOfStrings(aboutJsonRootPkgsArr, "dist_name")
		}
	}

	if pathsErr == nil {
		res["paths"] = pathsJson["paths"]
	}

	if filesErr == nil {
		res["files"] = filesJson["files"]
	}

	if aboutErr == nil {
		res["about"] = aboutJson
	}

	metadataFilename := filepath.Join(prefixDir, "metadata.json")
	metadataFile, err := os.OpenFile(metadataFilename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return "", logger.ErrorPrintf("could not open/create metadata.json file for writing: %s", err.Error())
	}
	defer metadataFile.Close()

	hasher := sha256.New()
	mw := io.MultiWriter(metadataFile, hasher)

	if err = json.NewEncoder(mw).Encode(res); err != nil {
		return "", logger.ErrorPrintf("could not dump metadata as json to file: %s", err.Error())
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// arrayOfObjectsToArrayOfStrings walks through an array of objects and
// tries extracting field as a string in each element. If that object cannot be parsed,
// it is ignored. The order is preserved. If field is not present in any of the elements,
// an empty array is returned. This function never returns nil.
func arrayOfObjectsToArrayOfStrings(arrObj []interface{}, field string) []string {
	res := []string{}
	for _, v := range arrObj {
		if obj, objIsMap := v.(map[string]interface{}); objIsMap {
			if val, fieldIsPresent := obj[field]; fieldIsPresent {
				if s, valIsString := val.(string); valIsString {
					res = append(res, s)
				}
			}
		} else if s, objIsString := v.(string); objIsString {
			res = append(res, s)
		}
	}

	return res
}
