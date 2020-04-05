package model

import (
	"bufio"
	"conda-rlookup/helpers"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type CondaRepodata struct {
	Packages map[string]CondaPackage `json:"packages"`
}

type CondaPackage map[string]interface{}

func (s *Subdir) Index(prefixDir string, svrName string, src CondaChannelFileSource) error {
	histRepodataFilename := filepath.Join(prefixDir, s.RelativeLocation, "repodata.json.history")
	workDir := filepath.Join(prefixDir, s.RelativeLocation)

	repodataTempFile, err := ioutil.TempFile(workDir, ".repodata.json.new.*")
	if err != nil {
		return fmt.Errorf("could not open repodata temp file: %s", err.Error())
	}
	cleanupTempFile := true
	defer func() {
		if cleanupTempFile {
			tmpFilename := repodataTempFile.Name()
			repodataTempFile.Close()
			os.Remove(tmpFilename)
		}
	}()

	histRepodataFile, err := os.OpenFile(histRepodataFilename, os.O_RDONLY, 0755)
	if err != nil {
		return fmt.Errorf("could not open repodata history file %s for reading: %s",
			histRepodataFilename, err.Error())
	}
	defer histRepodataFile.Close()

	histRepodata, err := readCondaRepodata(histRepodataFile)
	if err != nil {
		return fmt.Errorf("could not read repodata histroic data from file %s: %s", histRepodataFilename, err.Error())
	}
	histRepodataFile.Close()

	curRepodataReader, err := src.GetFileReadCloser(filepath.Join(s.RelativeLocation, "repodata.json"))
	if err != nil {
		return fmt.Errorf("could not read current repodata: %s", err.Error())
	}
	curRepodata, err := readCondaRepodata(curRepodataReader)
	if err != nil {
		return fmt.Errorf("could not read repodata current data: %s", err.Error())
	}

	successRepodata := CondaRepodata{Packages: make(map[string]CondaPackage)}

	for name, pkg := range curRepodata.Packages {
		var oldpkgSha, newpkgSha string
		var updateRequired bool
		var ok bool

		if newpkgSha, ok = pkg["sha256"].(string); !ok {
			log.Printf("[ERROR] Could not get SHA256sum for %s", name)
			continue
		}

		if oldpkg, ok := histRepodata.Packages[name]; ok {
			if oldpkgSha, ok = oldpkg["sha256"].(string); !ok {
				log.Printf("[ERROR] Could not get historical SHA256sum for %s", name)
				updateRequired = true
			} else if oldpkgSha != newpkgSha {
				updateRequired = true
			}
		} else {
			updateRequired = true
		}

		if updateRequired {
			pkgFilename := filepath.Join(s.RelativeLocation, name)
			newTarFile, err := src.GetFileReadCloser(pkgFilename)
			if err != nil {
				log.Printf("[ERROR] Could not fetch package %s: %s", pkgFilename, err.Error())
				continue
			}
			tarFileDir := filepath.Join(workDir, name)
			id := filepath.Join(svrName, s.RelativeLocation, name)
			err = extractPackageAndGenerateMetadataDocument(newTarFile, tarFileDir, id, newpkgSha, pkg)
			if err != nil {
				log.Printf("[ERROR] Could not fetch and extract metadata for %s: %s", name, err.Error())
				continue
			}
			//TODO: Upload metadata to Kafka
			successRepodata.Packages[name] = pkg
		}
	}

	for name := range histRepodata.Packages {
		if _, ok := curRepodata.Packages[name]; !ok {
			log.Printf("[INFO] Deleting package %s", name)
			//TODO: Delete package from kafka here
		}
	}

	encoder := json.NewEncoder(repodataTempFile)
	err = encoder.Encode(successRepodata)
	if err != nil {
		return fmt.Errorf("could not write success data to new histpry file: %s", err.Error())
	}
	repodataTempFile.Close()

	err = os.Rename(repodataTempFile.Name(), histRepodataFilename)
	if err != nil {
		return fmt.Errorf("could not update histrorical repodata file: %s", err.Error())
	}
	cleanupTempFile = false

	return nil
}

func readCondaRepodata(r io.Reader) (*CondaRepodata, error) {
	decoder := json.NewDecoder(r)
	var res CondaRepodata
	err := decoder.Decode(&res)
	if err != nil {
		return nil, fmt.Errorf("could not read and parse repodata: %s", err.Error())
	}
	return &res, nil
}

func extractPackageAndGenerateMetadataDocument(r io.Reader, prefixDir string, id string, expectedSha256sum string, repodata CondaPackage) error {
	allowedFiles := []string{
		"info/about.json",
		"info/index.json",
		"info/files",
		"info/paths.json",
	}
	actualSha256sum, err := helpers.TarBz2ExtractFilesAndGetSha256sum(r, prefixDir, allowedFiles)
	if err != nil {
		return fmt.Errorf("could not extract package: %s", err.Error())
	}
	if expectedSha256sum != "" && actualSha256sum != expectedSha256sum {
		return fmt.Errorf("sha256sum mismatch: actual %s vs expected %s", actualSha256sum, expectedSha256sum)
	}

	// Generate MetadataDocument
	res := make(map[string]interface{})
	for k, v := range repodata {
		res[k] = v
	}
	res["id"] = id

	pathsJsonFilename := filepath.Join(prefixDir, "info/paths.json")
	pathsJson, pathsErr := readJsonFromFile(pathsJsonFilename)

	filesFilename := filepath.Join(prefixDir, "info/files")
	filesJson, filesErr := readLinesIntoJsonArray(filesFilename, "files")

	if filesErr != nil {
		if pathsErr == nil {
			var lines []string
			for _, v := range pathsJson["path"].([]map[string]string) {
				lines = append(lines, v["_path"])
			}
			filesJson = make(map[string]interface{})
			filesJson["files"] = lines
			filesErr = nil
		} else {
			return fmt.Errorf("could not parse both of info/files and info/paths.json")
		}
	}

	aboutJsonFilename := filepath.Join(prefixDir, "info/about.json")
	aboutJson, aboutErr := readJsonFromFile(aboutJsonFilename)

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
		return fmt.Errorf("could not open/create metadata.json file for writing: %s", err.Error())
	}
	metadataFile.Close()

	encoder := json.NewEncoder(metadataFile)
	err = encoder.Encode(res)
	if err != nil {
		return fmt.Errorf("could not dump metadata as json to file: %s", err.Error())
	}

	return nil
}

func readJsonFromFile(filename string) (map[string]interface{}, error) {
	var res map[string]interface{}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not open file %s for reading json data: %s", filename, err.Error())
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	if err = decoder.Decode(&res); err != nil {
		return nil, fmt.Errorf("error decoding json")
	}

	return res, nil
}

func readLinesIntoJsonArray(filename string, key string) (map[string]interface{}, error) {
	res := make(map[string]interface{})

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not open file %s for reading json data: %s", filename, err.Error())
	}
	defer f.Close()

	var lines []string
	var builder strings.Builder
	rdr := bufio.NewReader(f)

	for {
		data, more, _ := rdr.ReadLine()
		if data == nil {
			break
		}
		builder.Write(data)
		if !more {
			lines = append(lines, builder.String())
			builder.Reset()
		}
	}

	res[key] = lines
	return res, nil
}
