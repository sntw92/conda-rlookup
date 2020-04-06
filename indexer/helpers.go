package indexer

import (
	"bufio"
	"conda-rlookup/domain"
	"conda-rlookup/helpers"
	"encoding/json"
	"io"
	"os"
	"strings"
)

func readJsonFromFile(filename string) (map[string]interface{}, error) {
	logger := helpers.GetAppLogger()
	var res map[string]interface{}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, logger.ErrorPrintf("could not open file %s for reading json data: %s", filename, err.Error())
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	if err = decoder.Decode(&res); err != nil {
		return nil, logger.ErrorPrintf("error decoding json")
	}

	return res, nil
}

func readLinesIntoJsonArray(filename string, key string) (map[string]interface{}, error) {
	logger := helpers.GetAppLogger()
	res := make(map[string]interface{})

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, logger.ErrorPrintf("could not open file %s for reading json data: %s", filename, err.Error())
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

func readInRepodataFile(filename string) (*domain.CondaRepodata, error) {
	logger := helpers.GetAppLogger()

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		logger.Printf("[INFO] Repodata file %s does not exist. Creating an empty one.", filename)

		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			return nil, logger.ErrorPrintf("could not open/create historic repodata file: %s", err.Error())
		}
		defer f.Close()

		res := domain.CondaRepodata{
			Packages: make(map[string]domain.CondaPackage),
		}

		if err = json.NewEncoder(f).Encode(res); err != nil {
			return nil, logger.ErrorPrintf("could not write empty conda repodata to historic repodata file: %s", err.Error())
		}

		return &res, nil
	}

	logger.Printf("[DEBUG] Opening repodata file: %s", filename)
	repodataFile, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, logger.ErrorPrintf("could not open repodata file %s for reading: %s", filename, err.Error())
	}
	defer repodataFile.Close()

	logger.Printf("[DEBUG] Reading repodata from file: %s", filename)
	repodata, err := readCondaRepodata(repodataFile)
	if err != nil {
		return nil, logger.ErrorPrintf("could not read repodata from file %s: %s", filename, err.Error())
	}

	return repodata, nil
}

func readInRepodataFromSource(fileref string, src domain.CondaChannelFileSource) (*domain.CondaRepodata, error) {
	logger := helpers.GetAppLogger()

	repodataReader, err := src.GetFileReadCloser(fileref)
	if err != nil {
		return nil, logger.ErrorPrintf("could not read repodata %s: %s", fileref, err.Error())
	}
	defer repodataReader.Close()

	repodata, err := readCondaRepodata(repodataReader)
	if err != nil {
		return nil, logger.ErrorPrintf("could not read repodata %s: %s", fileref, err.Error())
	}
	return repodata, nil
}

func readCondaRepodata(r io.Reader) (*domain.CondaRepodata, error) {
	logger := helpers.GetAppLogger()

	var res domain.CondaRepodata
	if err := json.NewDecoder(r).Decode(&res); err != nil {
		return nil, logger.ErrorPrintf("could not read and parse repodata: %s", err.Error())
	}

	return &res, nil
}
