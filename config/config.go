package config

import (
	"conda-rlookup/domain"
	"conda-rlookup/utils"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/imdario/mergo"
)

// Version details
var (
	Version      = "No Version Provided"
	GitCommitSha = "Unknown"

	BuildHost = "Unknown"
	BuildTime = "Unknown"
	BuildUser = "Unknown"
)

func GetVersion() *utils.VersionDetails {
	return &utils.VersionDetails{
		Version:      Version,
		GitCommitSha: GitCommitSha,

		BuildHost: BuildHost,
		BuildTime: BuildTime,
		BuildUser: BuildUser,
	}
}

type AppConfig struct {
	Server domain.CondaServer `json:"server"`
	Kafka  KafkaWriterConfig  `json:"kafka"`
	Debug  string             `json:"debug"`
}

func SetDebugMode(val bool) {
	appCfg.Debug = strconv.FormatBool(val)
}

func IsModeDebug() bool {
	return strings.ToLower(appCfg.Debug) == "true"
}

var appCfg = AppConfig{
	Debug: "false",
	Server: domain.CondaServer{
		Name: "conda-master",
		Url:  "",
		Path: "conda-forge",

		Workdir:  "workdir",
		Channels: map[string]domain.Channel{},
	},
	Kafka: KafkaWriterConfig{},
}

func SetAppConfig(cfg *AppConfig) error {
	err := mergo.Merge(cfg, appCfg)
	appCfg = *cfg

	return err
}

func GetAppConfig() AppConfig {
	return appCfg
}

func ReadConfigFromFile(filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", filename)
	}

	configFile, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return fmt.Errorf("could not open file %s for reading: %s", filename, err.Error())
	}
	defer configFile.Close()

	jsonDecoder := json.NewDecoder(configFile)
	var cfgData AppConfig
	err = jsonDecoder.Decode(&cfgData)
	if err != nil {
		return fmt.Errorf("could not parse file %s for config data: %s", filename, err.Error())
	}

	// Merge configuration
	return SetAppConfig(&cfgData)
}

// DumpConfigToFile writes application config data to a file as prettified JSON.
// The file will be created if it does not exist and its contents truncated if it does.
// It can be used to export config to a file which can then be imported.
func DumpConfigToFile(filename string) error {
	outputFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("could not open file %s for writing config: %s", filename, err.Error())
	}
	defer outputFile.Close()
	return DumpConfigToStream(outputFile)
}

// DumpConfigToStream write application config as prettified JSON to an open File stream
// It does not attempt to close the filestream.
// It can be used to dump application configuration to console OR to log files for debugging
func DumpConfigToStream(f io.Writer) error {
	jsonPrettyData, err := DumpConfigAsPrettyJson()
	if err != nil {
		return fmt.Errorf("could not marshal application config as JSON: %s", err.Error())
	}
	if _, err = f.Write(jsonPrettyData); err != nil {
		return fmt.Errorf("could not write config data to file/stream: %s", err.Error())
	}

	return nil
}

func DumpConfigAsPrettyJson() ([]byte, error) {
	return json.MarshalIndent(appCfg, "", "  ")
}
