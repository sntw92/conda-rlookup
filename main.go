package main

import (
	"conda-rlookup/config"
	"conda-rlookup/helpers"
	"conda-rlookup/indexer"
	"flag"
	"fmt"
	"log"
	"os"
)

const (
	ERR_NONE = iota
	ERR_CONFIG_READ
	ERR_CONFIG_DUMP
	ERR_LOGGER_INIT
	ERR_WORKDIR_CREATE
	ERR_KAFKA_INIT
	ERR_SUBDIR_REPODATA_INDEX
	ERR_KAFKA_DOC_UPDATE
)

func main() {
	var err error

	dumpVersion := flag.Bool("version", false, "Print version information and exit")
	configFile := flag.String("config", "", "Config file in JSON format")
	debug := flag.Bool("debug", false, "Turn on debugging (overrides config file)")
	dumpConfig := flag.Bool("dump-config", false, "Dump all configuration and exit. '--config' supplied config is combined as well.")

	flag.Parse()

	// Version
	if *dumpVersion {
		printVersion()
		os.Exit(ERR_NONE)
	}

	// Read and update config
	if *configFile != "" {
		err := config.ReadConfigFromFile(*configFile)
		if err != nil {
			log.Printf("could not read and parse config file at %s: %s", *configFile, err.Error())
			os.Exit(ERR_CONFIG_READ)
		}
	}

	// Set debugging flag(s) if required
	if *debug {
		config.SetDebugMode(true)
	}

	// Initialize logger
	if err = helpers.InitAppLogger(); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not initialize logger: %s", err.Error())
		os.Exit(ERR_LOGGER_INIT)
	}
	logger := helpers.GetAppLogger()

	// Dump config and exit, if that is requested
	if *dumpConfig {
		if err = config.DumpConfigToStream(os.Stdout); err != nil {
			logger.Printf("[ERROR] could not obtain application config: %s", err.Error())
			os.Exit(ERR_CONFIG_DUMP)
		}
		os.Exit(ERR_NONE)
	}

	appCfg := config.GetAppConfig()

	logger.Printf("[INFO] Ensuring working directory: %s\n", appCfg.Server.Workdir)
	if err = os.MkdirAll(appCfg.Server.Workdir, 0755); err != nil {
		logger.Printf("[ERROR] Could not create working directory %s: %s", appCfg.Server.Workdir, err.Error())
		os.Exit(ERR_WORKDIR_CREATE)
	}

	logger.Printf("[INFO] Intitiating Kafka Writer\n")
	if err = indexer.InitKafkaWriter(&appCfg.Kafka); err != nil {
		logger.Printf("Error intitializing kafka writer: %s", err.Error())
		os.Exit(ERR_KAFKA_INIT)
	}

	localSrc := helpers.LocalFileSource{
		TempDir:              "/tmp",
		RepodataLockFilename: "",
		SourceDir:            appCfg.Server.Path,
	}

	for _, ch := range appCfg.Server.Channels {
		logger.Printf("[INFO] Started Processing conda-channel: %s", ch.RelativeLocation)
		for _, subdir := range ch.Subdirs {
			logger.Printf("[INFO] Started Processing subdirectory: %s", subdir.RelativeLocation)
			err := indexer.IndexSubdir(subdir, appCfg.Server.Workdir, "conda-master", &localSrc)
			if err != nil {
				logger.Printf("[ERROR] In indexing subdirectory %s: %s", subdir.RelativeLocation, err.Error())
			}
			if err = indexer.SubdirFlushToKafka(subdir, appCfg.Server.Workdir); err != nil {
				logger.Printf("[ERROR] In pushing stats to kafka for subdir %s: %s", subdir.RelativeLocation, err.Error())
			}
			logger.Printf("[INFO] Finished Processing subdirectory: %s", subdir.RelativeLocation)
		}
		logger.Printf("[INFO] Finished Processing conda-channel: %s", ch.RelativeLocation)
	}
}

func printVersion() {
	version := config.GetVersion()
	fmt.Printf("Name: %s, Version: %s, GitCommitSha: %s, BuildTime: %s, BuildHost: %s, BuildUser: %s\n",
		"conda-rlookup-indexer",
		version.Version,
		version.GitCommitSha,
		version.BuildTime,
		version.BuildHost,
		version.BuildUser)
}
