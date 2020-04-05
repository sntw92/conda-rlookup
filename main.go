package main

import (
	"conda-rlookup/config"
	"conda-rlookup/helpers"
	"os"
)

func main() {
	helpers.InitAppLogger()
	logger := helpers.GetAppLogger()

	appCfg := config.GetAppConfig()

	logger.Printf("[INFO] Ensuring working directory: %s\n", appCfg.Server.Workdir)
	os.MkdirAll(appCfg.Server.Workdir, 0755)

	localSrc := helpers.LocalFileSource{
		TempDir:              "/tmp",
		RepodataLockFilename: "",
		SourceDir:            appCfg.Server.Path,
	}

	for _, ch := range appCfg.Server.Channels {
		logger.Printf("[INFO] Started Processing conda-channel: %s", ch.RelativeLocation)
		for _, subdir := range ch.Subdirs {
			logger.Printf("[INFO] Started Processing subdirectory: %s", subdir.RelativeLocation)
			err := subdir.Index(appCfg.Server.Workdir, "conda-master", &localSrc)
			if err != nil {
				logger.Printf("[ERROR] In subdirectory %s: %s", subdir.RelativeLocation, err.Error())
			}
			logger.Printf("[INFO] Finished Processing subdirectory: %s", subdir.RelativeLocation)
		}
		logger.Printf("[INFO] Finished Processing conda-channel: %s", ch.RelativeLocation)
	}
}
