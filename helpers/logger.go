package helpers

import (
	"conda-rlookup/utils"
	"log"
	"os"
)

var appLogger *utils.AppLogger

// InitAppLogger initializes the applications global logger.
// The default logger is a combined file + console logger,
// but only the location of the log-file has been left configurable.
func InitAppLogger() error {
	if appLogger == nil {
		appLogger = &utils.AppLogger{
			Filename:           "",
			Writer:             os.Stderr,
			FileLoggerFlags:    log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile,
			ConsoleLoggerFlags: log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile,
			Prefix:             "[INDEXER] ",
		}
		return appLogger.Init()
	}

	return nil
}

// GetAppLogger returns a pointer to the global application logger.
// Most functions can just call this function to start using the logger.
func GetAppLogger() *utils.AppLogger {
	return appLogger
}
