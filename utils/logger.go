package utils

import (
	"fmt"
	"io"
	"log"
	"os"
)

type AppLogger struct {
	Filename string
	Writer   io.Writer

	FileLogger    *log.Logger
	ConsoleLogger *log.Logger

	FileLoggerFlags    int
	ConsoleLoggerFlags int

	Prefix string
}

func (a *AppLogger) Init() error {
	if a.FileLogger == nil && a.Filename != "" {
		appLogFilePath := a.Filename

		appLogFile, err := os.OpenFile(appLogFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0664)
		if err != nil {
			return fmt.Errorf("could not open app log file %s: %s", appLogFilePath, err.Error())
		}
		a.FileLogger = log.New(appLogFile, a.Prefix, a.FileLoggerFlags)
	}

	if a.ConsoleLogger == nil && a.Writer != nil {
		a.ConsoleLogger = log.New(a.Writer, a.Prefix, a.ConsoleLoggerFlags)
	}

	return nil
}

// GetFileLogger returns the file logger associated with this AppLogger if any,
// If there is no FileLogger associated nil is returned.
func (a *AppLogger) GetFileLogger() *log.Logger {
	if a == nil {
		return nil
	}
	return a.FileLogger
}

// GetConsoleLogger returns the ConsoleLogger associated with this AppLogger, if any.
// If there is no console logger nil is returned
func (a *AppLogger) GetConsoleLogger() *log.Logger {
	if a == nil {
		return nil
	}
	return a.ConsoleLogger
}

func (a *AppLogger) Printf(format string, v ...interface{}) {
	errStr := fmt.Sprintf(format, v...)
	if a.FileLogger != nil {
		a.FileLogger.Output(2, errStr)
	}
	if a.ConsoleLogger != nil {
		a.ConsoleLogger.Output(2, errStr)
	}
}

func (a *AppLogger) Print(v ...interface{}) {
	errStr := fmt.Sprint(v...)
	if a.FileLogger != nil {
		a.FileLogger.Output(2, errStr)
	}
	if a.ConsoleLogger != nil {
		a.ConsoleLogger.Output(2, errStr)
	}
}

func (a *AppLogger) Println(v ...interface{}) {
	errStr := fmt.Sprintln(v...)
	if a.FileLogger != nil {
		a.FileLogger.Output(2, errStr)
	}
	if a.ConsoleLogger != nil {
		a.ConsoleLogger.Output(2, errStr)
	}
}

func (a *AppLogger) ErrorPrintf(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	if a.FileLogger != nil {
		a.FileLogger.Output(2, err.Error())
	}
	if a.ConsoleLogger != nil {
		a.ConsoleLogger.Output(2, err.Error())
	}
	return err
}
