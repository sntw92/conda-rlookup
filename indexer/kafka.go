package indexer

import (
	"conda-rlookup/config"
	"conda-rlookup/domain"
	"conda-rlookup/helpers"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/google/renameio"
	kafka "github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer

func GetKafkaWriter() *kafka.Writer {
	return kafkaWriter
}

func InitKafkaWriter(cfg *config.KafkaWriterConfig) error {
	appLogger := helpers.GetAppLogger()

	if kafkaWriter == nil {
		dialer := &kafka.Dialer{
			Timeout:   kafka.DefaultDialer.Timeout,
			DualStack: kafka.DefaultDialer.DualStack,
			TLS:       cfg.TLSConfig,
		}

		errorLogger := kafka.LoggerFunc(appLogger.Printf)
		logger := kafka.LoggerFunc(appLogger.Printf)

		kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
			Brokers:     cfg.Brokers,
			Topic:       cfg.Topic,
			BatchBytes:  50 * 1024 * 1024, // 50MB max message size
			Balancer:    &kafka.LeastBytes{},
			Dialer:      dialer,
			Logger:      logger,
			ErrorLogger: errorLogger,
		})
	}

	return nil
}

func writeJsonFileToKafka(filename string, kw *kafka.Writer) error {
	logger := helpers.GetAppLogger()
	var res map[string]interface{}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return logger.ErrorPrintf("could not open file %s for reading json data: %s", filename, err.Error())
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	if err = decoder.Decode(&res); err != nil {
		return logger.ErrorPrintf("error decoding json for file %s", filename)
	}
	f.Close()

	data, _ := json.Marshal(res)
	if err = kw.WriteMessages(context.Background(), kafka.Message{Value: data}); err != nil {
		return logger.ErrorPrintf("couldn't write message from file %s to kafka: %s", filename, err)
	}
	logger.Printf("[INFO] Written file %s to Kafka", filename)
	return nil
}

func writeDeleteMessageToKafka(id string, kw *kafka.Writer) error {
	logger := helpers.GetAppLogger()

	delDoc := struct {
		Id       string `json:"id"`
		EsAction string `json:"es_action"`
	}{
		Id:       id,
		EsAction: "delete",
	}

	data, err := json.Marshal(delDoc)
	if err != nil {
		return logger.ErrorPrintf("could not create es deletion doc for kafka: %s", err.Error())
	}
	if err = kw.WriteMessages(context.Background(), kafka.Message{Value: data}); err != nil {
		return logger.ErrorPrintf("couldn't write deletion doc for id %s to kafka: %s", id, err)
	}
	logger.Printf("[INFO] Written deletion doc for id %s to Kafka", id)
	return nil
}

func readInKafkadocsFile(filename string) (*domain.Kafkadocs, error) {
	logger := helpers.GetAppLogger()

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		logger.Printf("[INFO] Kafkadocs file %s does not exist. Creating an empty one.", filename)

		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			return nil, logger.ErrorPrintf("could not open/create kafkadocs file: %s", err.Error())
		}
		defer f.Close()

		res := domain.Kafkadocs{
			Docs: make(map[string]domain.KafkadocEntry),
		}

		if err = json.NewEncoder(f).Encode(res); err != nil {
			return nil, logger.ErrorPrintf("could not write empty data to kafkadocs file: %s", err.Error())
		}

		return &res, nil
	}

	logger.Printf("[DEBUG] Opening kafkadocs file: %s", filename)
	kafkadocsFile, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return nil, logger.ErrorPrintf("could not open kafkadocs file %s for reading: %s", filename, err.Error())
	}
	defer kafkadocsFile.Close()

	logger.Printf("[DEBUG] Reading Kafkadocs from file: %s", filename)
	kafkadocs, err := readKafkadocs(kafkadocsFile)
	if err != nil {
		return nil, logger.ErrorPrintf("could not read kafkadocs from file %s: %s", filename, err.Error())
	}

	return kafkadocs, nil
}

func readKafkadocs(r io.Reader) (*domain.Kafkadocs, error) {
	logger := helpers.GetAppLogger()

	var res domain.Kafkadocs
	if err := json.NewDecoder(r).Decode(&res); err != nil {
		return nil, logger.ErrorPrintf("could not read and parse kafkadocs: %s", err.Error())
	}

	return &res, nil
}

func SubdirFlushToKafka(s domain.Subdir, prefixDir string) error {
	logger := helpers.GetAppLogger()

	kw := GetKafkaWriter()

	// Create Working directory, if required
	workDir := filepath.Join(prefixDir, s.RelativeLocation)
	err := os.MkdirAll(workDir, 0755)
	if err != nil {
		return logger.ErrorPrintf("could not create workdir at %s for conda-channel-subdir: %s",
			workDir, err.Error())
	}

	//TODO: Make historic kafkadocs filename configurable
	histKafkadocsFilename := filepath.Join(workDir, "kafkadocs.json.history")
	curKafkadocsFilename := filepath.Join(workDir, "kafkadocs.json")

	kafkadocsTempFile, err := renameio.TempFile("", histKafkadocsFilename)
	if err != nil {
		return logger.ErrorPrintf("could not open kafkadocs temp file: %s", err.Error())
	}
	//nolint:errcheck
	defer kafkadocsTempFile.Cleanup()

	// Get the historic kafkadocs
	histKafkadocs, err := readInKafkadocsFile(histKafkadocsFilename)
	if err != nil {
		return logger.ErrorPrintf("could not read in historic kafkadocs file %s: %s", histKafkadocsFilename, err.Error())
	}

	// Get the current kafkadocs
	curKafkadocs, err := readInKafkadocsFile(curKafkadocsFilename)
	if err != nil {
		return logger.ErrorPrintf("could not read in current kafkadocs %s: %s", curKafkadocsFilename, err.Error())
	}

	// Start with a black success state; add no-ops and successful updates as we progress
	successKafkadocs := domain.Kafkadocs{Docs: make(map[string]domain.KafkadocEntry)}

	// Statistics
	var nOldPackages, nCurPackages, nSkipped, nUpdated, nDeleted, nFailed, nUpToDate int
	nOldPackages = len(histKafkadocs.Docs)
	nCurPackages = len(curKafkadocs.Docs)

	for id, doc := range curKafkadocs.Docs {
		var updateRequired bool

		if oldDoc, ok := histKafkadocs.Docs[id]; !ok {
			updateRequired = true
		} else if oldDoc.Sha256 != doc.Sha256 {
			updateRequired = true
		}

		if updateRequired {
			nFailed += 1
			if doc.Path == "" || doc.Sha256 == "" {
				err := writeDeleteMessageToKafka(id, kw)
				if err != nil {
					logger.ErrorPrintf("could not delete document %s: %s", id, err.Error())
					continue
				}
				nDeleted += 1
			} else {
				err := writeJsonFileToKafka(filepath.Join(workDir, doc.Path), kw)
				if err != nil {
					logger.ErrorPrintf("could not index document %s: %s", id, err.Error())
					continue
				}
			}
			nFailed -= 1
			nUpdated += 1
			successKafkadocs.Docs[id] = doc
		} else {
			nUpToDate += 1
			successKafkadocs.Docs[id] = doc
		}
	}

	if err = json.NewEncoder(kafkadocsTempFile).Encode(successKafkadocs); err != nil {
		return logger.ErrorPrintf("could not write success data to new kafkadocs history file: %s", err.Error())
	}

	if err = kafkadocsTempFile.CloseAtomicallyReplace(); err != nil {
		return logger.ErrorPrintf("could not update histrorical kafkadocs file: %s", err.Error())
	}

	logger.Printf("[INFO] Kafka Summary for %s: (Old -> New) = (%d -> %d), Updated = %d, Deleted = %d, Failed = %d, Skipped = %d, Up-to-date = %d",
		s.RelativeLocation, nOldPackages, nCurPackages, nUpdated, nDeleted, nFailed, nSkipped, nUpToDate)

	return nil
}
