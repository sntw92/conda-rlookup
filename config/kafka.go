package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"log"
	"strings"
)

// KafkaWriterConfig represents the kafka configuration to be used to connect to kafka brokers
type KafkaWriterConfig struct {
	Brokers       []string    `json:"brokers"`
	Topic         string      `json:"topic"`
	TLSEnabled    string      `json:"tls_enabled"`
	TLSCertFile   string      `json:"tls_cert_file"`
	TLSKeyFile    string      `json:"tls_key_file"`
	TLSSkipVerify string      `json:"tls_skip_verify"`
	CAFile        string      `json:"ca_file"`
	TLSConfig     *tls.Config `json:"-"`
}

// GenerateKafkaTLSConfig validates and generates a TLS Config for the given kafka config
func (k *KafkaWriterConfig) GenerateTLSConfig(cfg *KafkaWriterConfig) error {
	if strings.ToLower(cfg.TLSEnabled) != "true" {
		return nil
	}

	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return errors.New("TLS Cert File/Key missing")
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return err
	}

	var caCertPool *x509.CertPool

	if cfg.CAFile != "" {
		caCert, err := ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			log.Fatal("kafka TLS CA file error: ", err)
		}
		caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
	}

	k.TLSConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: strings.ToLower(cfg.TLSSkipVerify) == "true",
	}

	return nil
}
