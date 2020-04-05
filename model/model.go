package model

import "io"

type CondaChannelFileSource interface {
	// GetFile takes a relative location such as "base-ng/linux-64/repodata.json" and
	// returns a read-closer for it.
	GetFileReadCloser(string) (io.ReadCloser, error)
}

type CondaServer struct {
	Name string `json:"name"`

	Url  string `json:"url"`
	Path string `json:"path"`

	Workdir string `json:"workdir"`

	Channels []Channel `json:"channels"`
}

type Channel struct {
	Name string `json:"name"`

	RelativeLocation string   `json:"relative_location"`
	Subdirs          []Subdir `json:"subdirs"`
}

type Subdir struct {
	Name             string `json:"name"`
	RelativeLocation string `json:"relative_location"`
}
