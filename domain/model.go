package domain

import "io"

type CondaChannelFileSource interface {
	// GetFile takes a relative location such as "base-ng/linux-64/repodata.json" and
	// returns a read-closer for it.
	GetFileReadCloser(string) (io.ReadCloser, error)
}

// CondaRepodata is a bare-minimum abstraction of the structure of a conda repodata.json file
// for the purpose of reverse indexing the files in packages.
type CondaRepodata struct {
	Packages map[string]CondaPackage `json:"packages"`
}

// Kafkadocs maps kafka doc json files to their SHA256sum
type Kafkadocs struct {
	Docs map[string]KafkadocEntry `json:"docs"`
}

type KafkadocEntry struct {
	Path   string `json:"path"`
	Sha256 string `json:"sha256"`
}

// CondaPackage is a generic abstraction of the "packages" section of a conda repodata.json file.
// It's structured generically so that we do not have to care about what fields are added or removed
// in the future as long as the essentially ones are there.
type CondaPackage map[string]interface{}

// CondaServer stores configuration information about a single conda server.
// A conda-server is a collection of channels under a single directory (local)
// or accessible under a base url (remote).
type CondaServer struct {
	Name string `json:"name"`

	Url                              string `json:"url"`
	Path                             string `json:"path"`
	RepodataLockFilename             string `json:"repodata_lock_filename"`
	RepodataLockMaxWaitSeconds       int    `json:"repodata_lock_max_wait_seconds"`
	RepodataLockRetryIntervalSeconds int    `json:"repodata_lock_retry_interval_seconds"`

	Workdir string `json:"workdir"`

	Channels map[string]Channel `json:"channels"`
}

type Channel struct {
	Name string `json:"name"`

	RelativeLocation string            `json:"relative_location"`
	Subdirs          map[string]Subdir `json:"subdirs"`
}

type Subdir struct {
	Name             string                 `json:"name"`
	RelativeLocation string                 `json:"relative_location"`
	ExtraData        map[string]interface{} `json:"extra_data"`
}
