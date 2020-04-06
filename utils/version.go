package utils

type VersionDetails struct {
	Version      string `json:"version"`
	GitCommitSha string `json:"git_commitsha"`

	BuildTime string `json:"build_time"`
	BuildHost string `json:"build_host"`
	BuildUser string `json:"build_user"`
}
