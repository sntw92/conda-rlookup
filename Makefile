PROJECT_NAME=conda-rlookup
BINARY_NAME=conda-rlookup-indexer
CI_PIPELINE_ID ?= 0
BINARY_DIR ?= bin

#--------------------------------------------------------------
MAJOR_MINOR_VERSION := $(shell cat VERSION)
ifndef PATCH_NUMBER
	VERSION := $(MAJOR_MINOR_VERSION).$(CI_PIPELINE_ID)
else
	VERSION := $(MAJOR_MINOR_VERSION).$(PATCH_NUMBER)
endif

GIT_COMMIT_SHA=`git rev-list -1 HEAD`

BUILD_TIME=`date +%FT%T%z`

BUILD_USER := ${GITLAB_USER_EMAIL}
ifndef BUILD_USER
	BUILD_USER := ${USER}
endif
ifndef BUILD_USER
	BUILD_USER := $(shell id -un)
endif

BUILD_HOST := ${HOSTNAME}
ifndef BUILD_HOST
	BUILD_HOST := $(shell hostname)
endif

#---------------------------------------------------------------
build: build_only

build_prod: check_for_go build_only

build_only:
	mkdir -p "${BINARY_DIR}"
	GO111MODULE=on CGO_ENABLED=1 go build -ldflags "-s -w \
	-X ${PROJECT_NAME}/config.Version=${VERSION} \
	-X ${PROJECT_NAME}/config.GitCommitSha=${GIT_COMMIT_SHA} \
	-X ${PROJECT_NAME}/config.BuildTime=${BUILD_TIME} \
	-X ${PROJECT_NAME}/config.BuildUser=${BUILD_USER} \
	-X ${PROJECT_NAME}/config.BuildHost=${BUILD_HOST}" \
	-o "${BINARY_DIR}/${BINARY_NAME}"

	# Format Go files
	find . -name '*.go' -type f -exec gofmt -w {} \;

check_for_go:
	go version

clean:
	if [[ -n "${BINARY_DIR}" ]] && [[ -d "${BINARY_DIR}" ]]; then \
		rm -rf "${BINARY_DIR}"; \
	fi
