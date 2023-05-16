# init project path
HOMEDIR	        := $(shell pwd)
OUTDIR          := $(HOMEDIR)/output
GOPKGS          := $$(go list ./...| grep -vE "vendor")
VERSION         := $$(awk '/^\#\#\# /{gsub(/\[|\]/, "", $$2);print $$2;exit}' $(HOMEDIR)/CHANGELOG.md)
COMMIT          := $$(git rev-parse HEAD)
BUILD_AT        := $(shell date '+%Y-%m-%d_%H:%M:%S_%Z')

# set go env
export GOENV = $(HOMEDIR)/go.env

# set vars
BYELLOW := \033[1;33m
BGREEN := \033[1;32m
BRED := \033[1;31m
NC := \033[0m

# test cover files
COVPROF	:=	$(HOMEDIR)/covprof.out  # coverage profile
COVFUNC	:=	$(HOMEDIR)/covfunc.txt  # coverage profile information for each function
COVHTML	:=	$(HOMEDIR)/covhtml.html # HTML representation of coverage profile

.ONESHELL:
# make, make all
all: prepare compile package

# make prepare, download dependencies
prepare: prepare-dep
prepare-dep:
	@printf "$(BYELLOW)######## prepare$(NC)\n"
	git version
	go env

# make compile
compile: build
build:
	@printf "$(BYELLOW)######## build$(NC)\n"
	mkdir -vp $(OUTDIR)
	GOOS=$$(go env GOOS)
	GOARCH=$$(go env GOARCH)
	target_file="$(OUTDIR)/mybackup-$${GOOS}-$${GOARCH}"

	printf "$(BRED)#### building for $${GOOS}/$${GOARCH}$(NC)\n"
	printf "version:   $(VERSION)\n" 
	printf "commit:    $(COMMIT)\n"
	printf "build at:  $(BUILD_AT)\n";
	go build -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildAt=$(BUILD_AT)" -o $${target_file} && \
		printf "target file: $(BGREEN)$${target_file}$(NC)\n"
# make test, test your code
test: test-case
test-case: set-env
	@printf "$(BYELLOW)######## test$(NC)\n"
	go test -race -timeout=120s -v -cover $(GOPKGS) -coverprofile=coverage.out | tee unittest.txt

# make package
package: package-bin
package-bin:
	@printf "$(BYELLOW)######## package build result$(NC)\n"
	rsync -rpltu conf_examples/* $(OUTDIR)

# make clean
clean:
	@printf "$(BYELLOW)######## clean packaged output$(NC)\n"
	rm -vrf $(OUTDIR)

.PHONY: all prepare compile package clean build