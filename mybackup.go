package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mybackup/config"
	"mybackup/utils"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
)

const (
	errCodeUndefined = 1 + iota
	errCodeWrongArgs
	errCodeConfigLog
	errCodeConfigFileNotExist
	errCodeParseConfigFile
	errCodeMarshalConfigFile
	errCodeEmptyDefaultTargetFile
	errCodeEmptyDefaultTargetBaseDIR
	errCodeRunAction
	errCodeMarshalResultFailed
	errCodeWriteResultFileFailed
	errCodeWriteMetadataFailed
)

var (
	version string // set with -ldflags "-X main.version=1.0.0"
	commit  string // set with -ldflags "-X main.commit=abc123"
	buildAt string // set with -ldflags "-X main.buildAt=2022-12-08 12:00:00 CST"
)

// finish function, operations before exit
func finish(exitCode int) {
	// for log flush
	log.Infof("======== mybackup end with exit code %d ========", exitCode)
	os.Exit(exitCode)
}

type Options struct {
	Version       bool   `short:"v" long:"version" description:"Show version"`
	Config        string `short:"c" long:"config" description:"Specify the config file"`
	SourceFile    string `short:"s" long:"source-file" description:"Specify the source file path in storage, if '-' means read from stdin"`
	TargetFile    string `short:"t" long:"target-file" description:"Specify the target file path in storage"`
	Overwrite     bool   `short:"o" long:"overwrite" description:"Whether overwrite when target file exists while uploading or backuping"`
	LogFilePrefix string `short:"l" long:"log-file-prefix" description:"Specify the log file prefix"`
}

func main() {
	// check and parse args
	var opts Options
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		os.Exit(errCodeWrongArgs)
	}

	if opts.Version {
		fmt.Printf("version:   %s\n", version)
		fmt.Printf("commit:    %s\n", commit)
		fmt.Printf("build_at:  %s\n", buildAt)
		os.Exit(1)
	}

	configFile := opts.Config
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Printf("config file %s does not exist\n", configFile)
		finish(errCodeConfigFileNotExist)
	}

	targetFile := opts.TargetFile
	sourceFile := opts.SourceFile
	overwrite := opts.Overwrite
	logFilePrefix := opts.LogFilePrefix

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// read configuration
	conf, err := config.ReadConfig(configFile)
	if err != nil {
		fmt.Printf("read config file %s failed: %s\n", configFile, err.Error())
		finish(errCodeParseConfigFile)
	}
	if sourceFile != "" && logFilePrefix == "" {
		logFilePrefix = fmt.Sprintf(
			"%s-%s-%d",
			filepath.Base(sourceFile),
			conf.TaskTime.Format("20060102_150405"),
			conf.TaskPID,
		)
	}
	if logFilePrefix != "" {
		if strings.HasPrefix(logFilePrefix, "-") {
			logFilePrefix = strings.Replace(logFilePrefix, "-", "stdin", 1)
		}
		conf.FilePrefix = logFilePrefix
	}

	err = conf.SetLogFiles()
	if err != nil {
		log.Errorf("set log failed: %+v", err)
		finish(errCodeConfigLog)
	}

	log.Infof("======== mybackup start ========")

	err = conf.SetMetaDB()
	if err != nil {
		log.Errorf("set metadb failed: %+v", err)
		finish(errCodeConfigLog)
	}

	// output config as json
	c, err := json.MarshalIndent(*conf, "", "\t")
	if err != nil {
		log.Errorf("marshal config failed: %+v", err)
		finish(errCodeMarshalConfigFile)
	}
	log.Debugf("parsed configuration:\n%s", string(c))

	log.Infof("start %s", conf.Action)

	// prepare variables
	// set source storage file path
	if sourceFile != "" {
		if conf.Action != "list" {
			log.Infof(
				"overwrite source storage file path(%s) to command line source file(%s)",
				conf.Source.GetStorageFilePath(),
				sourceFile,
			)
			conf.Source.SetStorageFilePath(sourceFile)
		} else {
			log.Infof("action 'list' only run for 'target' config, no need to set 'source'")
		}
	}
	// set target storage file path
	if targetFile != "" {
		log.Infof(
			"overwrite target storage file path(%s) to command line target file(%s)",
			conf.Target.GetStorageFilePath(),
			targetFile,
		)
		conf.Target.SetStorageFilePath(targetFile)
	}
	// if check target file existance
	if !overwrite {
		log.Infof(
			"overwrite(false): when target file exists, upload or backup will failed with exit code 8",
		)
	}

	// set default target filename
	defaultTargetFilename := fmt.Sprintf("%s-%s-%s-%d",
		conf.TaskName,
		conf.Target.GetType(),
		conf.TaskTime.Format("20060102_150405"),
		conf.TaskPID,
	)
	if err = conf.Target.SetDefaultFileName(defaultTargetFilename); err != nil {
		log.Errorf("%s failed: %+v", conf.Action, err)
		finish(errCodeEmptyDefaultTargetFile)
	}
	// set default target base directory
	if err = conf.Target.SetDefaultBaseDIR(conf.BaseDIR); err != nil {
		log.Errorf("%s failed: %+v", conf.Action, err)
		finish(errCodeEmptyDefaultTargetBaseDIR)
	}

	// run action
	switch conf.Action {
	case "move":
		err = conf.Source.Move(ctx, *conf.Utils, conf.Target, overwrite)
	case "upload":
		err = conf.Source.Upload(ctx, *conf.Utils, conf.Target, overwrite)
	case "backup":
		err = conf.Source.Backup(ctx, *conf.Utils, conf.Target, overwrite)
	case "download":
		err = conf.Source.Download(ctx, *conf.Utils, conf.Target, overwrite)
	case "recover":
		err = conf.Source.Recover(ctx, *conf.Utils, conf.Target, overwrite)
	case "list":
		err = conf.Target.List(ctx, *conf.Utils)
	default:
		err = fmt.Errorf("unsupport action %s", conf.Action)
	}
	// failed
	if err != nil {
		log.Errorf("%s failed: %+v, retry command:\n%s", conf.Action, err, strings.Join(os.Args, " "))
		if errors.Is(err, os.ErrExist) {
			finish(errCodeRunAction)
		}
		finish(errCodeUndefined)
	}
	// write result file
	result, err := json.MarshalIndent(conf.Target, "", "  ")
	if err != nil {
		log.Errorf("%s failed: %+v", conf.Action, err)
		finish(errCodeMarshalResultFailed)
	}
	err = os.WriteFile(conf.ResultFile, result, 0644)
	if err != nil {
		log.Errorf("%s failed: %+v", conf.Action, err)
		finish(errCodeWriteResultFileFailed)
	}
	// clean expired files in base dir
	if conf.BaseDIRExpireDuration > 0 {
		err = utils.CleanExpiredSubFiles(conf.BaseDIR, conf.BaseDIRExpireDuration)
		if err != nil {
			log.Infof("clean expired sub files of %s failed: %+v", conf.BaseDIR, err)
		}
	}
	log.Infof("%s result: %s", conf.Action, string(result))
	// success
	finish(0)
}
