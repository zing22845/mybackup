package utils

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"gopkg.in/ini.v1"

	log "github.com/sirupsen/logrus"
)

// PgBackRest PgBackRest struct, contain the PgBackRest command config etc.
type PgBackRest struct {
	Main            string             `json:"main,omitempty"`
	Action          string             `json:"action,omitempty"`
	ConfigFile      string             `json:"config_file,omitempty"`
	Delta           bool               `json:"delta,omitempty"`
	LogLevelConsole string             `json:"log_level_console,omitempty"`
	LogLevelFile    string             `json:"log_level_file,omitempty"`
	LogPath         string             `json:"log_path,omitempty"`
	LockPath        string             `json:"lock_path,omitempty"`
	SpoolPath       string             `json:"spool_path,omitempty"`
	Type            string             `json:"type,omitempty"`
	Target          string             `json:"target,omitempty"`
	OutputFormat    string             `json:"output_format,omitempty"`
	StanzaList      map[string]*Stanza `json:"stanza_list,omitempty"`
	INIFile         *ini.File          `json:"-"`
}

type Stanza struct {
	PG1Path           string `json:"pg1_path,omitempty"`
	RepoID            uint16 `json:"repo_id,omitempty"`
	RepoName          string `json:"repo_name,omitempty"`
	BackupStandby     bool   `json:"backup_standby,omitempty"`
	CompressType      string `json:"compress_type,omitempty"`
	Path              string `json:"path,omitempty"`
	Bundle            bool   `json:"bundle,omitempty"`
	RetentionFull     uint16 `json:"retention_full,omitempty"`
	RetentionFullType string `json:"retention_full_type,omitempty"`
	RetentionHistory  uint16 `json:"retention_history,omitempty"`
	StorageVerifyTLS  bool   `json:"storage_verify_tls,omitempty"`
	RepoType          string `json:"repo_type,omitempty"`
	S3Bucket          string `json:"s3_bucket,omitempty"`
	S3Endpoint        string `json:"s3_endpoint,omitempty"`
	S3Region          string `json:"s3_region,omitempty"`
	S3Key             string `json:"s3_key,omitempty"`
	S3KeySecret       string `json:"s3_key_secret,omitempty"`
	S3Token           string `json:"s3_token,omitempty"`
	S3URIStyle        string `json:"s3_uri_style,omitempty"`
}

func (p *PgBackRest) CreateGlobalSection() (err error) {
	section, _ := p.INIFile.NewSection("global")
	// log-level_console
	key, err := section.NewKey("log-level-console", p.LogLevelConsole)
	if err != nil {
		return err
	}
	if key.Value() == "" {
		key.SetValue("info")
	}
	// log-level_file
	key, err = section.NewKey("log-level-file", p.LogLevelFile)
	if err != nil {
		return err
	}
	if key.Value() == "" {
		key.SetValue("debug")
	}
	// get the orgin path by executable path
	executablePath, err := os.Executable()
	if err != nil {
		return err
	}
	originPath := filepath.Dir(executablePath)
	// log-path
	if p.LogPath == "" {
		p.LogPath = filepath.Join(originPath, "log")
	}
	_, err = section.NewKey("log-path", p.LogPath)
	if err != nil {
		return err
	}
	// lock-path
	if p.LockPath == "" {
		p.LockPath = filepath.Join(originPath, "lock")
	}
	_, err = section.NewKey("lock-path", p.LockPath)
	if err != nil {
		return err
	}
	// spool-path
	if p.LockPath == "" {
		p.LockPath = filepath.Join(originPath, "spool")
	}
	_, err = section.NewKey("spool-path", p.SpoolPath)
	if err != nil {
		return err
	}
	// delta
	key, err = section.NewKey("delta", "n")
	if err != nil {
		return err
	}
	if p.Delta {
		key.SetValue("y")
	}
	return nil
}

func (p *PgBackRest) CreateStanzaSection(name string) (err error) {
	stanza, ok := p.StanzaList[name]
	if !ok {
		return fmt.Errorf("no stanza %s in stanza list", name)
	}
	// create stanza section
	section, err := p.INIFile.NewSection(name)
	if err != nil {
		return err
	}

	// create stanza key/value pairs
	if stanza == nil {
		return fmt.Errorf("(%s) empty stanza", name)
	}
	// pg1-path
	if len(stanza.PG1Path) == 0 {
		return fmt.Errorf("(%s) empty pg1 path", name)
	}
	_, err = section.NewKey("pg1-path", stanza.PG1Path)
	if err != nil {
		return fmt.Errorf("(%s) new pg1-path failed: %w", name, err)
	}
	// backup-standby
	if stanza.BackupStandby {
		_, err = section.NewKey("backup-standby", "y")
		if err != nil {
			return fmt.Errorf("(%s) new backup-standby failed: %w", name, err)
		}
	}
	// compress-type
	key, err := section.NewKey("compress-type", stanza.CompressType)
	if err != nil {
		return fmt.Errorf("(%s) new compress-type failed: %w", name, err)
	}
	if key.Value() == "" {
		key.SetValue("zst")
	}
	// repo name: repo[id]
	if stanza.RepoID == 0 || stanza.RepoID > 256 {
		return fmt.Errorf("(%s) repo id must between [1, 256]", name)
	}
	repoName := fmt.Sprintf("repo%d", stanza.RepoID)
	// repoN-path
	if stanza.Path == "" {
		return fmt.Errorf("(%s) empty %s-path", name, repoName)
	}
	_, err = section.NewKey(repoName+"-path", stanza.Path)
	if err != nil {
		return fmt.Errorf("(%s) new %s-path failed: %w", name, repoName, err)
	}
	// repoN-retention-full-type
	key, err = section.NewKey(repoName+"-retention-full-type", stanza.RetentionFullType)
	if err != nil {
		return fmt.Errorf("(%s) new %s-retention-full-type failed: %w", name, repoName, err)
	}
	if key.Value() == "" {
		key.SetValue("count")
	}
	// repoN-retention-full
	key, err = section.NewKey(
		repoName+"-retention-full",
		fmt.Sprintf("%d", stanza.RetentionFull),
	)
	if err != nil {
		return fmt.Errorf("(%s) new %s-retention-full failed: %w", name, repoName, err)
	}
	if key.Value() == "" {
		key.SetValue("7")
	}
	// repoN-retention-history
	key, err = section.NewKey(
		repoName+"-retention-history",
		fmt.Sprintf("%d", stanza.RetentionHistory),
	)
	if err != nil {
		return fmt.Errorf("(%s) new %s-retention-history failed: %w", name, repoName, err)
	}
	if key.Value() == "0" {
		key.SetValue("3650") // default retain the backup history info for 10 years
	}
	// repoN-bundle
	key, err = section.NewKey(repoName+"-bundle", "n")
	if err != nil {
		return fmt.Errorf("(%s) new %s-bundle failed: %w", name, repoName, err)
	}
	if stanza.Bundle {
		key.SetValue("y")
	}
	// repoN-storage-verify-tls
	key, err = section.NewKey(repoName+"-storage-verify-tls", "n")
	if err != nil {
		return fmt.Errorf("(%s) new %s-storage-verify-tls failed: %w", name, repoName, err)
	}
	if stanza.StorageVerifyTLS {
		key.SetValue("y")
	}
	// repoN-type
	repoType := repoName + "-type"
	key, err = section.NewKey(repoType, stanza.RepoType)
	if err != nil {
		return fmt.Errorf("(%s) new %s failed: %w", name, repoType, err)
	}
	if key.Value() == "" {
		key.SetValue("posix")
	}
	storageType := key.Value()
	// add storage configs
	switch storageType {
	case "s3":
		err = p.AddS3Options(name, repoName)
		if err != nil {
			return err
		}
	case "posix":
	default:
		return fmt.Errorf("(%s) not support repo type: %s", name, repoType)
	}
	return nil
}

func (p *PgBackRest) AddS3Options(stanzaName, repoName string) (err error) {
	// check basic vars
	if p.INIFile == nil {
		return fmt.Errorf("empty ini file")
	}
	section := p.INIFile.Section(stanzaName)
	if section == nil {
		return fmt.Errorf("(%s) empty section", stanzaName)
	}
	stanza := p.StanzaList[stanzaName]
	if stanza == nil {
		return fmt.Errorf("(%s) empty stanza", stanzaName)
	}
	// repoN-s3-bucket
	key, err := section.NewKey(repoName+"-s3-bucket", stanza.S3Bucket)
	if err != nil {
		return fmt.Errorf("(%s) new %s-s3-bucket failed: %w", stanzaName, repoName, err)
	}
	if key.Value() == "" {
		return fmt.Errorf("(%s) empty s3 bucket", stanzaName)
	}
	// repoN-s3-region
	key, err = section.NewKey(repoName+"-s3-region", stanza.S3Region)
	if err != nil {
		return fmt.Errorf("(%s) new %s-s3-region failed: %w", stanzaName, repoName, err)
	}
	if key.Value() == "" {
		return fmt.Errorf("(%s) empty s3 region", stanzaName)
	}
	// repoN-s3-endpoint
	key, err = section.NewKey(repoName+"-s3-endpoint", stanza.S3Endpoint)
	if err != nil {
		return fmt.Errorf("(%s) new %s-s3-endpoint failed: %w", stanzaName, repoName, err)
	}
	if key.Value() == "" {
		return fmt.Errorf("(%s) empty s3 endpoint", stanzaName)
	}
	// repoN-s3-key
	key, err = section.NewKey(repoName+"-s3-key", stanza.S3Key)
	if err != nil {
		return fmt.Errorf("(%s) new %s-s3-key failed: %w", stanzaName, repoName, err)
	}
	if key.Value() == "" {
		return fmt.Errorf("(%s) empty s3 key", stanzaName)
	}
	// repoN-s3-key-secret
	key, err = section.NewKey(repoName+"-s3-key-secret", stanza.S3KeySecret)
	if err != nil {
		return fmt.Errorf("(%s) new %s-s3-key-secret failed: %w", stanzaName, repoName, err)
	}
	if key.Value() == "" {
		return fmt.Errorf("(%s) empty s3 key secret", stanzaName)
	}
	// repoN-s3-token
	if stanza.S3Token != "" {
		_, err = section.NewKey(repoName+"-s3-token", stanza.S3Token)
		if err != nil {
			return fmt.Errorf("(%s) new %s-s3-token failed: %w", stanzaName, repoName, err)
		}
	}
	// repoN-s3-key-uri-style
	key, err = section.NewKey(repoName+"-s3-uri-style", stanza.S3URIStyle)
	if err != nil {
		return fmt.Errorf("(%s) new %s-s3-uri-style failed: %w", stanzaName, repoName, err)
	}
	if key.Value() == "" {
		key.SetValue("host")
	}
	return nil
}

// GenerateConfigFile generate pgBackRest config file
func (p *PgBackRest) GenerateConfigFile(backPostfix string) (err error) {
	if p.ConfigFile == "" {
		return fmt.Errorf("empty pgBackRest config file")
	}
	// rename old config file
	fileInfo, err := os.Stat(p.ConfigFile)
	if err == nil {
		if fileInfo.IsDir() {
			return fmt.Errorf("'%s' is directory", p.ConfigFile)
		}
		backConfigFile := fmt.Sprintf("%s%s", p.ConfigFile, backPostfix)
		err = os.Rename(p.ConfigFile, backConfigFile)
		if err != nil {
			return err
		}
	}
	// create INI config
	p.INIFile = ini.Empty()
	err = p.CreateGlobalSection()
	if err != nil {
		return err
	}
	for k := range p.StanzaList {
		err = p.CreateStanzaSection(k)
		if err != nil {
			return err
		}
	}
	// save INI file
	ini.PrettySection = true
	ini.PrettyFormat = true
	ini.PrettyEqual = true
	err = p.INIFile.SaveTo(p.ConfigFile)
	if err != nil {
		return err
	}
	return nil
}

func (p *PgBackRest) PrepareRun() (err error) {
	backPostfix := time.Now().Format(".bak_20060102_150405")
	// generate config file
	err = p.GenerateConfigFile(backPostfix)
	if err != nil {
		return err
	}
	// backup log path
	if _, err := os.Stat(p.LogPath); err == nil {
		err = os.Rename(p.LogPath, p.LogPath+backPostfix)
		if err != nil {
			return err
		}
	}
	// init log path
	err = CreateDIR(p.LogPath)
	if err != nil {
		return err
	}
	// init lock path
	err = CreateDIR(p.LockPath)
	if err != nil {
		return err
	}
	// init spool path
	err = CreateDIR(p.SpoolPath)
	if err != nil {
		return err
	}
	return nil
}

// stanza-create
func (p *PgBackRest) GenStanzaCreateCMD(stanzaName string) (cmd *exec.Cmd, err error) {
	if stanzaName == "" {
		return nil, fmt.Errorf("empty stanza name")
	}
	return exec.Command("sh", "-c",
		fmt.Sprintf("%s --config=%s --stanza=%s stanza-create",
			p.Main, p.ConfigFile, stanzaName)), nil
}
func (p *PgBackRest) StanzaCreate(stanzaName string) (err error) {
	cmd, err := p.GenStanzaCreateCMD(stanzaName)
	if err != nil {
		return fmt.Errorf("(%s) generate stanza-create cmd failed: %w", stanzaName, err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf(
			"(%s) stanza-create failed: %w, cmd: %s, stderr: %s",
			stanzaName, err, cmd.String(), stderr.String())
	}
	log.Infof("(%s) stanza-create success: %s", stanzaName, cmd.String())
	return nil
}

// check
func (p *PgBackRest) GenCheckCMD(stanzaName string) (cmd *exec.Cmd, err error) {
	if stanzaName == "" {
		return nil, fmt.Errorf("empty stanza name")
	}
	return exec.Command("sh", "-c",
		fmt.Sprintf("%s --config=%s --stanza=%s check",
			p.Main, p.ConfigFile, stanzaName)), nil
}
func (p *PgBackRest) Check(stanzaName string) (err error) {
	cmd, err := p.GenCheckCMD(stanzaName)
	if err != nil {
		return fmt.Errorf("(%s) check cmd failed: %w", stanzaName, err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf(
			"(%s) check failed: %w, cmd: %s, stderr: %s",
			stanzaName, err, cmd.String(), stderr.String())
	}
	log.Infof("(%s) check success: %s", stanzaName, cmd.String())
	return nil
}

// backup
func (p *PgBackRest) GenBackupCMD(stanzaName string) (cmd *exec.Cmd, err error) {
	if stanzaName == "" {
		return nil, fmt.Errorf("empty stanza name")
	}
	return exec.Command("sh", "-c",
		fmt.Sprintf("%s --config=%s --stanza=%s --type=%s backup",
			p.Main, p.ConfigFile, stanzaName, p.Type)), nil
}
func (p *PgBackRest) Backup(stanzaName string) (err error) {
	cmd, err := p.GenBackupCMD(stanzaName)
	if err != nil {
		return fmt.Errorf("(%s) backup cmd failed: %w", stanzaName, err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf(
			"(%s) backup failed: %w, cmd: %s, stderr: %s",
			stanzaName, err, cmd.String(), stderr.String())
	}
	log.Infof("(%s) backup success: %s", stanzaName, cmd.String())
	return nil
}

// restore
func (p *PgBackRest) GenRestoreCMD(stanzaName string) (cmd *exec.Cmd, err error) {
	if stanzaName == "" {
		return nil, fmt.Errorf("empty stanza name")
	}
	switch p.Type {
	case "name", "xid", "time":
		cmd = exec.Command("sh", "-c",
			fmt.Sprintf("%s --config=%s --stanza=%s --type=%s --target='%s' restore",
				p.Main, p.ConfigFile, stanzaName, p.Type, p.Target))
	default:
		cmd = exec.Command("sh", "-c",
			fmt.Sprintf("%s --config=%s --stanza=%s --type=%s restore",
				p.Main, p.ConfigFile, stanzaName, p.Type))
	}
	return cmd, nil
}
func (p *PgBackRest) Restore(stanzaName string) (err error) {
	cmd, err := p.GenRestoreCMD(stanzaName)
	if err != nil {
		return fmt.Errorf("(%s) restore cmd failed: %w",
			stanzaName, err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf(
			"(%s) restore failed: %w, cmd: %s, stderr: %s",
			stanzaName, err, cmd.String(), stderr.String())
	}
	log.Infof("(%s) restore success: %s", stanzaName, cmd.String())
	return nil
}

// info
func (p *PgBackRest) GenInfoCMD(stanzaName string) (cmd *exec.Cmd, err error) {
	if stanzaName == "" {
		return nil, fmt.Errorf("empty stanza name")
	}
	if p.OutputFormat == "" {
		p.OutputFormat = "text"
	}
	switch p.OutputFormat {
	case "text", "json":
		return exec.Command("sh", "-c",
			fmt.Sprintf("%s --config=%s --stanza=%s --output=%s info",
				p.Main, p.ConfigFile, stanzaName, p.OutputFormat)), nil
	}
	return nil, fmt.Errorf("invalid output format %s", p.OutputFormat)
}
func (p *PgBackRest) Info(stanzaName string) (output string, err error) {
	cmd, err := p.GenInfoCMD(stanzaName)
	if err != nil {
		return "", fmt.Errorf("(%s) generate info cmd failed: %w", stanzaName, err)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf(
			"(%s) info failed: %w, cmd: %s, stderr: %s",
			stanzaName, err, cmd.String(), stderr.String())
	}
	log.Infof("(%s) info success: %s", stanzaName, cmd.String())
	return stdout.String(), nil
}
