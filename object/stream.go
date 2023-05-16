package object

import (
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"mybackup/storage"
	"mybackup/utils"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/klauspost/pgzip"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Snapshot MySQL physical backup result
type Stream struct {
	CTX              context.Context
	Interval         time.Duration
	InputName        string
	InputSize        int64
	OutputSize       int64
	InputSpeedLimit  string
	OutputSpeedLimit string
	CompressMethod   string
	DecompressMethod string
	UnpackMethod     string

	EncryptMethod string
	EncryptKey    string
	DecryptMethod string
	DecryptKey    string

	OutputName       string
	InputWriter      io.Writer
	OutputReader     io.Reader
	Ticker           *time.Ticker
	Timer            *utils.Timer
	WaitGroup        sync.WaitGroup
	Cancel           context.CancelFunc
	Err              error
	InputHash        hash.Hash
	OutputHash       hash.Hash
	compressedWriter *pgzip.Writer

	ExpectInputChecksum string
	InputChecksum       string
	OutputChecksum      string
}

func NewStream(ctx context.Context, interval time.Duration, inputName string) *Stream {
	if interval == 0 {
		interval = time.Minute
	}
	s := &Stream{
		InputName:   inputName,
		InputWriter: io.Discard,
		Interval:    interval,
		Ticker:      time.NewTicker(interval),
		Err:         nil,
		InputHash:   md5.New(),
		OutputHash:  md5.New(),
		InputSize:   -1,
		OutputSize:  -1,
		Timer:       utils.NewTimer(),
	}
	s.CTX, s.Cancel = context.WithCancel(ctx)
	return s
}

func (s *Stream) Flush() {
	if s.compressedWriter != nil {
		s.Err = s.compressedWriter.Flush()
		log.Infof("compressed writer flushed")
	}
}

func (s *Stream) in(st storage.Storage) (err error) {
	// 4. compress
	err = s.compress()
	if err != nil {
		log.Errorf("(%s) compress failed: %s", s.InputName, err)
		return err
	}
	// 3. limit speed
	err = s.limitInputSpeed()
	if err != nil {
		return err
	}
	// 2. decrypt
	s.Err = s.decrypt()
	if s.Err != nil {
		log.Errorf("(%s) decrypt failed: %s", s.OutputName, s.Err)
		return
	}
	// 1. get input hash
	s.InputWriter = io.MultiWriter(s.InputWriter, s.InputHash)
	return nil
}

func (s *Stream) out(u utils.Utils, st storage.Storage, overwrite bool) (err error) {
	s.WaitGroup.Add(1)
	go func() {
		defer s.WaitGroup.Done()
		// 1. decompress
		s.Err = s.decompress()
		if s.Err != nil {
			log.Errorf("(%s) decompress failed: %s", s.OutputName, s.Err)
			return
		}
		// 2. limit speed
		s.Err = s.limitOutputSpeed()
		if s.Err != nil {
			return
		}
		// 3. encrypt
		s.Err = s.encrypt()
		if s.Err != nil {
			log.Errorf("(%s) encrypt failed: %s", s.OutputName, s.Err)
			return
		}
		// 4. get output hash
		s.OutputReader = io.TeeReader(s.OutputReader, s.OutputHash)
		// 5. extract
		s.Err = s.extract(u, st)
		if s.Err != nil {
			s.Cancel()
			log.Errorf("(%s) extract failed: %s", s.OutputName, s.Err)
			return
		}
		// get and check checksum
		s.InputChecksum = fmt.Sprintf("%x", s.InputHash.Sum(nil))
		log.Infof("the md5sum of input stream is: %s", s.OutputChecksum)
		if s.ExpectInputChecksum != "" && s.InputChecksum != s.ExpectInputChecksum {
			s.Err = fmt.Errorf("input checksum (%s) should match expecting checksum (%s)", s.InputChecksum, s.ExpectInputChecksum)
			log.Errorf("%v", s.Err)
			return
		}
		s.OutputChecksum = fmt.Sprintf("%x", s.OutputHash.Sum(nil))
		log.Infof("the md5sum of output stream before extracting is %s", s.OutputChecksum)
	}()
	return nil
}

func (s *Stream) compress() (err error) {
	switch s.CompressMethod {
	case "gzip":
		s.compressedWriter = pgzip.NewWriter(s.InputWriter)
		err = s.compressedWriter.SetConcurrency(128*1024, 8)
		if err != nil {
			return err
		}
		s.InputWriter = s.compressedWriter
		log.Infof("(%s) compressing with pgzip",
			s.InputName)
	case "":
		return nil
	default:
		return fmt.Errorf("unsupport compress method")
	}
	return nil
}

func (s *Stream) decompress() (err error) {
	switch s.DecompressMethod {
	case "gzip":
		s.OutputReader, err = pgzip.NewReaderN(s.OutputReader, 128*1024, 8)
		if err != nil {
			return err
		}
		log.Infof("(%s) decompressing with pgzip", s.OutputName)
	case "":
		s.OutputSize = s.InputSize
		return nil
	default:
		return fmt.Errorf("unsupport decompress method")
	}
	return nil
}

func (s *Stream) encrypt() (err error) {
	switch s.EncryptMethod {
	case "":
		log.Info("no encryption stream")
	default:
		if !utils.Contains(utils.SupportEncryptMethod, s.EncryptMethod) {
			return fmt.Errorf("unsupport encrypt method: %s", s.EncryptMethod)
		}
		if len(s.EncryptKey) != 32 {
			return fmt.Errorf("invalid encrypt key length: %d", len(s.EncryptKey))
		}
		s.OutputReader, err = utils.CryptedReader(s.EncryptKey, s.OutputReader)
		if err != nil {
			return fmt.Errorf("encrypt stream failed: %w", err)
		}
		log.Infof("encrypt method: %s", s.EncryptMethod)
	}
	return nil
}

func (s *Stream) decrypt() (err error) {
	switch s.DecryptMethod {
	case "":
		log.Info("no decryption stream")
	default:
		if !utils.Contains(utils.SupportEncryptMethod, s.DecryptMethod) {
			return fmt.Errorf("unsupport decrypt method: %s", s.DecryptMethod)
		}
		if len(s.DecryptKey) != 32 {
			return fmt.Errorf("invalid decrypt key length: %d", len(s.DecryptKey))
		}
		s.InputWriter, err = utils.CryptedWriter(s.DecryptKey, s.InputWriter)
		if err != nil {
			return fmt.Errorf("decrypt stream failed: %w", err)
		}
		log.Infof("decrypt method: %s", s.DecryptMethod)
	}
	return nil
}

func (s *Stream) limitInputSpeed() (err error) {
	if s.InputSpeedLimit == "" {
		s.InputSpeedLimit = "80M"
		log.Infof(
			"(%s) input speed limit is default %s/s",
			s.InputName, s.InputSpeedLimit,
		)
	} else {
		log.Infof(
			"(%s) input speed limit is %s/s",
			s.InputName, s.InputSpeedLimit,
		)
	}
	s.InputWriter, err = utils.LimitWriter(s.InputWriter, s.InputSpeedLimit)
	if err != nil {
		return fmt.Errorf("(%s) limit input speed to %s failed: %w",
			s.InputName, s.InputSpeedLimit, err)
	}
	// show progress
	s.InputWriter = utils.NewProgressWriter(s.CTX, s.InputWriter, s.InputName, s.InputSize, s.Timer, s.Interval)
	return nil
}

func (s *Stream) limitOutputSpeed() (err error) {
	if s.OutputSpeedLimit == "" {
		s.OutputSpeedLimit = "160M"
		log.Infof(
			"(%s) output speed limit is default %s/s",
			s.OutputName, s.OutputSpeedLimit,
		)
	} else {
		log.Infof(
			"(%s) output speed limit is %s/s",
			s.OutputName, s.OutputSpeedLimit,
		)
	}
	s.OutputReader, err = utils.LimitReader(s.OutputReader, s.OutputSpeedLimit)
	if err != nil {
		return fmt.Errorf("(%s) limit output speed to %s failed: %w",
			s.OutputName, s.OutputSpeedLimit, err)
	}
	// show progress
	s.OutputReader = utils.NewProgressReader(s.CTX, s.OutputReader, s.OutputName, s.OutputSize, s.Timer, s.Interval)
	return nil
}

func (s *Stream) extract(u utils.Utils, st storage.Storage) (err error) {
	if st.GetType() == "fs" && s.UnpackMethod != "" {
		// extract by xbstream
		switch s.UnpackMethod {
		case "xbstream":
			if u.Xbstream == nil {
				return fmt.Errorf("no xbstream set")
			}
			// init xbstream members
			u.Xbstream.Directory = st.GetFilePath()
			u.Xbstream.Action = "extract"
			if u.Xbstream.Parallel <= 0 {
				u.Xbstream.Parallel = 4
			}
			u.Xbstream.DecompressSwitch = 1
			if u.Xbstream.DecompressThreads <= 0 {
				u.Xbstream.DecompressThreads = 2
			}
			// generate xbstream command
			err = u.Xbstream.GenerateCMD()
			if err != nil {
				return fmt.Errorf("generate xbstream command failed: %w", err)
			}
			u.Xbstream.CMD.Stdin = s.OutputReader
			u.Xbstream.CMD.Stdout = nil
			// run command
			if err = u.Xbstream.CMD.Run(); err != nil {
				return fmt.Errorf("run xbstream command failed: %w", err)
			}
		case "tar":
			s.OutputSize, err = utils.Untar(s.OutputReader, st.GetFilePath())
			if err != nil {
				return fmt.Errorf("untar failed: %w", err)
			}
			log.Infof("untared %d succeeded", s.OutputSize)
		default:
			return fmt.Errorf("unsupport unpack method: %s", s.UnpackMethod)
		}
	} else {
		s.OutputSize, err = st.ReadFrom(s.OutputReader)
		if err != nil {
			return fmt.Errorf("(%s) read from stream failed: %w",
				s.OutputName, err)
		}
	}
	return nil
}

func (s *Stream) prepareStorage(st storage.Storage, defaultFilename string, overwrite bool) (err error) {
	// set storage
	switch st := st.(type) {
	case *storage.FS:
		log.Debugf("target storage base directory: %s", filepath.Dir(st.FilePath))
	case *storage.S3:
		if s.InputSize <= 0 {
			s.InputSize = storage.S3_MAX_UPLOAD_SIZE
		}
		s3PartSize := int64(math.Ceil(float64(s.InputSize)/s3manager.MaxUploadParts - 5))
		if s3PartSize > storage.S3_MAX_PART_SIZE {
			s3PartSize = storage.S3_MAX_PART_SIZE
		}
		concurrency := int(s.InputSize / s3manager.MinUploadPartSize)
		err = st.InitS3Parameter(s3PartSize, concurrency)
		if err != nil {
			return err
		}
	case *storage.TCP:
		err = st.InitClient()
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupport storage")
	}
	// set target file path
	targetFilePath := st.GetFilePath()
	if targetFilePath == "" {
		targetPrefix := st.GetPrefix()
		if targetPrefix == "" {
			return fmt.Errorf(
				"neither FilePath nor Prefix was specified",
			)
		}
		if strings.HasSuffix("/", targetPrefix) {
			targetFilePath = filepath.Join(targetPrefix, defaultFilename)
		} else {
			targetFilePath = fmt.Sprintf("%s%s", targetPrefix, defaultFilename)
		}
		st.SetFilePath(targetFilePath)
	}
	log.Debugf("target base directory: %s", filepath.Dir(targetFilePath))
	log.Infof("to %s filepath: %s", st.GetType(), targetFilePath)
	// check if target file existance
	targetFileInfo, err := st.GetFileInfo()
	if err != nil {
		return errors.Wrap(
			err,
			fmt.Sprintf("(%s) get file stat failed",
				s.OutputName))
	}
	if targetFileInfo.FileStatus == "existent" {
		if !overwrite {
			return errors.Wrap(
				os.ErrExist,
				fmt.Sprintf("(%s) failed", s.OutputName))
		}
		err = st.DeleteFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Stream) Store(u utils.Utils, st storage.Storage, defaultFilename string, overwrite bool) (err error) {
	// prepare storage
	err = s.prepareStorage(st, defaultFilename, overwrite)
	if err != nil {
		return err
	}
	// calculate time
	defer func() {
		s.Timer.Stop()
		log.Infof("(%s) duration: %s",
			s.InputName,
			s.Timer.Duration.String(),
		)
	}()
	s.OutputName = filepath.Base(st.GetFilePath())
	pipeOut, pipeIn := io.Pipe()
	s.InputWriter = pipeIn
	s.OutputReader = pipeOut
	// input stream
	err = s.in(st)
	if err != nil {
		return err
	}
	// close the PipeWriter after the stream receive done signal to trigger EOF
	s.WaitGroup.Add(1)
	go func() {
		defer s.WaitGroup.Done()
		defer pipeIn.Close()
		// close compressedWriter or data will not flush
		defer s.Flush()
		<-s.CTX.Done()
	}()
	// output stream
	err = s.out(u, st, overwrite)
	if err != nil {
		return err
	}
	return nil
}
