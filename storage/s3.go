package storage

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mybackup/utils"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dustin/go-humanize"
	"github.com/hako/durafmt"
	log "github.com/sirupsen/logrus"
)

const (
	S3_MAX_PART_SIZE   int64 = 5 * 1024 * 1024 * 1024                      // 5GB
	S3_MAX_UPLOAD_SIZE int64 = S3_MAX_PART_SIZE * s3manager.MaxUploadParts // 50000GB(48.8TB)
	S3_MIN_CONCURRENCY int   = 1
	S3_MAX_CONCURRENCY int   = 5
)

// customRetryer implements the `request.Retryer` interface and allows for
// customization of the retry behavior of an AWS client.
type customRetryer struct {
	client.DefaultRetryer
}

// isErrReadConnectionReset returns true if the underlying error is a read
// connection reset error.
//
// NB: A read connection reset error is thrown when the SDK is unable to read
// the response of an underlying API request due to a connection reset. The
// DefaultRetryer in the AWS SDK does not treat this error as a retryable error
// since the SDK does not have knowledge about the idempotence of the request,
// and whether it is safe to retry -
// https://github.com/aws/aws-sdk-go/pull/2926#issuecomment-553637658.
//
// In mybackup all operations with s3 (read, write, list) are considered idempotent,
// and so we can treat the read connection reset error as retryable too.
func isErrReadConnectionReset(err error) bool {
	// The error string must match the one in
	// github.com/aws/aws-sdk-go/aws/request/connection_reset_error.go. This is
	// unfortunate but the only solution until the SDK exposes a specialized error
	// code or type for this class of errors.
	return err != nil && strings.Contains(err.Error(), "read: connection reset")
}

// ShouldRetry implements the request.Retryer interface.
func (cr *customRetryer) ShouldRetry(r *request.Request) bool {
	return cr.DefaultRetryer.ShouldRetry(r) || isErrReadConnectionReset(r.Error)
}

// FakeWriterAt to satisfy s3 downloader
type FakeWriterAt struct {
	w io.Writer
}

// WriteAt ignore offset just write
func (fw *FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

// S3 s3 interface config only support for only one object
type S3 struct {
	Type           string `json:"type,omitempty"`
	Endpoint       string `json:"endpoint,omitempty"`
	Region         string `json:"region,omitempty"`
	AccessKey      string `json:"access_key,omitempty"`
	SecretKey      string `json:"secret_key,omitempty"`
	Token          string `json:"token,omitempty"`
	PartSize       int64  `json:"part_size,omitempty"`
	Concurrency    int    `json:"concurrency,omitempty"`
	ExtraFiles     []File `json:"extra_files,omitempty"`
	EnableSSL      bool   `json:"enable_ssl,omitempty"`
	SecureTLS      bool   `json:"secure_tls,omitempty"`
	MaxRetries     int    `json:"max_retries,omitempty"`
	CertFile       string `json:"cert_file,omitempty"`
	ClientCertFile string `json:"client_cert_file,omitempty"`
	ClientKeyFile  string `json:"client_key_file,omitempty"`
	ForcePathStyle bool   `json:"force_path_style,omitempty"`
	Client         *s3.S3 `json:"-"`
	File
}

// user defined unmarshall method
func (s *S3) UnmarshalJSON(data []byte) (err error) {
	items := make(map[string]json.RawMessage)
	err = json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	s.ForcePathStyle = true
	for k, v := range items {
		switch k {
		case "type":
			err = json.Unmarshal(v, &s.Type)
			if err != nil {
				return err
			}
		case "endpoint":
			err = json.Unmarshal(v, &s.Endpoint)
			if err != nil {
				return err
			}
		case "region":
			err = json.Unmarshal(v, &s.Region)
			if err != nil {
				return err
			}
		case "access_key":
			err = json.Unmarshal(v, &s.AccessKey)
			if err != nil {
				return err
			}
		case "secret_key":
			err = json.Unmarshal(v, &s.SecretKey)
			if err != nil {
				return err
			}
		case "token":
			err = json.Unmarshal(v, &s.Token)
			if err != nil {
				return err
			}
		case "bucket":
			err = json.Unmarshal(v, &s.Bucket)
			if err != nil {
				return err
			}
		case "file_path":
			err = json.Unmarshal(v, &s.FilePath)
			if err != nil {
				return err
			}
		case "prefix":
			err = json.Unmarshal(v, &s.Prefix)
			if err != nil {
				return err
			}
		case "extra_files":
			err = json.Unmarshal(v, &s.ExtraFiles)
			if err != nil {
				return err
			}
		case "part_size":
			err = json.Unmarshal(v, &s.PartSize)
			if err != nil {
				return err
			}
		case "concurrency":
			err = json.Unmarshal(v, &s.Concurrency)
			if err != nil {
				return err
			}
		case "storage_class":
			err = json.Unmarshal(v, &s.StorageClass)
			if err != nil {
				return err
			}
		case "enable_ssl":
			err = json.Unmarshal(v, &s.EnableSSL)
			if err != nil {
				return err
			}
		case "secure_tls":
			err = json.Unmarshal(v, &s.SecureTLS)
			if err != nil {
				return err
			}
		case "cert_file":
			err = json.Unmarshal(v, &s.CertFile)
			if err != nil {
				return err
			}
		case "client_cert_file":
			err = json.Unmarshal(v, &s.ClientCertFile)
			if err != nil {
				return err
			}
		case "client_key_file":
			err = json.Unmarshal(v, &s.ClientKeyFile)
			if err != nil {
				return err
			}
		case "list_limit":
			err = json.Unmarshal(v, &s.ListLimit)
			if err != nil {
				return err
			}
		case "force_path_style":
			err = json.Unmarshal(v, &s.ForcePathStyle)
			if err != nil {
				return err
			}
		case "max_retries":
			err = json.Unmarshal(v, &s.MaxRetries)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognize field: %s", k)
		}
	}
	// init s3 client
	err = s.InitClient()
	return err
}

func (s *S3) InitS3Parameter(partSize int64, concurrency int) error {
	// set PartSize
	s.PartSize = partSize
	if s.PartSize < s3manager.MinUploadPartSize {
		s.PartSize = s3manager.MinUploadPartSize
	} else if s.PartSize > S3_MAX_PART_SIZE {
		return fmt.Errorf(
			"set part size(%s), exceeded max part size(%s)",
			humanize.Bytes(uint64(s.PartSize)),
			humanize.Bytes(uint64(S3_MAX_PART_SIZE)),
		)
	}
	log.Infof("s3 PartSize: %s", humanize.Bytes(uint64(s.PartSize)))

	// set Concurrency
	s.Concurrency = concurrency
	if s.Concurrency > S3_MAX_CONCURRENCY {
		s.Concurrency = S3_MAX_CONCURRENCY
	} else if s.Concurrency < S3_MIN_CONCURRENCY {
		s.Concurrency = S3_MIN_CONCURRENCY
	}
	log.Infof("s3 Concurrency: %d", s.Concurrency)
	return nil
}

// InitClient init s3 client
func (s *S3) InitClient() (err error) {
	/*
		// upgrade to aws-sdk-go-v2
		cfg, err := config.LoadDefaultConfig(
			context.TODO(),
			config.WithRegion(s.Region),
			config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     s.AccessKey,
					SecretAccessKey: s.SecretKey,
					SessionToken:    "",
				},
			}),
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{URL: s.Endpoint, Source: aws.EndpointSourceCustom}, nil
				})),
		)
		if err != nil {
			return err
		}

		s.Client = s3.NewFromConfig(
			cfg,
			func(o *s3.Options) {
				o.UsePathStyle = true
			},
		)
	*/
	if s.MaxRetries == 0 {
		s.MaxRetries = 5
	}
	retryer := &customRetryer{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries: s.MaxRetries,
		},
	}
	config := &aws.Config{
		Region:           aws.String(s.Region),
		Endpoint:         aws.String(s.Endpoint),
		Credentials:      credentials.NewStaticCredentials(s.AccessKey, s.SecretKey, s.Token),
		S3ForcePathStyle: aws.Bool(s.ForcePathStyle),
		DisableSSL:       aws.Bool(!s.EnableSSL),
		Retryer:          retryer,
	}

	if !*config.DisableSSL {
		var tlsConfig *tls.Config
		tlsConfig, err = s.getTLSConfig()
		if err != nil {
			return err
		}
		config.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
	}

	// The session s3 Uploader will use
	sess, err := session.NewSessionWithOptions(session.Options{Config: *config})
	if err != nil {
		return err
	}

	s.Client = s3.New(sess)
	if s.PartSize <= 0 {
		s.PartSize = 300 * 1024 * 1024
	}
	if s.Concurrency <= 0 {
		s.Concurrency = 5
	}
	return nil
}

func (s *S3) getTLSConfig() (tlsConfig *tls.Config, err error) {
	if !s.SecureTLS {
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	var clientCert tls.Certificate
	if s.ClientCertFile != "" && s.ClientKeyFile != "" {
		clientCert, err = tls.LoadX509KeyPair(s.ClientCertFile, s.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("create x509 keypair from client cert file %s and client key file %s failed: %w",
				s.ClientCertFile, s.ClientKeyFile, err)
		}
	}

	cert, err := os.ReadFile(s.CertFile)
	if err != nil {
		return nil, fmt.Errorf("open cert file %s failed: %w",
			s.CertFile, err)
	}
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(cert)

	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
	}, nil
}

func (s *S3) GetType() string {
	return s.Type
}

func (s *S3) GetFilePath() string {
	return s.FilePath
}

func (s *S3) SetFilePath(filepath string) {
	s.FilePath = filepath
}

func (s *S3) GetPrefix() string {
	return s.Prefix
}

func (s *S3) SetPrefix(prefix string) {
	s.Prefix = prefix
}

// ReadFrom read from reader and write to s3 object
func (s *S3) ReadFrom(r io.Reader) (n int64, err error) {
	return s.StreamUploadObject(r)
}

// WriteTo read from s3 object and write to writer
func (s *S3) WriteTo(w io.Writer) (n int64, err error) {
	return s.StreamDonwloadObject(w)
}

// list expired by datetime
func (s *S3) ListExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error) {
	FileList := []File{
		{
			Bucket: s.Bucket,
			Prefix: s.Prefix,
		},
	}
	FileList = append(FileList, s.ExtraFiles...)

	// check list limit
	if s.ListLimit == 0 {
		s.ListLimit = DEFAULT_LIST_LIMIT
		log.Infof(
			"set default list_limit: %d",
			DEFAULT_LIST_LIMIT,
		)
	} else if s.ListLimit > MAX_LIST_LIMIT {
		return nil, fmt.Errorf("current list_limit(%d) exceeded max value: %d",
			s.ListLimit, MAX_LIST_LIMIT)
	}

	for _, file := range FileList {
		pattern := fmt.Sprintf("%s*", file.Prefix)
		log.Infof(
			"s3 objects pattern: %s",
			pattern,
		)
		// list objects
		input := &s3.ListObjectsInput{
			Bucket: aws.String(file.Bucket),
			Prefix: aws.String(file.Prefix),
		}
		var pageNumber, fileCount uint32
		var reachListLimit bool
		err = s.Client.ListObjectsPages(
			input,
			func(output *s3.ListObjectsOutput, _ bool) (shouldContinue bool) {
				log.Infof("page: %d", pageNumber)
				pageNumber++

				// check modified time
				for _, o := range output.Contents {
					if o.LastModified.Before(expireDatetime.Time) {
						f := File{
							FilePath:   *(o.Key),
							Bucket:     file.Bucket,
							Prefix:     file.Prefix,
							ModifyTime: o.LastModified.Local(),
							Size:       *(o.Size),
							FileStatus: "existent",
						}
						if o.StorageClass != nil {
							f.StorageClass = *(o.StorageClass)
						}
						files = append(files, f)
						fileCount++
						if fileCount >= s.ListLimit {
							reachListLimit = true
							return false
						}
					}
				}
				return true
			},
		)
		if err != nil {
			return nil, err
		}
		if reachListLimit {
			log.Infof("file count reach the list limit(%d)", s.ListLimit)
			continue
		}
	}

	// sort files by ModifyTime
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModifyTime.After(files[j].ModifyTime)
	})

	// print files
	for _, f := range files {
		log.Infof(
			"bucket: %s, storage class: %s, file name: %s, last modify time: %s, size: %s",
			f.Bucket,
			f.StorageClass,
			f.FilePath,
			f.ModifyTime.Format("2006-01-02 15:04:05"),
			humanize.Bytes(uint64(f.Size)),
		)
	}

	return files, nil
}

// list expired objects
func (s *S3) ListExpiredByDuration(expireDuration time.Duration) (files []File, err error) {
	// set base time
	baseTime := time.Now()

	// get expire time
	expireDatetime := utils.NewTime(baseTime.Add(-expireDuration))

	return s.ListExpiredByDatetime(expireDatetime)
}

// Delete expired objects
func (s *S3) DeleteExpiredByDatetime(expireDatetime *utils.SQLNullTime) (files []File, err error) {
	if s.Prefix == "" {
		return nil, fmt.Errorf("can not remove expired files without prefix")
	}
	baseTime := time.Now()

	// check if expire datetime in one day, reset expireDatetime
	earliestExpireDatetime := baseTime.Add(-MIN_EXPIRE_DURATION)
	if expireDatetime.Time.After(earliestExpireDatetime) {
		expireDatetime.SetTime(earliestExpireDatetime)
		log.Warnf("given expire datetime must before %s, reset expire_datetime to %s",
			durafmt.Parse(MIN_EXPIRE_DURATION).String(), expireDatetime.Time)
	}

	expiredFileList, err := s.ListExpiredByDatetime(expireDatetime)
	if err != nil {
		return nil, err
	}

	// check modified time and delete
	for _, f := range expiredFileList {
		request := &s3.DeleteObjectInput{
			Bucket: aws.String(f.Bucket),
			Key:    aws.String(f.FilePath),
		}
		_, warn := s.Client.DeleteObject(request)
		if warn != nil {
			log.Warnf("delete file %s failed", f.FilePath)
			f.FileStatus = "failed to delete"
			continue
		}
		f.FileStatus = "deleted"
		files = append(files, f)
	}
	return files, nil
}

// Delete expired objects
func (s *S3) DeleteExpiredByDuration(expireDuration time.Duration) (files []File, err error) {
	// set base time
	baseTime := time.Now()

	// get expire time
	expireDatetime := utils.NewTime(baseTime.Add(-expireDuration))

	return s.DeleteExpiredByDatetime(expireDatetime)
}

// StreamUploadS3Object upload s3 object from reader
func (s *S3) StreamUploadObject(uploadReader io.Reader) (n int64, err error) {
	//     uploader := s3manager.NewUploaderWithClient(s3Svc, func(u *s3manager.Uploader) {
	//          u.PartSize = 64 * 1024 * 1024 // 64MB per part
	//     }
	// Create an uploader with the session and default option
	poolSize := 5 * 1024 * 1024 // 5 MiB
	uploader := s3manager.NewUploaderWithClient(
		s.Client,
		func(u *s3manager.Uploader) {
			// Define a strategy that will buffer 25 MiB in memory
			u.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(poolSize)
		})
	uploader.PartSize = s.PartSize
	uploader.Concurrency = s.Concurrency

	log.Infof("S3 upload max retries: %d", s.Client.MaxRetries())

	// create uploaderInput
	uploaderInput := &s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.FilePath),
		Body:   uploadReader,
	}

	// set storage class
	if s.StorageClass != "" {
		uploaderInput.StorageClass = aws.String(s.StorageClass)
	}

	// Streaming upload the file to S3.
	_, err = uploader.Upload(uploaderInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchUpload:
				// when encountering ErrCodeNoSuchUpload, maybe the file is completed sucessfully
				// then log warning and try HeadObject to confirm
				log.Warnf("error: %s, try recover by head the object", aerr.Error())
			default:
				return 0, aerr
			}
		} else {
			return 0, err
		}
	}

	// get resutl
	result, err := s.HeadObject()
	if err != nil {
		return 0, err
	}
	s.ModifyTime = *result.LastModified
	s.Size = *result.ContentLength
	if result.StorageClass != nil {
		s.StorageClass = *result.StorageClass
	}
	return *result.ContentLength, err
}

// StreamDonwloadS3Object download s3 object to WriterAt
func (s *S3) StreamDonwloadObject(w io.Writer) (n int64, err error) {
	poolSize := 5 * 1024 * 1024 // 5 MiB
	downloader := s3manager.NewDownloaderWithClient(
		s.Client,
		func(d *s3manager.Downloader) {
			d.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(poolSize)
		})
	downloader.PartSize = s.PartSize
	// download the parts sequentially for streaming
	downloader.Concurrency = 1

	// wrap downloadWriter as FakeWriterAt for downloader
	downloadWriter := &FakeWriterAt{w: w}
	/*
		// convert io.Writer to io.WriterAt
		stream := iostream.OpenWriterAtStream(w, int(downloader.PartSize), downloader.Concurrency+3)
		defer stream.Close()
	*/

	// Streaming download from S3.
	n, err = downloader.Download(
		downloadWriter,
		&s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(s.FilePath),
		})
	return n, err
}

// HeadS3Object get s3 object info from head
func (s *S3) HeadObject() (hOutput *s3.HeadObjectOutput, err error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.FilePath),
	}

	hOutput, err = s.Client.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				err = aerr
			}
		}
	}
	return hOutput, err
}

// get s3 file data by offset
func (s *S3) ReadOffset(off, n int64) (buf []byte, err error) {
	fileRange := fmt.Sprintf("bytes=%d-%d", off, off+n)
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.FilePath),
		Range:  aws.String(fileRange),
	}
	output, err := s.Client.GetObject(input)
	if err != nil {
		return nil, fmt.Errorf("read s3 file range(%s) faild: %w",
			fileRange, err)
	}

	buf = make([]byte, n)
	_, err = output.Body.Read(buf)
	if !errors.Is(err, io.EOF) && err != nil {
		return nil, fmt.Errorf("read s3 output to buffer faild: %w", err)
	}
	return buf, nil
}

// get file content type by reading first 512 bytes
func (s *S3) GetContentType() (contentType string, err error) {
	var n int64 = 512
	if s.Size <= n {
		n = s.Size
	}
	buf, err := s.ReadOffset(0, n)
	if err != nil {
		return "", fmt.Errorf("read s3 output to buffer faild: %w", err)
	}
	contentType = http.DetectContentType(buf)
	return contentType, nil
}

// get uncompressed size of a gzip compressed file by last 4 bytes
func (s *S3) GetGzipUncompressedSize() (uncompressedSize uint32, err error) {
	n := 4
	if s.Size <= int64(n) {
		return 0, fmt.Errorf("invalid object size: %d", s.Size)
	}
	buf, err := s.ReadOffset(s.Size-int64(n), int64(n))
	if err != nil {
		return 0, fmt.Errorf("read s3 output to buffer faild: %w", err)
	}
	if len(buf) == n {
		uncompressedSize = binary.LittleEndian.Uint32(buf)
	}
	return uncompressedSize, nil
}

// get file info
func (s *S3) GetFileInfo() (fileInfo *File, err error) {
	o, err := s.HeadObject()
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
				s.FileStatus = "non-existent"
				return &s.File, nil
			default:
				return nil, err
			}
		}
	}
	s.Size = *o.ContentLength
	s.ModifyTime = *o.LastModified
	s.FileType, err = s.GetContentType()
	if err != nil {
		return nil, err
	}
	s.Size = *o.ContentLength
	if s.FileType == "application/x-gzip" {
		uncompressedSize, err := s.GetGzipUncompressedSize()
		if err != nil {
			return nil, err
		}
		s.UncompressedSize = int64(uncompressedSize)
	}
	s.Count = 1
	if o.StorageClass != nil {
		s.StorageClass = *o.StorageClass
	}
	s.FileStatus = "existent"
	return &s.File, nil
}

// delete file
func (s *S3) DeleteFile() (err error) {
	request := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.FilePath),
	}
	log.Infof("deleting file: %s", s.FilePath)
	_, err = s.Client.DeleteObject(request)
	return err
}
