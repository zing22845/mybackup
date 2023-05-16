package utils

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"fmt"
	"io"
)

// SupportEncryptMethod for encrypt and decrypt
var (
	SupportEncryptMethod = []string{"AES256"}
)

// cipher block
func newBlock(key string) (cipher.Block, error) {
	hash := md5.Sum([]byte(key))
	block, err := aes.NewCipher(hash[:])
	if err != nil {
		return nil, err
	}
	return block, nil
}

// CryptedReader wraps r with an OFB cipher stream, encrypt and decrypt method are same.
func CryptedReader(key string, in io.Reader) (out io.Reader, err error) {
	if len([]byte(key)) < aes.BlockSize {
		return nil, fmt.Errorf("invalid key")
	}
	// get iv by key
	iv := []byte(key)[:aes.BlockSize]
	// get cipher block
	block, err := newBlock(key)
	if err != nil {
		return nil, err
	}
	// crypt the stream reader
	out = &cipher.StreamReader{
		S: cipher.NewOFB(block, iv),
		R: in,
	}
	return out, nil
}

// CryptedWriter wraps w with an OFB cipher stream, encrypt and decrypt method are same.
func CryptedWriter(key string, out io.Writer) (in io.Writer, err error) {
	if len([]byte(key)) < aes.BlockSize {
		return nil, fmt.Errorf("invalid key")
	}
	// get iv by key
	iv := []byte(key)[:aes.BlockSize]
	// get cipher block
	block, err := newBlock(key)
	if err != nil {
		return nil, err
	}
	// crypt the stream writer
	in = &cipher.StreamWriter{
		S: cipher.NewOFB(block, iv),
		W: out,
	}
	return in, nil
}
