// Copyright (C) 2016 The Syncthing Authors.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The S3Filesystem implements all aspects by delegating to package os.
// All paths are relative to the root and cannot (should not) escape the root directory.
type S3Filesystem struct {
	bucket    string
	keyPrefix string
	client    *s3.S3
}

func newS3Filesystem(uri string) *S3Filesystem {
	f, _ := newS3FilesystemBucket("jewart-syncthing-test", "sync")
	return f
}

func newS3FilesystemBucket(bucket string, keyPrefix string) (*S3Filesystem, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err != nil {
		fmt.Printf("Can't connect to AWS....\n")
		return nil, err
	}

	// Create S3 service client
	client := s3.New(sess)

	//_, err = client.CreateBucket(&s3.CreateBucketInput{
	//	Bucket: aws.String(bucket),
	//})
	//if err != nil {
	//		return nil, err
	//}

	// Wait until bucket is created before finishing
	//fmt.Printf("Waiting for bucket %q to be created...\n", bucket)

	_ = client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	return &S3Filesystem{
		bucket:    bucket,
		keyPrefix: keyPrefix,
		client:    client,
	}, nil
}

func (f *S3Filesystem) unrooted(path string) string {
	return s3rel(path, f.keyPrefix)
}

func (f *S3Filesystem) rooted(path string) string {
	if strings.HasPrefix(path, f.keyPrefix+"/") {
		return path
	} else {
		return f.keyPrefix + "/" + path
	}
}

func (f *S3Filesystem) unrootedSymlinkEvaluated(path string) string {
	return s3rel(path, f.keyPrefix)
}

func s3rel(path, prefix string) string {
	return strings.TrimPrefix(strings.TrimPrefix(path, prefix), string(PathSeparator))
}

// No such operation...
func (f *S3Filesystem) Chmod(name string, mode FileMode) error {
	return nil
}

func (f *S3Filesystem) CreateSymlink(src string, dst string) error {
	return nil
}

func (f *S3Filesystem) ReadSymlink(name string) (string, error) {
	return "", nil
}

func (f *S3Filesystem) Hide(name string) error {
	return nil
}

func (f *S3Filesystem) Unhide(name string) error {
	return nil
}

func (f *S3Filesystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}

func (f *S3Filesystem) Mkdir(name string, perm FileMode) error {
	client := f.client
	dirKey := f.rooted(name) + "/"
	fmt.Printf("S3 mkdir: %s\n", dirKey)

	input := &s3.PutObjectInput{
		Body:   nil,
		Bucket: aws.String(f.bucket),
		Key:    aws.String(dirKey),
	}

	_, err := client.PutObject(input)

	if err != nil {
		return err
	}

	return nil
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error.
// The permission bits perm are used for all directories that MkdirAll creates.
// If path is already a directory, MkdirAll does nothing and returns nil.
func (f *S3Filesystem) MkdirAll(path string, perm FileMode) error {
	fmt.Printf("S3FS MkdirAll: %s\n", path)
	f.Mkdir(path, perm)
	return nil
}

// Fake stat()
func (fs *S3Filesystem) Lstat(name string) (FileInfo, error) {
	fmt.Printf("S3 Lstat: %s\n", name)
	key := name

	if name == "." {
		key = fs.keyPrefix + "/"
	} else {
		key = fs.rooted(name)
	}

	trimmed := strings.TrimPrefix(filepath.Clean(key), "/")

	if trimmed == fs.keyPrefix {
		fmt.Printf("Head on %s\n", key)
		headObj, err := fs.client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(key),
		})

		if err != nil {
			return nil, err
		}

		return s3FileStat{
			name:        ".",
			size:        0,
			mode:        0777,
			isDirectory: true,
			modTime:     *headObj.LastModified,
		}, nil
	}

	rootedName := fs.rooted(name)
	stat, err := s3stat(fs.client, fs.bucket, rootedName, name)

	if err != nil {
		return nil, err
	}

	if stat.IsDir() || stat.Size() > 0 {
		return stat, nil
	} else {
		meta, err := fs.GetMetadata(name)
		if err != nil {
			return nil, err
		}
		st := s3FileStat{
			name:        stat.Name(),
			size:        meta.Size,
			mode:        0777,
			isDirectory: stat.IsDir(),
			modTime:     stat.ModTime(),
		}
		return st, nil
	}

}

func (f *S3Filesystem) Remove(name string) error {
	_, err := f.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.rooted(name)),
	})

	if err != nil {
		return err
	}

	return nil
}

func (f *S3Filesystem) RemoveAll(name string) error {
	// TODO
	return nil
}

func (f *S3Filesystem) Rename(oldpath, newpath string) error {

	fmt.Printf("S3FS Rename %s -> %s\n", oldpath, newpath)
	src := f.rooted(oldpath)
	target := f.rooted(newpath)

	copySrc := fmt.Sprintf("%s/%s", f.bucket, src)

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(f.bucket),
		CopySource: aws.String(copySrc),
		Key:        aws.String(target),
	}

	// copy first
	fmt.Printf("S3FS Copying %s -> %s\n", copySrc, target)
	_, err := f.client.CopyObject(input)
	if err != nil {
		return err
	}

	fmt.Printf("S3FS Removing %s\n", src)
	_, err = f.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(src),
	})

	if err != nil {
		return err
	}

	// Move metadata
	srcMetadataKey := fmt.Sprintf(".metadata/%s", src)
	tgtMetadataKey := fmt.Sprintf(".metadata/%s", target)
	copyMetadataSrc := fmt.Sprintf("%s/%s", f.bucket, srcMetadataKey)

	mdInput := &s3.CopyObjectInput{
		Bucket:     aws.String(f.bucket),
		CopySource: aws.String(copyMetadataSrc),
		Key:        aws.String(tgtMetadataKey),
	}

	// copy first
	fmt.Printf("S3FS Copying %s -> %s\n", copyMetadataSrc, tgtMetadataKey)
	_, err = f.client.CopyObject(mdInput)
	if err != nil {
		return err
	}

	fmt.Printf("S3FS Removing %s\n", srcMetadataKey)
	_, err = f.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(srcMetadataKey),
	})

	if err != nil {
		return err
	}

	return nil
}

func (f *S3Filesystem) Stat(name string) (FileInfo, error) {
	fmt.Printf("S3FS Stat: %s\n", name)

	return f.Lstat(name)
}

func (f *S3Filesystem) DirNames(name string) ([]string, error) {

	dirPrefix := f.rooted(name)

	if name == "." {
		dirPrefix = f.keyPrefix + "/"
	} else {
		dirPrefix = dirPrefix + "/"
	}

	fmt.Printf("S3FS DirNames: %s from %s\n", name, dirPrefix)

	directories := []string{}
	hasMoreObjects := true

	// Keep track of how many objects we delete
	for hasMoreObjects {
		resp, err := f.client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(f.bucket),
			Prefix: aws.String(dirPrefix),
		})

		if err != nil {
			return nil, err
		}

		for _, o := range resp.Contents {
			if *o.Key != dirPrefix {
				dirKey := s3rel(*o.Key, dirPrefix)
				fmt.Printf("S3Dir found: %s\n", dirKey)
				directories = append(directories, dirKey)
			}
		}

		hasMoreObjects = *resp.IsTruncated
	}

	return directories, nil
}

func (fs *S3Filesystem) GetMetadata(name string) (*s3FileMetadata, error) {
	rootedName := fs.rooted(name)
	fmt.Printf("S3FS GetMetadata for %s\n", rootedName)
	metadataKey := fmt.Sprintf(".metadata/%s", rootedName)
	metadataBuff := &aws.WriteAtBuffer{}
	fmt.Printf("Fetching metadata file %s for %s\n", metadataKey, rootedName)
	downloader := s3manager.NewDownloaderWithClient(fs.client)
	_, err := downloader.Download(metadataBuff, &s3.GetObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(metadataKey),
	})

	var metadata s3FileMetadata
	err = json.Unmarshal(metadataBuff.Bytes(), &metadata)

	if err != nil {
		return nil, err
	}
	fmt.Printf("Loaded metadata: %s\n", metadata)

	return &metadata, nil
}

func (f *S3Filesystem) Open(name string) (File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0755)
}

// TODO check if it only contains a block pointer, if not convert to blocks
func (f *S3Filesystem) OpenFile(name string, flags int, mode FileMode) (File, error) {
	fmt.Printf("S3FS OpenFile: %s with flags %s and mode %s\n", name, flags, mode)
	rootedName := f.rooted(name)

	if flags&os.O_CREATE != 0 {
		fmt.Printf("Creating file %s\n", name)
	}

	// TODO don't reset offset...
	// Just return a handle to it
	handle := &s3File{
		bucket:    f.bucket,
		name:      name,
		key:       rootedName,
		client:    f.client,
		offset:    0,
		blockName: "",
		size:      0,
	}

	stat, err := s3stat(f.client, f.bucket, rootedName, name)

	if err != os.ErrNotExist && !stat.IsDir() {
		if stat.Size() == 0 {
			// Using blocks
			metadata, _ := f.GetMetadata(name)
			handle.blockName = metadata.BlockName
			handle.size = metadata.Size
		} else {
			// Migrate to blocks...
			fmt.Printf("File exists but doesn't only contain only a block id, need to migrate it...\n")
			handle.blockName = GetMD5Hash(rootedName)
			// TODO Stream blocks
			downloader := s3manager.NewDownloaderWithClient(f.client)
			buff := &aws.WriteAtBuffer{}
			fmt.Printf("Fetching file %s\n", rootedName)

			_, err := downloader.Download(buff, &s3.GetObjectInput{
				Bucket: aws.String(f.bucket),
				Key:    aws.String(rootedName),
			})

			if err != nil {
				return nil, err
			}

			handle.Write(buff.Bytes())
			handle.WriteMetadata()
			handle.WritePlaceholder()
			fmt.Printf("Migrated to blocks as %s -> %s\n", handle.key, handle.blockName)
		}
	} else {
		// Doesn't exist
		// Opened with CREATE, write out a file descriptor
		if flags&os.O_CREATE != 0 {
			// TODO UUID
			handle.blockName = GetMD5Hash(rootedName)
			handle.WriteMetadata()
			handle.WritePlaceholder()
		} else {
			// Wasn't to be created, error that it doesn't exist
			return nil, os.ErrNotExist
		}
	}

	return handle, nil
}

func (f *S3Filesystem) Create(name string) (File, error) {
	fmt.Printf("S3FS Create: %s\n", name)
	return f.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (f *S3Filesystem) Walk(root string, walkFn WalkFunc) error {

	// implemented in WalkFilesystem
	return errors.New("not implemented")
}

func (f *S3Filesystem) Roots() ([]string, error) {
	roots := make([]string, 1)
	return roots, nil
}

func (f *S3Filesystem) SymlinksSupported() bool {
	return false
}

func (f *S3Filesystem) Glob(pattern string) ([]string, error) {
	fmt.Printf("S3FS Glob: %s\n", pattern)
	pattern = f.rooted(pattern)
	files, err := filepath.Glob(pattern)
	unrooted := make([]string, len(files))
	for i := range files {
		unrooted[i] = f.unrooted(files[i])
	}
	return unrooted, err
}

// TODO: Compute / cache / update actual usage size?
func (f *S3Filesystem) Usage(name string) (Usage, error) {
	name = f.rooted(name)
	return Usage{
		Free:  9223372036854775807,
		Total: 0,
	}, nil
}

func (f *S3Filesystem) Watch(path string, ignore Matcher, ctx context.Context, ignorePerms bool) (<-chan Event, error) {
	fmt.Printf("S3 Watch: %s\n", path)
	return nil, nil
}

func (f *S3Filesystem) Type() FilesystemType {
	return FilesystemTypeS3
}

func (f *S3Filesystem) URI() string {
	uri := "s3://" + f.bucket + "/" + f.keyPrefix + "/"
	fmt.Printf("URI FOR FILESYSTEM: %s\n", uri)
	return uri
}

func (f *S3Filesystem) SameFile(fi1, fi2 FileInfo) bool {
	// Like os.SameFile, we always return false unless fi1 and fi2 were created
	// by this package's Stat/Lstat method.
	/*
		f1, ok1 := fi1.(s3FileInfo)
		f2, ok2 := fi2.(s3FileInfo)
		if !ok1 || !ok2 {
			return false
		}*/

	return f.rooted(fi1.Name()) == f.rooted(fi2.Name())
}

// s3File implements the fs.File interface on top of an S3 object
type s3File struct {
	*os.File
	bucket    string
	name      string
	client    *s3.S3
	key       string
	offset    int64
	blockName string
	size      int64
}

type s3FileMetadata struct {
	BlockName string `json:"blockName"`
	Size      int64  `json:"size"`
}

func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) Close() error {
	fmt.Printf("S3 Close: %s\n", f.key)
	return nil
}

func (f *s3File) Stat() (FileInfo, error) {
	fmt.Printf("S3 Stat: %s\n", f.name)
	stat, err := s3stat(f.client, f.bucket, f.key, f.name)

	if err != nil {
		return nil, err
	}

	st := s3FileStat{
		name:        stat.Name(),
		size:        f.size,
		mode:        0777,
		isDirectory: stat.IsDir(),
		modTime:     stat.ModTime(),
	}

	return st, err
}

type s3Block struct {
	id     int64
	key    string
	file   string
	bucket string
	data   []byte
	dirty  bool
	start  int64
	size   int64
}

// TODO: Block cache optimization
// Only write blocks when they're full / file is closed
// Blocks needs to be smarter about internal state then...
// Also consider what happens if failure; I think it will just retry...
var blockCache = make(map[string]*s3Block)
var blockSize = int64(131072)

func FetchBlockFromCache(blockId string) (*s3Block, bool) {
	b, ok := blockCache[blockId]
	return b, ok
}

func CacheBlock(blockId string, block *s3Block) {
	blockCache[blockId] = block
}

// Cache these?
func (f *s3File) FetchBlocks(offset int64, nBytes int) []*s3Block {
	startBlock := offset / blockSize
	endBlock := (offset + int64(nBytes) - 1) / blockSize
	blockCount := (endBlock - startBlock) + 1
	fmt.Printf("S3 FetchBlocks will pull %d blocks (%d -> %d)\n", blockCount, startBlock, endBlock)
	downloader := s3manager.NewDownloaderWithClient(f.client)
	blocks := make([]*s3Block, 0)
	for blockId := startBlock; blockId <= endBlock; blockId++ {
		blockKey := fmt.Sprintf(".blocks/%s.%d", f.blockName, blockId)
		block, ok := FetchBlockFromCache(blockKey)
		if !ok {
			buff := &aws.WriteAtBuffer{}
			fmt.Printf("Fetching block %s\n", blockKey)

			_, err := downloader.Download(buff, &s3.GetObjectInput{
				Bucket: aws.String(f.bucket),
				Key:    aws.String(blockKey),
			})

			block = &s3Block{
				id:     blockId,
				key:    blockKey,
				file:   f.key,
				bucket: f.bucket,
				data:   buff.Bytes(),
				dirty:  false,
				start:  blockId * blockSize,
				size:   blockSize,
			}

			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					fmt.Printf("Error code getting block: %s\n", aerr.Code())
					if aerr.Code() == "NoSuchKey" {
						fmt.Printf("No such block, creating empty data block!\n")
						block.data = make([]byte, block.size)
					}
				}
			} else {
				fmt.Printf("Downloaded block!\n")
			}

			CacheBlock(blockKey, block)
		}

		blocks = append(blocks, block)
	}

	return blocks
}

func (f *s3File) WriteBlocks(blocks []*s3Block) error {

	fmt.Printf("S3 WriteBlocks: %d blocks\n", len(blocks))
	for _, block := range blocks {
		fmt.Printf("S3 WriteBlocks: Block ID: %d\n", block.id)

		buffReader := bytes.NewReader(block.data)

		params := &s3.PutObjectInput{
			Bucket:        aws.String(block.bucket),
			Key:           aws.String(block.key),
			Body:          buffReader,
			ContentLength: aws.Int64(block.size),
		}

		_, err := f.client.PutObject(params)
		if err != nil {
			fmt.Printf("S3 Write bad response: %s", err)
			return err
		}

		// Update cache too...
		CacheBlock(block.key, block)

	}

	return nil

}

// This is stupid; there's no seek / fpntr for these... guess we may need that
func (f *s3File) Read(buf []byte) (int, error) {
	fmt.Printf("S3 Read: %s as %s from %s starting at %d\n", f.name, f.key, f.bucket, f.offset)
	bytesRead, err := f.ReadAt(buf, f.offset)
	if err != nil {
		return 0, err
	}

	f.offset = f.offset + int64(bytesRead)
	return bytesRead, nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (f *s3File) ReadAt(buf []byte, off int64) (n int, err error) {
	dataLen := int64(len(buf))
	fmt.Printf("S3 ReadAt: %s from %d to %d\n", f.name, off, off+dataLen)
	blocks := f.FetchBlocks(off, len(buf))
	nextOffset := off
	bufOffset := 0
	bytesRead := 0
	bytesReadInBlock := 0
	for _, block := range blocks {
		fmt.Printf("Reading from block: %s (start: %d, size: %d)\n", block.key, block.start, block.size)
		start := nextOffset - block.start
		nextOffset = block.start + block.size
		bytesInBlockToRead := min(dataLen-int64(bytesReadInBlock), block.size-start) //min(int64(len(buf)), block.size-start)
		end := start + bytesInBlockToRead

		fmt.Printf("Reading range from %d -> %d\n", start, end)
		fmt.Printf("NextOffset: %d, BytesInBlockToRead: %d\n", nextOffset, bytesInBlockToRead)
		for _, b := range block.data[start:end] {
			buf[bufOffset] = b
			bufOffset++
			bytesRead++
			bytesReadInBlock++
		}
	}

	fmt.Printf("S3 ReadAt read %d bytes across %d blocks\n", bytesRead, len(blocks))
	return int(bytesRead), err
}

func (f *s3File) Seek(offset int64, whence int) (ret int64, err error) {
	fmt.Printf("S3 Seek: %s\n", f.name)
	return 0, nil
}

func (f *s3File) Sync() error {
	return nil
}

func (f *s3File) Truncate(size int64) error {
	return nil
}

func (f *s3File) WritePlaceholder() error {
	emptyBytes := make([]byte, 0)
	buffReader := bytes.NewReader(emptyBytes)
	params := &s3.PutObjectInput{
		Bucket:        aws.String(f.bucket),
		Key:           aws.String(f.key),
		Body:          buffReader,
		ContentLength: aws.Int64(int64(0)),
	}
	_, err := f.client.PutObject(params)

	return err
}

func (f *s3File) WriteMetadata() error {
	metadataKey := fmt.Sprintf(".metadata/%s", f.key)

	metadata := s3FileMetadata{
		BlockName: f.blockName,
		Size:      f.size,
	}

	fmt.Printf("Metadata to write: %s\n", metadata)
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		fmt.Println("error serializing:", err)
	}
	fmt.Printf("Metadata bytes to write: %s\n", metadataBytes)
	fmt.Printf("Writing pointer for %s -> %s -- len: %d\n", f.key, metadataKey, len(metadataBytes))
	buffReader := bytes.NewReader(metadataBytes)
	params := &s3.PutObjectInput{
		Bucket:        aws.String(f.bucket),
		Key:           aws.String(metadataKey),
		Body:          buffReader,
		ContentLength: aws.Int64(int64(len(metadataBytes))),
	}
	_, err = f.client.PutObject(params)
	if err != nil {
		fmt.Printf("S3 Write bad response: %s", err)
		return err
	}
	return nil
}

func (f *s3File) Write(buff []byte) (n int, err error) {
	fmt.Printf("S3 Write: %s\n", f.name)
	return f.WriteAt(buff, f.offset)
}

// TODO: Optimize this by instead writing files as blocks so we don't need
// to fetch the whole thing and overwrite part of it; this whole thing is terribly ineffective...
func (f *s3File) WriteAt(buff []byte, off int64) (n int, err error) {
	dataLen := len(buff)
	blocks := f.FetchBlocks(off, dataLen)
	end := int64(off) + int64(dataLen)
	fmt.Printf("S3 WriteAt: %s %d-%d (%d bytes)\n", f.name, off, end, dataLen)
	bytesWritten := 0
	/*
		block start = initial offset + bytes written - block starting byte
		bytes to write in block = smaller of (bytes in buffer - bytes written so far) or all remaining bytes in the block
		block end = block start + bytes to write in block


	*/
	for _, block := range blocks {
		fmt.Printf("Writing to block: %d\n", block.id)
		blockStart := (off + int64(bytesWritten)) - block.start
		bytesInBlockToWrite := min(int64(dataLen-bytesWritten), block.size-blockStart)
		blockEnd := blockStart + bytesInBlockToWrite
		fmt.Printf("Block range: %d -> %d, Block size: %d\n", blockStart, blockEnd, block.size)
		fmt.Printf("Writing %d bytes\n", bytesInBlockToWrite)
		for i := int64(0); i < bytesInBlockToWrite; i++ {
			block.data[blockStart+i] = buff[bytesWritten]
			bytesWritten++
		}
	}

	f.WriteBlocks(blocks)

	if f.size < end {
		f.size = end
	}

	return dataLen, nil
}

func (f *s3File) Exists() bool {

	fmt.Printf("Checking if %s exists...", f.key)

	_, err := f.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			fmt.Printf(" Nope!\n")
			return false
		}
	}

	fmt.Printf(" Yep!\n")

	return true
}

type s3FileStat struct {
	name        string
	size        int64
	mode        FileMode
	modTime     time.Time
	isDirectory bool
}

func (fs s3FileStat) Name() string       { return fs.name }
func (fs s3FileStat) IsDir() bool        { return fs.isDirectory }
func (fs s3FileStat) ModTime() time.Time { return fs.modTime }
func (fs s3FileStat) Mode() FileMode     { return fs.mode }
func (fs s3FileStat) Size() int64        { return fs.size }
func (fs s3FileStat) BlockSize() int64   { return 1024000 }

func (e s3FileStat) IsSymlink() bool {
	// Must use s3FileInfo.Mode() because it may apply magic.
	//return e.Mode()&ModeSymlink != 0
	return false
}

func (e s3FileStat) IsRegular() bool {
	// Must use s3FileInfo.Mode() because it may apply magic.
	//return e.Mode()&ModeType == 0
	return !e.isDirectory
}

func s3stat(client *s3.S3, bucket, key, name string) (FileInfo, error) {

	fmt.Printf("S3STAT: %s -> %s\n", key, name)

	// Is it a directory?
	out, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key + "/"),
	})

	if err != nil {
		fmt.Printf("Error with stat: %s\n", err)
		return nil, err
	}

	keyIsDirectory := false
	if len(out.Contents) > 0 {
		fmt.Printf("S3STAT %s is a dir\n", key)
		keyIsDirectory = true
	}

	objectKey := key
	if keyIsDirectory {
		objectKey = objectKey + "/"
	}

	headObj, err := client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return nil, os.ErrNotExist
		}

		fmt.Printf("Unhandled error getting head for %s: %s\n", key, err)
		return nil, err
	}

	return s3FileStat{
		name:        name,
		size:        *headObj.ContentLength,
		mode:        0777,
		isDirectory: keyIsDirectory,
		modTime:     *headObj.LastModified,
	}, nil
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
