// Copyright (C) 2016 The Syncthing Authors.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
	return nil
}

// Fake stat()
func (f *S3Filesystem) Lstat(name string) (FileInfo, error) {
	fmt.Printf("S3 Lstat: %s\n", name)
	key := name

	if name == "." {
		key = f.keyPrefix + "/"
	} else {
		key = f.rooted(name)
	}

	trimmed := strings.TrimPrefix(filepath.Clean(key), "/")

	if trimmed == f.keyPrefix {
		fmt.Printf("Head on %s\n", key)
		headObj, err := f.client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(f.bucket),
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

	return (s3stat(f.client, f.bucket, key, name))
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
				dirKey := s3rel(*o.Key, f.keyPrefix)
				fmt.Printf("S3Dir found: %s\n", dirKey)
				directories = append(directories, dirKey)
			}
		}

		hasMoreObjects = *resp.IsTruncated
	}

	return directories, nil
}

func (f *S3Filesystem) Open(name string) (File, error) {
	rootedName := f.rooted(name)

	fmt.Printf("S3FS Open %s -> %s\n", name, rootedName)

	handle := s3File{
		bucket: f.bucket,
		name:   name,
		key:    rootedName,
		client: f.client,
		offset: 0,
	}

	if !handle.Exists() {
		return nil, os.ErrNotExist
	}

	return handle, nil
}

func (f *S3Filesystem) OpenFile(name string, flags int, mode FileMode) (File, error) {
	fmt.Printf("S3FS OpenFile: %s\n", name)

	rootedName := f.rooted(name)

	// Just return a handle to it
	return s3File{
		bucket: f.bucket,
		name:   name,
		key:    rootedName,
		client: f.client,
		offset: 0,
	}, nil
}

func (f *S3Filesystem) Create(name string) (File, error) {
	fmt.Printf("S3FS Create: %s\n", name)
	rootedName := f.rooted(name)

	// Just return a handle to it
	return s3File{
		bucket: f.bucket,
		name:   name,
		key:    rootedName,
		client: f.client,
		offset: 0,
	}, nil
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
	return FilesystemTypeBasic
}

func (f *S3Filesystem) URI() string {
	return "s3://" + f.bucket + "/" + f.keyPrefix + "/"
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
	bucket string
	name   string
	client *s3.S3
	key    string
	offset int64
}

func (f s3File) Name() string {
	return f.name
}

func (f s3File) Close() error {
	return nil
}

func (f s3File) Stat() (FileInfo, error) {
	fmt.Printf("S3 Stat: %s\n", f.name)
	return s3stat(f.client, f.bucket, f.key, f.name)
}

// This is stupid; there's no seek / fpntr for these... guess we may need that
func (f s3File) Read(buf []byte) (int, error) {
	fmt.Printf("S3 Read: %s as %s from %s starting at %d\n", f.name, f.key, f.bucket, f.offset)
	numBytes, err := f.ReadAt(buf, f.offset)

	if err != nil {
		return 0, err
	}

	/*
		buff := aws.NewWriteAtBuffer()
		downloader := s3manager.NewDownloaderWithClient(f.client)
		numBytes, err := downloader.Download(buff, &s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		})*/
	fmt.Printf("S3 Read %d bytes -- buffer is %d in size\n", numBytes, len(buf))
	f.offset = f.offset + int64(numBytes)
	fmt.Printf("Offset is now %d\n", f.offset)

	return int(numBytes), err
}

func (f s3File) ReadAt(buf []byte, off int64) (n int, err error) {
	fmt.Printf("S3 ReadAt: %s\n", f.name)
	bytes := len(buf)
	endByte := off + int64(bytes)
	rangeValue := fmt.Sprintf("bytes=%d-%d", off, endByte-1)
	fmt.Printf("RANGE: %s\n", rangeValue)
	buff := aws.NewWriteAtBuffer(buf)
	downloader := s3manager.NewDownloaderWithClient(f.client)
	numBytes, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
		Range:  aws.String(rangeValue),
	})
	fmt.Printf("S3 Read %d bytes\n", numBytes)

	return int(numBytes), err
}

func (f s3File) Seek(offset int64, whence int) (ret int64, err error) {
	fmt.Printf("S3 Seek: %s\n", f.name)
	return 0, nil
}

func (f s3File) Sync() error {
	return nil
}

func (f s3File) Truncate(size int64) error {
	return nil
}

func (f s3File) Write(buff []byte) (n int, err error) {
	fmt.Printf("S3 Write: %s\n", f.name)
	dataLen := len(buff)
	buffReader := bytes.NewReader(buff)

	params := &s3.PutObjectInput{
		Bucket:        aws.String(f.bucket),
		Key:           aws.String(f.key),
		Body:          buffReader,
		ContentLength: aws.Int64(int64(dataLen)),
	}

	_, err = f.client.PutObject(params)
	if err != nil {
		fmt.Printf("S3 Write bad response: %s", err)
		return 0, err
	}

	return dataLen, nil

}

// TODO: Optimize this by instead writing files as blocks so we don't need
// to fetch the whole thing and overwrite part of it; this whole thing is terribly ineffective...
func (f s3File) WriteAt(buff []byte, off int64) (n int, err error) {
	dataLen := len(buff)
	end := int(off) + int(dataLen)
	fmt.Printf("S3 WriteAt: %s %d-%d (%d bytes)\n", f.name, off, end, dataLen)

	if !f.Exists() {
		f.Write(buff)
	} else {
		wab := &aws.WriteAtBuffer{}
		downloader := s3manager.NewDownloaderWithClient(f.client)
		numBytes, err := downloader.Download(wab, &s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		})

		if err != nil {
			return 0, err
		}
		fmt.Printf("S3 WriteAt Read %d bytes\n", numBytes)
		fmt.Printf("Writing %d - %d\n", off, end)
		wab.WriteAt(buff, off)
		f.Write(wab.Bytes())
	}
	return dataLen, nil
}

func (f s3File) Exists() bool {

	fmt.Printf("Checking if %s exists\n", f.key)

	_, err := f.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return false
		}
	}

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
		fmt.Printf("Error getting head for %s\n", key)
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return nil, os.ErrNotExist
		}

		return nil, err
	}

	stat := s3FileStat{
		name:        name,
		size:        *headObj.ContentLength,
		mode:        0777,
		isDirectory: keyIsDirectory,
		modTime:     *headObj.LastModified,
	}

	fmt.Printf("Result: %s\n", stat)
	return stat, nil
}
