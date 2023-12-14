package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	miniogo "github.com/minio/minio-go/v7"
)

var dryRun bool

type objInfo struct {
	bucket       string
	object       string
	versionID    string
	deleteMarker bool

	partSize int64 // zero means the object is single part
}

func (i objInfo) String() string {
	return fmt.Sprintf("%s,%s,%s,%t", i.bucket, i.object, i.versionID, i.deleteMarker)
}

type CopyState struct {
	objectCh  chan objInfo
	failedCh  chan copyErr
	logCh     chan objInfo
	replicate bool
	count     uint64
	failCnt   uint64
	workersWg sync.WaitGroup
	statusWg  sync.WaitGroup
}

type copyErr struct {
	object objInfo
	err    error
}

func (m *CopyState) queueUploadTask(obj objInfo) {
	m.objectCh <- obj
}

var (
	copyState      *CopyState
	copyConcurrent = 100
)

func newCopyState(ctx context.Context, replicate bool) *CopyState {
	if runtime.GOMAXPROCS(0) > copyConcurrent {
		copyConcurrent = runtime.GOMAXPROCS(0)
	}
	cs := &CopyState{
		replicate: replicate,
		objectCh:  make(chan objInfo, copyConcurrent),
		failedCh:  make(chan copyErr, copyConcurrent),
		logCh:     make(chan objInfo, copyConcurrent),
	}

	return cs
}

// Increase count processed
func (c *CopyState) incCount() {
	atomic.AddUint64(&c.count, 1)
}

// Get total count processed
func (c *CopyState) getCount() uint64 {
	return atomic.LoadUint64(&c.count)
}

// Increase count failed
func (m *CopyState) incFailCount() {
	atomic.AddUint64(&m.failCnt, 1)
}

// Get total count failed
func (c *CopyState) getFailCount() uint64 {
	return atomic.LoadUint64(&c.failCnt)
}

// addWorker creates a new worker to process tasks
func (c *CopyState) addWorker(ctx context.Context) {
	c.workersWg.Add(1)
	// Add a new worker.
	go func() {
		defer c.workersWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case obj, ok := <-c.objectCh:
				if !ok {
					return
				}
				logDMsg(fmt.Sprintf("Copying...%s", obj), nil)
				var err error
				if c.replicate {
					if obj.versionID != "" {
						err = replicateObject(ctx, obj)
					} else {
						err = fmt.Errorf("replicating an object with unknown version, %s/%s", obj.bucket, obj.object)
					}
				} else {
					err = copyObject(ctx, obj)
				}
				if err != nil {
					c.incFailCount()
					logMsg(fmt.Sprintf("error copying object %s: %s", obj, err))
					c.failedCh <- copyErr{object: obj, err: err}
					continue
				}
				c.incCount()
				c.logCh <- obj
			}
		}
	}()
}

func (c *CopyState) finish(ctx context.Context) {
	close(c.objectCh)
	c.workersWg.Wait() // wait on workers to finish
	close(c.failedCh)
	close(c.logCh)
	c.statusWg.Wait()

	if !dryRun {
		logMsg(fmt.Sprintf("Copied %d objects, %d failures", c.getCount(), c.getFailCount()))
	}
}

func (c *CopyState) init(ctx context.Context) {
	if c == nil {
		return
	}
	for i := 0; i < copyConcurrent; i++ {
		c.addWorker(ctx)
	}
	c.statusWg.Add(2)
	go func() {
		defer c.statusWg.Done()
		f, err := os.OpenFile(path.Join(dirPath, getFileName(failCopyFile, "")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			logDMsg("could not create + copy_fails.txt", err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case o, ok := <-c.failedCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(o.object.String() + " : " + o.err.Error() + "\n"); err != nil {
					logMsg(fmt.Sprintf("Error writing to copy_fails.txt for "+o.object.String(), err))
					os.Exit(1)
				}

			}
		}
	}()
	go func() {
		defer c.statusWg.Done()
		f, err := os.OpenFile(path.Join(dirPath, getFileName(logCopyFile, "")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			logDMsg("could not create + copy_log.txt", err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case obj, ok := <-c.logCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(obj.String() + "\n"); err != nil {
					logMsg(fmt.Sprintf("Error writing to copy_log.txt for "+obj.String(), err))
					os.Exit(1)
				}

			}
		}
	}()
}

var errObjectNotFound = errors.New("The specified key does not exist.")

func isMethodNotAllowedErr(err error) bool {
	switch err.Error() {
	case "The specified method is not allowed against this resource.":
		return true
	case "405 Method Not Allowed":
		return true
	}
	return false
}

func copyObject(ctx context.Context, si objInfo) error {
	obj, err := srcClient.GetObject(ctx, si.bucket, si.object, miniogo.GetObjectOptions{})
	if err != nil {
		return err
	}

	oi, err := obj.Stat()
	if err != nil {
		return err
	}
	defer obj.Close()
	if dryRun {
		logMsg(fmt.Sprintf("%s", oi.Key))
		return nil
	}

	enc, ok := oi.Metadata[ContentEncoding]
	if !ok {
		enc = oi.Metadata[strings.ToLower(ContentEncoding)]
	}

	bucket := tgtBucket
	if bucket == "" {
		bucket = si.bucket
	}

	uoi, err := tgtClient.PutObject(ctx, bucket, oi.Key, obj, oi.Size, miniogo.PutObjectOptions{
		UserMetadata:    oi.UserMetadata,
		ContentType:     oi.ContentType,
		StorageClass:    oi.StorageClass,
		UserTags:        oi.UserTags,
		ContentEncoding: strings.Join(enc, ","),
		PartSize:        uint64(si.partSize),
	})
	if err != nil {
		logDMsg("upload to minio failed for "+oi.Key, err)
		return err
	}
	if uoi.Size != oi.Size {
		err = fmt.Errorf("expected size %d, uploaded %d", oi.Size, uoi.Size)
		logDMsg("upload to minio failed for "+oi.Key, err)
		return err
	}
	logDMsg("Uploaded "+uoi.Key+" successfully", nil)
	return nil
}

func replicateObject(ctx context.Context, si objInfo) error {
	obj, err := srcClient.GetObject(ctx, si.bucket, si.object, miniogo.GetObjectOptions{
		VersionID: si.versionID,
	})
	if err != nil {
		return err
	}

	oi, err := obj.Stat()
	if err != nil {
		if !(isMethodNotAllowedErr(err) && si.deleteMarker) {
			return err
		}
	}
	defer obj.Close()
	if dryRun {
		logMsg(fmt.Sprintf("dry-run replicate %s (%s)", oi.Key, oi.VersionID))
		return nil
	}

	bucket := tgtBucket
	if bucket == "" {
		bucket = si.bucket
	}

	if si.deleteMarker {
		_, err = tgtClient.StatObject(ctx, bucket, si.object, miniogo.StatObjectOptions{
			VersionID: si.versionID,
		})
		if err.Error() == errObjectNotFound.Error() {
			return tgtClient.RemoveObject(ctx, bucket, si.object, miniogo.RemoveObjectOptions{
				VersionID: si.versionID,
				Internal: miniogo.AdvancedRemoveOptions{
					ReplicationDeleteMarker: si.deleteMarker,
					ReplicationMTime:        oi.LastModified,
					ReplicationStatus:       miniogo.ReplicationStatusComplete,
					ReplicationRequest:      true, // always set this to distinguish between `mc mirror` replication and serverside
				},
			})
		} else {
			if isMethodNotAllowedErr(err) {
				logDMsg("object already exists on MinIO "+si.object+"("+si.versionID+") not copied", err)
				return nil
			}
			return err
		}
	}
	enc, ok := oi.Metadata[ContentEncoding]
	if !ok {
		enc = oi.Metadata[strings.ToLower(ContentEncoding)]
	}
	uoi, err := tgtClient.PutObject(ctx, bucket, oi.Key, obj, oi.Size, miniogo.PutObjectOptions{
		Internal: miniogo.AdvancedPutOptions{
			SourceMTime:       oi.LastModified,
			SourceVersionID:   oi.VersionID,
			SourceETag:        oi.ETag,
			ReplicationStatus: miniogo.ReplicationStatusComplete,
		},
		UserMetadata:    oi.UserMetadata,
		ContentType:     oi.ContentType,
		StorageClass:    oi.StorageClass,
		UserTags:        oi.UserTags,
		ContentEncoding: strings.Join(enc, ","),
	})
	if err != nil {
		logDMsg("upload to minio failed for "+oi.Key, err)
		return err
	}
	if uoi.Size != oi.Size {
		err = fmt.Errorf("expected size %d, uploaded %d", oi.Size, uoi.Size)
		logDMsg("upload to minio failed for "+oi.Key, err)
		return err
	}
	logDMsg("Uploaded "+uoi.Key+" successfully", nil)
	return nil
}

const (
	ContentEncoding = "Content-Encoding"
)
