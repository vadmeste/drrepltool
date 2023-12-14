/*
 * MinIO Client (C) 2023 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
)

var firstAlias, secondAlias string

var autoFixFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "inspect-dir, d",
		Usage:  "path to inspect dir",
		Hidden: true,
	},
	cli.StringFlag{
		Name:  "bucket, b",
		Usage: "S3 bucket",
	},
	cli.StringFlag{
		Name:  "prefix, p",
		Usage: "S3 prefix (optional)",
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "enable debugging",
	},
	cli.BoolFlag{
		Name:  "dry-run",
		Usage: "only scan without new write",
	},
	cli.StringFlag{
		Name:  "modified-since",
		Usage: "minimum modtime for object check",
	},
}

var autoFixCmd = cli.Command{
	Name:   "auto-fix",
	Usage:  "Verify objects checksum; for any corruption found, get the object from the good cluster and overwrites the one in the bad cluster",
	Action: autoFixAction,
	Flags:  autoFixFlags,
	CustomHelpTemplate: `NAME:
   {{.HelpName}} - {{.Usage}}
 
 FLAGS:
   {{range .VisibleFlags}}{{.}}
   {{end}}
 
 EXAMPLES:
 1. Override bad objects with a copy from the good cluster
	$ ./drrepltool auto-fix good-cluster-alias bad-cluster-alias --modified-since "2023-11-24T00:00:00Z"
 
 `,
}

func tryToFixThisObject(cliCtx *cli.Context, copyState *CopyState, obj objInfo) error {
	copyState.queueUploadTask(obj)
	logDMsg(fmt.Sprintf("adding %s to copy queue", obj), nil)
	return nil
}

var minModTimeStr string

// getMD5Sum returns MD5 sum of given data.
func getMD5Sum(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

func autoFixAction(cliCtx *cli.Context) error {
	logFlag = true
	dryRun = cliCtx.Bool("dry-run")

	minModTimeStr = cliCtx.String("modified-since")

	args := cliCtx.Args()
	srcClient, err = initMinioClientFromAlias(cliCtx, args[0])
	if err != nil {
		return fmt.Errorf("could not initialize src client %w", err)
	}

	tgtClient, err = initMinioClientFromAlias(cliCtx, args[1])
	if err != nil {
		return fmt.Errorf("could not initialize tgt client %w", err)
	}
	fmt.Println("Scanning the cluster ", tgtClient.EndpointURL(), " looking for suspicious objects..")
	fmt.Println("Backup cluster: ", srcClient.EndpointURL())
	fmt.Println("---")
	fmt.Println("copy_fails.txt and copy_success.txt will contain information about any suspicious objects found..")
	fmt.Println("Do not forget to nohup me..")

	var minModTime time.Time
	if minModTimeStr != "" {
		var e error
		minModTime, e = time.Parse(time.RFC3339, minModTimeStr)
		if e != nil {
			log.Fatalln("Unable to parse --modified-since:", e)
		}
	}

	ctx := context.Background()
	start := time.Now()

	copyState = newCopyState(ctx, true) // replicate instead of a simple copy
	copyState.init(ctx)

	bucket := "" // Get from the CLI

	var buckets []string
	if bucket != "" {
		buckets = append(buckets, bucket)
	} else {
		bucketsInfo, err := tgtClient.ListBuckets(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		for _, b := range bucketsInfo {
			buckets = append(buckets, b.Name)
		}
	}

	objectsListed := 0

	for _, bucket := range buckets {
		opts := minio.ListObjectsOptions{
			Recursive:    true,
			WithVersions: true,
			Prefix:       cliCtx.String("prefix"),
			WithMetadata: true,
		}

		// List all objects from a bucket-name with a matching prefix.
		for object := range tgtClient.ListObjects(context.Background(), bucket, opts) {
			if object.Err != nil {
				continue
			}

			objectsListed++
			if objectsListed%100000 == 0 {
				fmt.Println("..", objectsListed, "objects listed..")
			}
			if !object.IsLatest {
				continue
			}
			if !minModTime.IsZero() && object.LastModified.Before(minModTime) {
				continue
			}
			if object.IsDeleteMarker {
				continue
			}
			// Ignore empty object
			if object.Size == 0 {
				continue
			}
			if _, ok := object.UserMetadata["X-Amz-Server-Side-Encryption-Customer-Algorithm"]; ok {
				continue
			}
			if v, ok := object.UserMetadata["X-Amz-Server-Side-Encryption"]; ok && v == "aws:kms" {
				continue
			}
			parts := 1
			multipart := false
			s := strings.Split(object.ETag, "-")
			switch len(s) {
			case 1:
				// nothing to do
			case 2:
				if p, err := strconv.Atoi(s[1]); err == nil {
					parts = p
				} else {
					continue
				}
				multipart = true
			default:
				continue
			}

			var partsMD5Sum [][]byte
			var partSize int64
			var failedMD5 bool
			for p := 1; p <= parts; p++ {
				opts := minio.GetObjectOptions{
					VersionID:  object.VersionID,
					PartNumber: p,
				}
				obj, err := tgtClient.GetObject(context.Background(), bucket, object.Key, opts)
				if err != nil {
					failedMD5 = true
					break
				}
				h := md5.New()
				if n, err := io.Copy(h, obj); err != nil {
					failedMD5 = true
					break
				} else {
					if p == 1 && multipart {
						partSize = n
					}
				}
				partsMD5Sum = append(partsMD5Sum, h.Sum(nil))
			}

			if failedMD5 {
				tryToFixThisObject(cliCtx, copyState,
					objInfo{
						bucket:    bucket,
						object:    object.Key,
						versionID: object.VersionID,
						partSize:  partSize,
					})
				continue
			}

			corrupted := false
			if !multipart {
				md5sum := fmt.Sprintf("%x", partsMD5Sum[0])
				if md5sum != object.ETag {
					corrupted = true
				}
			} else {
				var totalMD5SumBytes []byte
				for _, sum := range partsMD5Sum {
					totalMD5SumBytes = append(totalMD5SumBytes, sum...)
				}
				s3MD5 := fmt.Sprintf("%x-%d", getMD5Sum(totalMD5SumBytes), parts)
				if s3MD5 != object.ETag {
					corrupted = true
				}
			}

			if corrupted {
				tryToFixThisObject(cliCtx, copyState,
					objInfo{
						bucket:    bucket,
						object:    object.Key,
						versionID: object.VersionID,
						partSize:  partSize,
					})
			}
		}
	}

	copyState.finish(ctx)

	end := time.Now()
	latency := end.Sub(start).Seconds()
	count := copyState.getCount() - copyState.getFailCount()
	logMsg(fmt.Sprintf("Copied %s / %s objects with latency %d secs", humanize.Comma(int64(count)), humanize.Comma(int64(copyState.getCount())), int64(latency)))

	return nil
}
