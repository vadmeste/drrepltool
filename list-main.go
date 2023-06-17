/*
 * MinIO Client (C) 2021 MinIO, Inc.
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
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/pkg/console"
)

var allFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "data-dir, d",
		Usage: "path to work directory for tool",
	},
	cli.StringFlag{
		Name:  "endpoint, e",
		Usage: "S3 endpoint url",
	},
	cli.StringFlag{
		Name:  "access-key, ak",
		Usage: "S3 access key",
	},
	cli.StringFlag{
		Name:  "secret-key, sk",
		Usage: "S3 secret key",
	},
	cli.StringFlag{
		Name:  "bucket, b",
		Usage: "S3 bucket",
	},
	cli.StringFlag{
		Name:  "prefix, p",
		Usage: "S3 bucket prefix (optional)",
	},
	cli.BoolFlag{
		Name:  "versions, v",
		Usage: "list all versions",
	},
	cli.BoolFlag{
		Name:  "insecure, i",
		Usage: "disable TLS certificate verification",
	},
	cli.BoolFlag{
		Name:  "log, l",
		Usage: "enable logging",
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "enable debugging",
	},
}
var (
	srcEndpoint, srcAccessKey, srcSecretKey string
	srcBucket, srcPrefix                    string
	tgtEndpoint, tgtAccessKey, tgtSecretKey string
	tgtBucket                               string

	debug, logFlag bool
	versions       bool
	dirPath        string
	format         string
)

const (
	objListFile  = "object_listing.txt"
	failCopyFile = "copy_fails.txt"
	logCopyFile  = "copy_success.txt"
)

var listCmd = cli.Command{
	Name:   "list",
	Usage:  "List objects in DR bucket and download to disk",
	Action: listAction,
	Flags:  allFlags,
	CustomHelpTemplate: `NAME:
   {{.HelpName}} - {{.Usage}}
 
 USAGE:
   {{.HelpName}}  --dir
 
 FLAGS:
   {{range .VisibleFlags}}{{.}}
   {{end}}
 
 EXAMPLES:
 1. List object versions in minio bucket "srcbucket" at https://minio1 and download list to /tmp/data
	$ drrepltool list --data-dir "/tmp/data" --endpoint https://minio1 --access-key minio --secret-key minio123 --bucket "srcbucket" --versions
 
 `,
}

func checkArgsAndInit(ctx *cli.Context) {
	debug = ctx.Bool("debug")
	dirPath = ctx.String("data-dir")

	srcEndpoint = ctx.String("endpoint")
	srcAccessKey = ctx.String("access-key")
	srcSecretKey = ctx.String("secret-key")
	srcBucket = ctx.String("bucket")
	srcPrefix = ctx.String("prefix")
	versions = ctx.Bool("versions")
	debug = ctx.Bool("debug")

	if srcEndpoint == "" {
		log.Fatalln("src Endpoint is not provided")
	}

	if srcAccessKey == "" {
		log.Fatalln("src Access key is not provided")
	}

	if srcSecretKey == "" {
		log.Fatalln("src Secret key is not provided")
	}

	if srcBucket == "" && srcPrefix != "" {
		log.Fatalln("--src-prefix is specified without --src-bucket.")
	}
	if srcBucket == "" {
		log.Fatalln("--src-bucket not specified.")
	}

	if dirPath == "" {
		console.Fatalln(fmt.Errorf("path to working dir required, please set --data-dir flag"))
		return
	}
}

func listAction(cliCtx *cli.Context) error {
	checkArgsAndInit(cliCtx)
	s3SrcClient, err := initMinioClient(cliCtx, srcAccessKey, srcSecretKey, srcBucket, srcEndpoint)
	if err != nil {
		log.Fatalln("Could not initialize client", err)
	}
	f, err := os.OpenFile(path.Join(dirPath, objListFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalln("Could not open file path", path.Join(dirPath, objListFile), err)
	}
	defer f.Close()
	datawriter := bufio.NewWriter(f)
	if debug {
		s3SrcClient.TraceOn(os.Stderr)
	}
	defer datawriter.Flush()
	opts := minio.ListObjectsOptions{
		Recursive:    true,
		Prefix:       srcPrefix,
		WithVersions: versions,
		WithMetadata: true,
	}
	// List all objects from a bucket-name with a matching prefix.
	for object := range s3SrcClient.ListObjects(context.Background(), srcBucket, opts) {
		if object.Err != nil {
			log.Fatalln("LIST error:", object.Err)
			continue
		}
		str := fmt.Sprintf("%s, %s, %s, %t\n", srcBucket, object.Key, object.VersionID, object.IsDeleteMarker)
		if _, err := datawriter.WriteString(str); err != nil {
			log.Println("Error writing object to file:", srcBucket, object.Key, object.VersionID, err)
		}
	}
	return nil
}
