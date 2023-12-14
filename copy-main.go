/*
 * MinIO Client (C) 2022 MinIO, Inc.
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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/console"
)

var srcFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "src-endpoint",
		Usage: "S3 endpoint url",
	},
	cli.StringFlag{
		Name:  "src-access-key",
		Usage: "S3 access key",
	},
	cli.StringFlag{
		Name:  "src-secret-key",
		Usage: "S3 secret key",
	},
	cli.StringFlag{
		Name:  "src-bucket",
		Usage: "source bucket",
	},
	cli.IntFlag{
		Name:  "skip, s",
		Usage: "number of entries to skip from input file",
		Value: 0,
	},
	cli.BoolFlag{
		Name:  "fake",
		Usage: "perform a fake copy",
	},
	cli.StringFlag{
		Name:  "input-file",
		Usage: "file with list of entries to copy from DR",
	},
	cli.StringFlag{
		Name:  "input-format",
		Usage: "drrepl | s3-check-md5 | compare-disks-ec",
		Value: "drrepl",
	},
	cli.BoolFlag{
		Name:  "simple-copy",
		Usage: "enforce a simple copy of the object",
	},
}

var copyCmd = cli.Command{
	Name:   "copy",
	Usage:  "Copy objects in data-dir/object_listing.txt and replicate to MinIO endpoint specified",
	Action: copyAction,
	Flags:  append(allFlags, srcFlags...),
	CustomHelpTemplate: `NAME:
	{{.HelpName}} - {{.Usage}}
  
  USAGE:
	{{.HelpName}}  --dir
  
  FLAGS:
	{{range .VisibleFlags}}{{.}}
	{{end}}
  
  EXAMPLES:
  1. Copy object versions in object_listing.txt of data-dir to minio bucket "dstbucket" at https://minio2 from "srcbucket" in https://minio1
	 $ drrepltool copy --data-dir "/tmp/data" --endpoint https://minio2 --access-key minio --secret-key minio123 --bucket "dstbucket" \
	  --src-endpoint https://minio1 --src-access-key minio1 --src-secret-key minio123 --src-bucket srcbucket  
  `,
}

var (
	srcClient *miniogo.Client // Good cluster
	tgtClient *miniogo.Client // Problematic cluster
	err       error
	re        *regexp.Regexp
)

func checkCopyArgsAndInit(ctx *cli.Context) {
	debug = ctx.Bool("debug")

	srcEndpoint = ctx.String("src-endpoint")
	srcAccessKey = ctx.String("src-access-key")
	srcSecretKey = ctx.String("src-secret-key")
	tgtEndpoint = ctx.String("endpoint")
	tgtAccessKey = ctx.String("access-key")
	tgtSecretKey = ctx.String("secret-key")
	tgtBucket = ctx.String("bucket")
	dirPath = ctx.String("data-dir")
	versions = ctx.Bool("versions")
	if tgtEndpoint == "" {
		log.Fatalln("--endpoint is not provided for target")
	}

	if tgtAccessKey == "" {
		log.Fatalln("--access-key is not provided for target")
	}

	if tgtSecretKey == "" {
		log.Fatalln("--secret-key is not provided for target")
	}

	if srcEndpoint == "" {
		log.Fatalln("--src-endpoint is not provided")
	}

	if srcAccessKey == "" {
		log.Fatalln("--src-access-key is not provided")
	}

	if srcSecretKey == "" {
		log.Fatalln("--src-secret-key is not provided")
	}
}

type aliasDef struct {
	URL       string `json:"url"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

type aliases struct {
	Version string              `json:"version"`
	Aliases map[string]aliasDef `json:"aliases"`
}

func initMinioClientFromAlias(ctx *cli.Context, alias string) (*miniogo.Client, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	configPath := filepath.Join(homeDir, ".mc", "config.json")
	jsonBytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var conf aliases
	err = json.Unmarshal(jsonBytes, &conf)
	if err != nil {
		return nil, err
	}

	info, ok := conf.Aliases[alias]
	if !ok {
		return nil, errors.New("unknown alias")
	}

	return initMinioClient(ctx, info.AccessKey, info.SecretKey, info.URL)
}

func initMinioClient(ctx *cli.Context, accessKey, secretKey, urlStr string) (*miniogo.Client, error) {
	target, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse input arg %s: %v", urlStr, err)
	}

	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("one or more of AccessKey:%s SecretKey: %s are missing in MinIO configuration for: %s", accessKey, secretKey, urlStr)
	}
	options := miniogo.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: target.Scheme == "https",
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConnsPerHost:   256,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: mustGetSystemCertPool(),
				// Can't use SSLv3 because of POODLE and BEAST
				// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
				// Can't use TLSv1.1 because of RC4 cipher usage
				MinVersion:         tls.VersionTLS12,
				NextProtos:         []string{"http/1.1"},
				InsecureSkipVerify: true,
			},
			// Set this value so that the underlying transport round-tripper
			// doesn't try to auto decode the body of objects with
			// content-encoding set to `gzip`.
			//
			// Refer:
			//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
			DisableCompression: true,
		},
		Region:       "",
		BucketLookup: 0,
	}

	return miniogo.New(target.Host, &options)
}

func copyAction(cliCtx *cli.Context) error {
	checkCopyArgsAndInit(cliCtx)
	srcClient, err = initMinioClient(cliCtx, srcAccessKey, srcSecretKey, srcEndpoint)
	if err != nil {
		return fmt.Errorf("could not initialize src client %w", err)
	}
	tgtClient, err = initMinioClient(cliCtx, tgtAccessKey, tgtSecretKey, tgtEndpoint)
	if err != nil {
		return fmt.Errorf("could not initialize tgt client %w", err)
	}
	ctx := context.Background()
	skip := cliCtx.Int("skip")
	inputFormat := cliCtx.String("input-format")
	inputFile := cliCtx.String("input-file")
	dryRun = cliCtx.Bool("fake")
	start := time.Now()

	var replicate bool
	switch inputFormat {
	case "drrepl":
		replicate = true
	case "s3-check-md5":
		replicate = false
		re = regexp.MustCompile(`.*CORRUPTED object: (.*)$`)
	case "compare-disks-ec":
		replicate = false
		re = regexp.MustCompile(`^CORRUPTED path=(.*), total=\d+, ecM=\d+, count=\d+$`)
	default:
		console.Fatalln("unknown --input-format", inputFormat)
	}

	if cliCtx.IsSet("simple-copy") {
		replicate = !cliCtx.Bool("simple-copy")
	}

	copyState = newCopyState(ctx, replicate)
	copyState.init(ctx)

	var err error
	var file *os.File

	if inputFile == "" {
		if dirPath == "" {
			console.Fatalln("--dir-path needs to be specified")
		}
		file, err = os.Open(path.Join(dirPath, objListFile))
		if err != nil {
			console.Fatalln("--input-file needs to be specified", err)
		}
	} else {
		file, err = os.Open(inputFile)
		if err != nil {
			console.Fatalln(err)
		}
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		o := scanner.Text()
		if skip > 0 {
			skip--
			continue
		}
		var slc []string
		switch inputFormat {
		case "drrepl":
			slc = strings.SplitN(o, ",", 4)
			if len(slc) < 3 || len(slc) > 4 {
				slc = nil
			}
		case "s3-check-md5", "compare-disks-ec":
			matches := re.FindAllStringSubmatch(o, -1)
			if len(matches) > 0 && len(matches[0]) >= 1 {
				slc = strings.SplitN(matches[0][1], "/", 2)
				// Version and Delete Marker are not supported yet
				slc = append(slc, "")
				slc = append(slc, "")
			}
		}

		if len(slc) == 0 {
			logDMsg(fmt.Sprintf("error processing line :%s ", o), nil)
			continue
		}

		obj := objInfo{
			bucket:       strings.TrimSpace(slc[0]),
			object:       strings.TrimSpace(slc[1]),
			versionID:    strings.TrimSpace(slc[2]),
			deleteMarker: strings.TrimSpace(slc[3]) == "true",
		}

		copyState.queueUploadTask(obj)
		logDMsg(fmt.Sprintf("adding %s to copy queue", o), nil)
	}
	if err := scanner.Err(); err != nil {
		logDMsg(fmt.Sprintf("error processing file :%s ", objListFile), err)
		return err
	}
	copyState.finish(ctx)
	if dryRun {
		logMsg("copy dry run complete")
	} else {
		end := time.Now()
		latency := end.Sub(start).Seconds()
		count := copyState.getCount() - copyState.getFailCount()
		logMsg(fmt.Sprintf("Copied %s / %s objects with latency %d secs", humanize.Comma(int64(count)), humanize.Comma(int64(copyState.getCount())), int64(latency)))
	}
	return nil
}
