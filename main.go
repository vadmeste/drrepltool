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
	"os"

	"github.com/minio/cli"
)

var version = "(dev)"

var subcommands = []cli.Command{
	// listCmd,
	// copyCmd,
	autoFixCmd,
}

// mainAction is the handle for "hcp-to-minio" command.
func mainAction(ctx *cli.Context) error {
	if !ctx.Args().Present() {
		cli.ShowCommandHelp(ctx, "")
		os.Exit(1)
	}
	command := ctx.Args().First()
	if command != "list" && command != "copy" && command != "auto-fix" {
		cli.ShowCommandHelp(ctx, "")
		os.Exit(1)
	}
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = os.Args[0]
	app.Author = "MinIO, Inc."
	app.Version = version
	app.Description = `copy tool to re-replicate/fix objects between two MinIO clusters`
	app.Flags = []cli.Flag{}
	app.Action = mainAction
	app.Commands = subcommands
	app.Run(os.Args)
}
