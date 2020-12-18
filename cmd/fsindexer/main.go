package main

import (
	"context"
	"os"

	"github.com/go-ee/fsindexer"
	"github.com/go-ee/utils/lg"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var source, includeFile, excludeFile, includeDir, excludeDir, includePath, excludePath string
var esURL, esUser, esPassword, elasticsearchIndex string
var noOperation bool
var chunkSize int

func main() {

	lg.LogrusTimeAsTimestampFormatter()

	name := "File System Indexer"
	runner := cli.NewApp()
	runner.Usage = name
	runner.Version = "1.0"
	runner.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "source",
			Aliases:     []string{"s"},
			Usage:       "folder or file to index recursively",
			Destination: &source,
		},
		&cli.StringFlag{
			Name:        "includeFile",
			Aliases:     []string{"if"},
			Usage:       "include file regular expression",
			Value:       ".*\\.(doc|docx|pdf|htm|html)$",
			Destination: &includeFile,
		},
		&cli.StringFlag{
			Name:        "excludeFile",
			Aliases:     []string{"ef"},
			Usage:       "exclude file regular expression",
			Destination: &excludeFile,
		},
		&cli.StringFlag{
			Name:        "includeDir",
			Aliases:     []string{"id"},
			Usage:       "include dir regular expression",
			Destination: &includeDir,
		},
		&cli.StringFlag{
			Name:        "excludeDir",
			Aliases:     []string{"ed"},
			Usage:       "exclude dir regular expression",
			Value:       "^(\\.|~|sdk)",
			Destination: &excludeDir,
		},
		&cli.StringFlag{
			Name:        "includePath",
			Aliases:     []string{"ip"},
			Usage:       "include path regular expression",
			Destination: &includePath,
		},
		&cli.StringFlag{
			Name:        "excludePath",
			Aliases:     []string{"ep"},
			Usage:       "exclude path regular expression",
			Destination: &excludePath,
		},
		&cli.StringFlag{
			Name:        "elasticsearchURL",
			Aliases:     []string{"esURL"},
			Usage:       "Elasticsearch URL",
			Value:       "http://localhost:9200",
			Destination: &esURL,
		}, &cli.StringFlag{
			Name:        "elasticsearchUser",
			Aliases:     []string{"esUser"},
			Usage:       "Elasticsearch User",
			Value:       "elastic",
			Destination: &esUser,
		}, &cli.StringFlag{
			Name:        "elasticsearchPassword",
			Aliases:     []string{"esPassword"},
			Usage:       "Elasticsearch Password",
			Value:       "changeme",
			Destination: &esPassword,
		},
		&cli.StringFlag{
			Name:        "elasticsearchIndex",
			Aliases:     []string{"esI"},
			Usage:       "Index name of the Elasticsearch",
			Value:       "fs",
			Destination: &elasticsearchIndex,
		},
		&cli.IntFlag{
			Name:        "chunkSize",
			Aliases:     []string{"c"},
			Usage:       "Chunk size for a indexed document",
			Value:       3000,
			Destination: &chunkSize,
		},
	}

	runner.Commands = []*cli.Command{
		{
			Name:  "index",
			Usage: "Start indexing",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:        "noOperation, nop",
					Usage:       "Only traversing without indexing",
					Destination: &noOperation,
				},
			},
			Action: func(c *cli.Context) (err error) {
				l(c).Info("index")

				var indexer *fsindexer.FsIndexer
				if indexer, err = buildIndexer(c); err != nil {
					return
				}

				done := func(label string) {
					l(c).Infof("%v completed", label)
				}

				err = indexer.Index(done, noOperation)

				return
			},
		},
	}

	if err := runner.Run(os.Args); err != nil {
		log.Infof("run failed, %v, %v", os.Args, err)
	}
	log.Infof("done %v", os.Args)
}

func l(c *cli.Context) *log.Entry {
	return log.WithFields(log.Fields{
		"source":      source,
		"includeFile": includeFile,
		"excludeFile": excludeFile,
		"includeDir":  includeDir,
		"excludeDir":  excludeDir,
		"includePath": includePath,
		"excludePath": excludePath,
		"esURL":       esURL,
		"esIndex":     elasticsearchIndex,
	})
}

func buildIndexer(c *cli.Context) (ret *fsindexer.FsIndexer, err error) {
	return fsindexer.NewFsIndexer(
		source,
		includeFile,
		excludeFile,
		includeDir,
		excludeDir,
		includePath,
		excludePath,
		esURL,
		esUser,
		esPassword,
		elasticsearchIndex,
		chunkSize,
		context.Background())
}
