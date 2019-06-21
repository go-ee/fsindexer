package main

import (
	"context"
	"fmt"
	"os"

	"github.com/go-ee/fsindexer"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const flagSource = "source"
const flagIncludeFile = "includeFileRegExp"
const flagExcludeFile = "excludeFileRegExp"
const flagIncludeDir = "includeDirRegExp"
const flagExcludeDir = "excludeDirRegExp"
const flagIncludePath = "includePathRegExp"
const flagExcludePath = "excludePathRegExp"
const flagElasticsearchURL = "elasticsearchURL"
const flagElasticsearchIndex = "elasticsearchIndex"
const flagChunkSize = "chunkSize"

const flagNoOperation = "noOperation"

func main() {

	name := "File System Indexer"
	runner := cli.NewApp()
	runner.Usage = name
	runner.Version = "1.0"
	runner.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagSource, "s"),
			Usage: "folder or file to index recursively",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagIncludeFile, "if"),
			Usage: "include file regular expression",
			Value: ".*\\.(doc|pdf|htm|html)$",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagExcludeFile, "ef"),
			Usage: "exclude file regular expression",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagIncludeDir, "id"),
			Usage: "include dir regular expression",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagExcludeDir, "ed"),
			Usage: "exclude dir regular expression",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagIncludePath, "ip"),
			Usage: "include path regular expression",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagExcludePath, "ep"),
			Usage: "exclude path regular expression",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagElasticsearchURL, "esURL"),
			Usage: "Elasticsearch URL",
			Value: "http://localhost:9200",
		},
		cli.StringFlag{
			Name:  fmt.Sprintf("%v, %v", flagElasticsearchIndex, "esI"),
			Usage: "Index name of the Elasticsearch",
			Value: "fs",
		},
		cli.IntFlag{
			Name:  fmt.Sprintf("%v, %v", flagChunkSize, "c"),
			Usage: "Chunk size for a indexed document",
			Value: 3000,
		},
	}

	runner.Commands = []cli.Command{
		{
			Name:  "index",
			Usage: "Start indexing",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  fmt.Sprintf("%v, %v", flagNoOperation, "nop"),
					Usage: "Only traversing without indexing",
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

				err = indexer.Index(done, c.Bool(flagNoOperation))

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
		flagSource:             c.GlobalString(flagSource),
		flagIncludeFile:        c.GlobalString(flagIncludeFile),
		flagExcludeFile:        c.GlobalString(flagExcludeFile),
		flagIncludeDir:         c.GlobalString(flagIncludeDir),
		flagExcludeDir:         c.GlobalString(flagExcludeDir),
		flagIncludePath:        c.GlobalString(flagIncludePath),
		flagExcludePath:        c.GlobalString(flagExcludePath),
		flagElasticsearchURL:   c.GlobalString(flagElasticsearchURL),
		flagElasticsearchIndex: c.GlobalString(flagElasticsearchIndex),
	})
}

func buildIndexer(c *cli.Context) (ret *fsindexer.FsIndexer, err error) {
	return fsindexer.NewFsIndexer(
		c.GlobalString(flagSource),
		c.GlobalString(flagIncludeFile),
		c.GlobalString(flagExcludeFile),
		c.GlobalString(flagIncludeDir),
		c.GlobalString(flagExcludeDir),
		c.GlobalString(flagIncludePath),
		c.GlobalString(flagExcludePath),
		c.GlobalString(flagElasticsearchURL),
		c.GlobalString(flagElasticsearchIndex),
		c.GlobalInt(flagChunkSize),
		context.Background())
}
