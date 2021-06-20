package main

import (
	"context"
	"github.com/go-ee/fsindexer"
	"github.com/go-ee/utils/lg"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	c := NewCli()

	if err := c.Run(os.Args); err != nil {
		log.Infof("run failed, %v, %v", os.Args, err)
	}
	log.Infof("done %v", os.Args)
}

type Cli struct {
	*cli.App
	source, includeFile, excludeFile, includeDir, excludeDir, includePath, excludePath string
	esURL, esUser, esPassword, elasticsearchIndex                                      string
	noOperation                                                                        bool
	chunkSize                                                                          int
}

func NewCli() (ret *Cli) {
	ret = &Cli{}
	ret.init()
	return
}

func (o *Cli) init() {

	lg.LogrusTimeAsTimestampFormatter()

	name := "File System Indexer"
	o.App = cli.NewApp()
	o.Usage = name
	o.Version = "1.0"
	o.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "source",
			Aliases:     []string{"s"},
			Usage:       "folder or file to index recursively",
			Destination: &o.source,
		},
		&cli.StringFlag{
			Name:        "includeFile",
			Aliases:     []string{"if"},
			Usage:       "include file regular expression",
			Value:       ".*\\.(doc|docx|pdf|htm|html)$",
			Destination: &o.includeFile,
		},
		&cli.StringFlag{
			Name:        "excludeFile",
			Aliases:     []string{"ef"},
			Usage:       "exclude file regular expression",
			Destination: &o.excludeFile,
		},
		&cli.StringFlag{
			Name:        "includeDir",
			Aliases:     []string{"id"},
			Usage:       "include dir regular expression",
			Destination: &o.includeDir,
		},
		&cli.StringFlag{
			Name:        "excludeDir",
			Aliases:     []string{"ed"},
			Usage:       "exclude dir regular expression",
			Value:       "^(\\.|~|sdk)",
			Destination: &o.excludeDir,
		},
		&cli.StringFlag{
			Name:        "includePath",
			Aliases:     []string{"ip"},
			Usage:       "include path regular expression",
			Destination: &o.includePath,
		},
		&cli.StringFlag{
			Name:        "excludePath",
			Aliases:     []string{"ep"},
			Usage:       "exclude path regular expression",
			Destination: &o.excludePath,
		},
		&cli.StringFlag{
			Name:        "elasticsearchURL",
			Aliases:     []string{"esURL"},
			Usage:       "Elasticsearch URL",
			Value:       "http://localhost:9200",
			Destination: &o.esURL,
		}, &cli.StringFlag{
			Name:        "elasticsearchUser",
			Aliases:     []string{"esUser"},
			Usage:       "Elasticsearch User",
			Value:       "elastic",
			Destination: &o.esUser,
		}, &cli.StringFlag{
			Name:        "elasticsearchPassword",
			Aliases:     []string{"esPassword"},
			Usage:       "Elasticsearch Password",
			Value:       "changeme",
			Destination: &o.esPassword,
		},
		&cli.StringFlag{
			Name:        "elasticsearchIndex",
			Aliases:     []string{"esI"},
			Usage:       "Index name of the Elasticsearch",
			Value:       "fs",
			Destination: &o.elasticsearchIndex,
		},
		&cli.IntFlag{
			Name:        "chunkSize",
			Aliases:     []string{"c"},
			Usage:       "Chunk size for a indexed document",
			Value:       3000,
			Destination: &o.chunkSize,
		},
	}

	o.Commands = []*cli.Command{
		{
			Name:  "index",
			Usage: "Start indexing",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:        "noOperation, nop",
					Usage:       "Only traversing without indexing",
					Destination: &o.noOperation,
				},
			},
			Action: func(c *cli.Context) (err error) {
				o.l().Info("index")

				var indexer *fsindexer.FsIndexer
				if indexer, err = o.buildIndexer(); err != nil {
					return
				}

				done := func(label string) {
					o.l().Infof("%v completed", label)
				}

				err = indexer.Index(done, o.noOperation)

				return
			},
		},
	}
}

func (o *Cli) l() *log.Entry {
	return log.WithFields(log.Fields{
		"source":      o.source,
		"includeFile": o.includeFile,
		"excludeFile": o.excludeFile,
		"includeDir":  o.includeDir,
		"excludeDir":  o.excludeDir,
		"includePath": o.includePath,
		"excludePath": o.excludePath,
		"esURL":       o.esURL,
		"esIndex":     o.elasticsearchIndex,
	})
}

func (o *Cli) buildIndexer() (ret *fsindexer.FsIndexer, err error) {
	return fsindexer.NewFsIndexer(
		o.source,
		o.includeFile,
		o.excludeFile,
		o.includeDir,
		o.excludeDir,
		o.includePath,
		o.excludePath,
		o.esURL,
		o.esUser,
		o.esPassword,
		o.elasticsearchIndex,
		o.chunkSize,
		context.Background())
}
