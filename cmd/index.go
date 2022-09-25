/*
Copyright © 2022 Eugen Eisler <eoeisler@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package cmd

import (
	"context"
	"github.com/go-ee/fsindexer/fsind"
	"github.com/go-ee/utils/lg"

	"github.com/spf13/cobra"
)

var (
	source, includeFile, excludeFile, includeDir, excludeDir, includePath, excludePath string
	esURL, esUser, esPassword, elasticsearchIndex                                      string
	noOperation                                                                        bool
	chunkSize                                                                          int
)

// indexCmd represents the index command
var indexCmd = &cobra.Command{
	Use: "index",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		lg.LOG.Info("index")

		var indexer *fsind.FsIndexer
		if indexer, err = buildIndexer(); err != nil {
			return
		}

		done := func(label string) {
			lg.LOG.Infof("%v completed", label)
		}

		err = indexer.Index(done, noOperation)

		return
	},
}

func buildIndexer() (ret *fsind.FsIndexer, err error) {
	return fsind.NewFsIndexer(
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

func init() {
	rootCmd.AddCommand(indexCmd)

	indexCmd.PersistentFlags().StringVarP(&source, "source", "s", "", "folder or file to index recursively")
	indexCmd.PersistentFlags().StringVarP(&includeFile, "includeFile", "", ".*\\.(doc|docx|pdf|htm|html)$", "include file regular expression")
	indexCmd.PersistentFlags().StringVarP(&excludeFile, "excludeFile", "", "", "exclude file regular expression")
	indexCmd.PersistentFlags().StringVarP(&includeDir, "includeDir", "", "", "include dir regular expression")
	indexCmd.PersistentFlags().StringVarP(&excludeDir, "excludeDir", "", "^(\\.|~|sdk)", "exclude dir regular expression")
	indexCmd.PersistentFlags().StringVarP(&includePath, "includePath", "", "", "include path regular expression")
	indexCmd.PersistentFlags().StringVarP(&excludePath, "excludePath", "", "", "exclude path regular expression")
	indexCmd.PersistentFlags().StringVarP(&esURL, "elasticsearchURL", "", "http://localhost:9200", "Elasticsearch URL")
	indexCmd.PersistentFlags().StringVarP(&esUser, "elasticsearchUser", "", "elastic", "Elasticsearch User")
	indexCmd.PersistentFlags().StringVarP(&esPassword, "elasticsearchPassword", "", "changeme", "Elasticsearch Password")
	indexCmd.PersistentFlags().StringVarP(&elasticsearchIndex, "elasticsearchIndex", "", "fs", "Index name of the Elasticsearch")
	indexCmd.PersistentFlags().IntVarP(&chunkSize, "chunkSize", "", 3000, "Chunk size for a indexed document")
}
