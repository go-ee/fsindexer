package fsindexer

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"code.sajari.com/docconv"
	"github.com/elastic/go-elasticsearch/esapi"
	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/k3a/html2text"
	log "github.com/sirupsen/logrus"
)

type FsIndexer struct {
	Source             string
	ChunkSize          int
	ElasticsearchIndex string
	Context            context.Context

	includeFile *regexp.Regexp
	excludeFile *regexp.Regexp
	includeDir  *regexp.Regexp
	excludeDir  *regexp.Regexp
	includePath *regexp.Regexp
	excludePath *regexp.Regexp

	spaces     *regexp.Regexp
	dotsSpaces *regexp.Regexp
	dots       *regexp.Regexp
	htmls      *regexp.Regexp

	sleepDurationAtTooManyRequests time.Duration
	es                             *elasticsearch.Client
	wg                             sync.WaitGroup
}

func NewFsIndexer(source, includeFile, excludeFile, includeDir, excludeDir, includePath, excludePath,
	esURL, esUser, esPassword, esIndex string,
	chunkSize int, context context.Context) (ret *FsIndexer, err error) {

	ret = &FsIndexer{Source: source, ElasticsearchIndex: esIndex, ChunkSize: chunkSize, Context: context,
		sleepDurationAtTooManyRequests: time.Second,
	}

	ret.spaces = regexp.MustCompile("\\s+")
	ret.dotsSpaces = regexp.MustCompile("(\\. )+")
	ret.dots = regexp.MustCompile("\\.+")
	ret.htmls = regexp.MustCompile("htm?")

	if includeFile != "" {
		ret.includeFile = regexp.MustCompile(includeFile)
	}
	if excludeFile != "" {
		ret.excludeFile = regexp.MustCompile(excludeFile)
	}
	if includeDir != "" {
		ret.includeDir = regexp.MustCompile(includeDir)
	}
	if excludeDir != "" {
		ret.excludeDir = regexp.MustCompile(excludeDir)
	}
	if includePath != "" {
		ret.includePath = regexp.MustCompile(includePath)
	}
	if excludePath != "" {
		ret.excludePath = regexp.MustCompile(excludePath)
	}

	esCfg := elasticsearch.Config{
		Addresses: []string{
			esURL,
		},
		Username: esUser,
		Password: esPassword,
	}

	ret.es, err = elasticsearch.NewClient(esCfg)

	if res, errInd := ret.es.Indices.Create(esIndex); errInd != nil {
		log.Infof("cannot create index: %s, %s", errInd, res)
	} else {
		log.Infof("index created: %s", res)
	}

	return
}

func (o *FsIndexer) Index(done func(label string), nop bool) (err error) {
	err = filepath.Walk(o.Source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Warnf("%v: %v\n", path, err)
			return nil
		}

		excludePath := o.excludePath == nil || !o.excludePath.MatchString(path)
		includePath := o.includePath == nil || o.includePath.MatchString(path)
		if info.IsDir() {
			excludeDir := o.excludeDir == nil || !o.excludeDir.MatchString(info.Name())
			includeDir := o.includeDir == nil || o.includeDir.MatchString(info.Name())
			if excludePath && includePath && excludeDir && includeDir {
				if nop {
					log.Infof("nop: index folder %v\n", path)
				} else {
					log.Infof("index folder %v\n", path)
				}
			} else {
				log.Infof("skip folder %v\n", path)
				return filepath.SkipDir
			}
		} else {
			exclude := o.excludeFile == nil || !o.excludeFile.MatchString(info.Name())
			include := o.includeFile == nil || o.includeFile.MatchString(info.Name())
			if excludePath && includePath && exclude && include {
				if nop {
					log.Infof("nop: index %v\n", path)
				} else {
					//go o.indexFile(path, info)
					o.indexFile(path, info)
				}
			} else {
				log.Debugf("skip file %v\n", path)
			}
		}
		return nil
	})
	o.wg.Wait()
	return
}

func (o *FsIndexer) indexFile(path string, info os.FileInfo) (err error) {
	defer func() {
		rc := recover()
		if rc != nil {
			log.Warnf("%v:%v => %v\n", path, info, rc)
		}
	}()

	h := md5.New()
	_, err = io.WriteString(h, path)
	id := fmt.Sprintf("%x", h.Sum(nil))

	var existChunk bool
	if o.ChunkSize > 1 {
		existChunk, err = o.existsChunk(id, 1)
	} else {
		existChunk, err = o.existsChunk(id, 0)
	}
	if existChunk {
		log.Infof("%v exists already, skip\n", info.Name())
		return
	}

	fileExt := strings.TrimLeft(strings.ToLower(filepath.Ext(info.Name())), ".")
	log.Infof("parse %v\n", info.Name())
	var content string
	var res *docconv.Response
	if res, err = docconv.ConvertPath(path); err == nil {
		content = res.Body
		content = strings.Trim(content, " ")
	}
	if err != nil || content == "" {
		if o.htmls.MatchString(fileExt) {
			var bs []byte
			if bs, err = ioutil.ReadFile(path); err == nil {
				content = string(bs)
				content = strings.Trim(content, " ")
				content2 := html2text.HTML2Text(content)
				if content2 != "" {
					content = content2
				}
			}
		}
	}

	if err == nil {
		if len(content) == 0 {
			log.Infof("%v, no content, %v\n", info.Name(), path)
			err = o.indexChunkAndWaitAndRepeatIfTooManyRequests(id, 0, "", path, info, fileExt)
			return
		}

		content = o.spaces.ReplaceAllString(content, " ")
		content = o.dotsSpaces.ReplaceAllString(content, ". ")
		content = o.dots.ReplaceAllString(content, ".")

		if o.ChunkSize > 1 {
			chunks := chunkStringSpace(content, o.ChunkSize)
			for i, chunk := range chunks {
				err = o.indexChunkAndWaitAndRepeatIfTooManyRequests(id, i+1, chunk, path, info, fileExt)
			}
		} else {
			err = o.indexChunkAndWaitAndRepeatIfTooManyRequests(id, 0, content, path, info, fileExt)
		}
	} else {
		log.Infof("can't parse file %v, %v\n", path, err)
	}
	return
}

func (o *FsIndexer) indexChunkAndWaitAndRepeatIfTooManyRequests(
	id string, chunkNum int, content string, path string, info os.FileInfo, fileExt string) (err error) {
	if err = o.indexChunk(id, chunkNum, content, path, info, fileExt); err != nil {
		if isTooManyRequests(err) {
			// sleep for the first time
			log.Warnf("sleep %v, because of %v", o.sleepDurationAtTooManyRequests, err)
			time.Sleep(o.sleepDurationAtTooManyRequests)
			if err = o.indexChunk(id, chunkNum, content, path, info, fileExt); err != nil {
				if isTooManyRequests(err) {
					// increase sleep duration and start recursive
					o.sleepDurationAtTooManyRequests = o.sleepDurationAtTooManyRequests * 2
					log.Warnf("increase sleep duration to %v", o.sleepDurationAtTooManyRequests)
					err = o.indexChunkAndWaitAndRepeatIfTooManyRequests(id, chunkNum, content, path, info, fileExt)
				}
			}
		}
	}
	return
}

func isTooManyRequests(err error) bool {
	return strings.Contains(err.Error(), "Too Many Requests")
}

func (o *FsIndexer) indexChunk(
	id string, chunkNum int, content string, path string, info os.FileInfo, fileExt string) (err error) {

	log.Infof("%v, %v chunk, size %v\n", info.Name(), chunkNum, len(content))
	o.wg.Add(1)
	defer o.wg.Done()

	// Build the request body.
	doc := Doc{
		Content: content,
		Num:     chunkNum,
		Path:    path,
		Name:    info.Name(),
		Type:    fileExt,
	}

	var docBytes []byte
	if docBytes, err = json.Marshal(doc); err != nil {
		log.Warnf("error marshaling, %s", err)
		return
	}

	chunkID := buildChunkID(id, chunkNum)

	req := esapi.IndexRequest{
		Index:      o.ElasticsearchIndex,
		DocumentID: chunkID,
		Body:       bytes.NewReader(docBytes),
		// Refresh:    "true",
		WaitForActiveShards: "1",
	}

	var res *esapi.Response
	if res, err = req.Do(o.Context, o.es); err != nil {
		log.Warnf("error getting response, %s, %s", err, res)
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		err = errors.New(fmt.Sprintf("[%s] error indexing document ID=%v", res.Status(), chunkID))
	}
	return
}

func (o *FsIndexer) existsChunk(id string, chunkNum int) (ret bool, err error) {
	chunkID := buildChunkID(id, chunkNum)

	req := esapi.GetRequest{
		Index:      o.ElasticsearchIndex,
		DocumentID: chunkID,
	}

	var res *esapi.Response
	if res, err = req.Do(o.Context, o.es); err != nil {
		log.Warnf("error getting response, %s, %s", err, res)
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		ret = false
		//log.Warnf("[%s] error check exists document ID=%v", res.Status(), chunkID)
	} else {
		var existsBody map[string]interface{}
		json.NewDecoder(res.Body).Decode(&existsBody)
		if exists, ok := existsBody["found"]; ok {
			ret = exists.(bool)
		}
	}
	return
}

func buildChunkID(id string, chunkNum int) (ret string) {
	if chunkNum > 0 {
		ret = fmt.Sprintf("%v_%v", id, chunkNum)
	} else {
		ret = id
	}
	return
}

func chunkStringSpace(s string, chunkSize int) []string {
	var chunks []string
	current := strings.Builder{}
	parts := strings.Split(s, " ")

	chunkSizeLessSpace := chunkSize - 1

	for _, part := range parts {
		current.WriteString(part)
		if current.Len() >= chunkSizeLessSpace {
			chunks = append(chunks, current.String())
			current.Reset()
		} else {
			current.WriteString(" ")
		}
	}
	if current.Len() > 0 {
		chunks = append(chunks, current.String())
	}
	return chunks
}

func chunkString(s string, chunkSize int) []string {
	var chunks []string
	runes := []rune(s)

	if len(runes) == 0 || chunkSize == 0 {
		return []string{s}
	}

	for i := 0; i < len(runes); i += chunkSize {
		nn := i + chunkSize
		if nn > len(runes) {
			nn = len(runes)
		}
		chunks = append(chunks, string(runes[i:nn]))
	}
	return chunks
}

/*
func htmlToPdf() {
	pdfg := wkhtmltopdf.NewPDFPreparer()
	htmlfile, err := ioutil.ReadFile("./testfiles/htmlsimple.html")
	if err != nil {
		log.Fatal(err)
	}

	pdfg.AddPage(NewPageReader(bytes.NewReader(htmlfile)))
	pdfg.Dpi.Set(600)

	// The contents of htmlsimple.html are saved as base64 string in the JSON file
	jb, err := pdfg.To .ToJSON()
	if err != nil {
		log.Fatal(err)
	}
}
*/

type Doc struct {
	Content string
	Num     int
	Path    string
	Name    string
	Type    string
}
