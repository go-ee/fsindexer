package fsindexer

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"code.sajari.com/docconv"
	elasticsearch "github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/k3a/html2text"
	log "github.com/sirupsen/logrus"
)

type FsIndexer struct {
	Source             string
	ChunkSize          int
	ElasticsearchURL   string
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

	es *elasticsearch.Client
	wg sync.WaitGroup
}

func NewFsIndexer(source string,
	includeFile string, excludeFile string,
	includeDir string, excludeDir string,
	includePath string, excludePath string,
	elasticsearchURL string, elasticsearchIndex string,
	chunkSize int, context context.Context) (ret *FsIndexer, err error) {

	ret = &FsIndexer{Source: source,
		ElasticsearchURL: elasticsearchURL, ElasticsearchIndex: elasticsearchIndex,
		ChunkSize: chunkSize,
		Context:   context}

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

	ret.es, err = elasticsearch.NewDefaultClient()

	if res, errInd := ret.es.Indices.Create(elasticsearchIndex); errInd != nil {
		log.Infof("cannot create index: %s, %s", errInd, res)
	} else {
		log.Infof("index created: %s", res)
	}

	return
}

func (o *FsIndexer) Index(done func(label string), nop bool) (err error) {
	err = filepath.Walk(o.Source, func(path string, info os.FileInfo, err error) error {
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

	h := md5.New()
	io.WriteString(h, path)
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
	} else {
		if o.htmls.MatchString(fileExt) {
			var bytes []byte
			if bytes, err = ioutil.ReadFile(path); err == nil {
				content = html2text.HTML2Text(string(bytes))
			}
		}
	}

	if err == nil {
		content := strings.Trim(content, " ")
		if len(content) == 0 {
			log.Infof("%v, no content, %v\n", info.Name(), path)
			o.indexChunk(id, 0, "", path, info, fileExt)
			return
		}

		content = o.spaces.ReplaceAllString(content, " ")
		content = o.dotsSpaces.ReplaceAllString(content, ". ")
		content = o.dots.ReplaceAllString(content, ".")

		if o.ChunkSize > 1 {
			chunks := chunkStringSpace(content, o.ChunkSize)
			for i, chunk := range chunks {
				o.indexChunk(id, i+1, chunk, path, info, fileExt)
			}
		} else {
			o.indexChunk(id, 0, content, path, info, fileExt)
		}
	} else {
		log.Infof("can't parse file %v, %v\n", path, err)
	}
	return
}

func (o *FsIndexer) indexChunk(id string, chunkNum int, content string,
	path string, info os.FileInfo, fileExt string) (err error) {
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
		Refresh:    "true",
	}

	var res *esapi.Response
	if res, err = req.Do(o.Context, o.es); err != nil {
		log.Warnf("error getting response, %s, %s", err, res)
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Warnf("[%s] error indexing document ID=%v", res.Status(), chunkID)
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

type Doc struct {
	Content string
	Num     int
	Path    string
	Name    string
	Type    string
}
