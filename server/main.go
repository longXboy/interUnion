package main

import (
	"flag"
	"fmt"
	"hash/crc64"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

const (
	serverNum = 2
)

var serverAddr string = "192.168.100.100"
var base uint64 = 1 << 63
var portBase int = 8081
var table *crc64.Table

func init() {
	table = crc64.MakeTable(crc64.ECMA)
}

func main() {
	lisPort := flag.Int("p", 8080, "listen port")
	flag.Parse()

	m := &http.ServeMux{}
	m.HandleFunc("/stream", streamHandlerFunc)
	m.HandleFunc("/inter", interHandlerFunc)

	log.Println("server listen at :", *lisPort)
	http.ListenAndServe(fmt.Sprintf(":%d", *lisPort), m)
}

func interHandlerFunc(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]struct{})
	var p []byte = make([]byte, 1024)
	var lastData []byte = make([]byte, 0)
	for {
		nRead, err := r.Body.Read(p)
		if nRead != 0 {
			for i := range p[:nRead] {
				if p[i] != 10 {
					lastData = append(lastData, p[i])
				} else {
					if len(lastData) == 0 {
						log.Println("zero data")
						continue
					}
					m[string(lastData)] = struct{}{}
					lastData = lastData[:0]
				}
			}
			fmt.Println("read P:", p[:nRead])
		}
		if err != nil {
			log.Println("read err:", err, nRead)
			break
		}
	}
	w.WriteHeader(200)
	for k, _ := range m {
		fmt.Println(k + "\n")
		w.Write([]byte(k + "\n"))
	}
}

func streamHandlerFunc(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var pipes [serverNum]struct {
		w *io.PipeWriter
		r *io.PipeReader
	}
	var waitGroup sync.WaitGroup
	outPipeR, outPipeW := io.Pipe()

	for i := 0; i < serverNum; i++ {
		pipeR, pipeW := io.Pipe()
		pipes[i] = struct {
			w *io.PipeWriter
			r *io.PipeReader
		}{w: pipeW, r: pipeR}
		waitGroup.Add(1)
		go func(idx int) {
			defer waitGroup.Done()
			resp, err := http.Post(fmt.Sprintf("http://%s:%d/inter", serverAddr, idx+portBase), "text", pipeR)
			if err != nil {
				log.Println("post data failed!", err, resp)
				return
			}
			defer resp.Body.Close()
			d, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("read all failed!", err)
			}
			fmt.Println("read all:", d)
			outPipeW.Write(d)
		}(i)
	}

	var p []byte = make([]byte, 1024)
	var lastData []byte = make([]byte, 0)
	for {
		nRead, err := r.Body.Read(p)
		if nRead != 0 {
			for i := range p[:nRead] {
				if p[i] != 10 {
					lastData = append(lastData, p[i])
				} else {
					if len(lastData) == 0 {
						log.Println("zero data")
						continue
					}
					idx := crc(lastData) / base
					var sendData []byte = make([]byte, len(lastData)+1)
					copy(sendData, lastData)
					sendData[len(sendData)-1] = 10
					pipes[idx].w.Write(sendData)
					lastData = lastData[:0]
				}
			}
		}
		if err != nil {
			log.Println("read req.body err:", err, nRead)
			break
		}

	}
	for i := 0; i < serverNum; i++ {

		pipes[i].w.Close()
		//pipes[i].r.CloseWithError(io.EOF)
	}
	w.WriteHeader(200)
	go func() {
		for {
			d := make([]byte, 1024)
			nRead, err := outPipeR.Read(d)
			if nRead != 0 {
				w.Write(d[:nRead])
			}
			if err != nil {
				fmt.Println("read outPUT failed!", err)
				return
			}
		}
	}()
	waitGroup.Wait()
	outPipeW.Close()
	outPipeR.CloseWithError(fmt.Errorf("EOF"))
}

func crc(s []byte) uint64 {
	return crc64.Checksum(s, table)
}
