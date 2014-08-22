package main

import (
	"encoding/json"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"github.com/cmceniry/frank"
	"github.com/cmceniry/golokia"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type MyConfig struct {
	Host		string
	Port		int
	Keyspace	string
	ColumnFamily	string
	Operation	string
}

var config = MyConfig{}

type int64Slice []int64
func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type MyResp struct {
	TS string
	Data []float64
}

var (
	ErrHistConvert     = errors.New("Did not convert correctly")
	ErrHistLenMismatch = errors.New("Did not return the right number of values")
	ErrHistEleConvert  = errors.New("Did not convert element correctly")
)

func getHistogram(ks, cf, field string) ([]float64, error) {
	bean := fmt.Sprintf("columnfamily=%s,keyspace=%s,type=ColumnFamilies", cf, ks)
	lrlhm, err := golokia.GetAttr("http://localhost:7025", "org.apache.cassandra.db", bean, field)
	if err != nil {
		return nil, err
	}
	l, ok := lrlhm.([]interface{})
	if !ok {
		return nil, ErrHistConvert
	}
	if len(l) != len(frank.Labels) {
		return nil, ErrHistLenMismatch
	}
	ret := make([]float64, len(l))
	for idx, val := range l {
		if valF, ok := val.(float64); ok {
			ret[idx] = valF
		} else {
			return nil, ErrHistEleConvert
		}
	}
	return ret, nil
}

func listener(sink chan frank.NamedSample) {
	for {
		ln, err := net.Listen("tcp", ":4271")
		if err != nil {
			fmt.Printf("Error starting listener: %s\n", err)
			time.Sleep(5 * time.Second)
			break
		}
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Printf("Error accepting a connection: %s\n", err)
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				var (
					res frank.NamedSample
					err error
				)
				dec := gob.NewDecoder(c)
				for {
					err = dec.Decode(&res)
					if err != nil {
						fmt.Printf("Error receiving %s\n", err)
						break
					}
					sink <- res
				}
			}(conn)
		}
	}
}

// Periodically sweeps out older values
// The second value is the number of entries to try to keep this to
func cleanup(data *frank.Meter, length int) {
	for {
		time.Sleep(20 * time.Second)
		if len(data.Data) > length {
			keys := make(int64Slice, len(data.Data)+2)
			count := 0
			for k, _ := range data.Data {
				keys[count] = k
				count = count + 1
			}
			keys = keys[:count]
			sort.Sort(keys)
			for i := 0; i<len(keys)-length; i++ {
				delete(data.Data, keys[i])
			}
		}
	}
}

var (
	d = make(map[string]*frank.Meter)
)

func storer(source chan frank.NamedSample) {
	for {
		chunk := <- source
		if _, ok := d[chunk.Name]; !ok {
			d[chunk.Name] = &frank.Meter{chunk.Name, make(map[int64]frank.Sample, 10)}
			go cleanup(d[chunk.Name], 500)
		}
		d[chunk.Name].Data[chunk.Sample.TimestampMS] = frank.Sample{chunk.Sample.TimestampMS, make([]float64, len(chunk.Sample.Data))}
		for k, v := range chunk.Sample.Data {
			d[chunk.Name].Data[chunk.Sample.TimestampMS].Data[k] = v
		}
	}
}

func printer(source chan frank.NamedSample) {
	for {
		data := <- source
		fmt.Printf("%s : %v\n", time.Unix(data.TimestampMS/1e3, 0).Format("00:00:00"), data.Data)
	}
}

func outputer() {
	for _ = range time.Tick(5 * time.Second) {
		fmt.Printf("OUTPUT (%d)\n", len(d))
		for src, _ := range d {
			fmt.Printf("%s : %d\n", src, len(d[src].Data))
		}
		fmt.Printf("\n")
	}
}

func rawHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path,"/")[1:]
	if len(path) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if path[0] != "raw" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if len(path) < 4 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	name := path[1] + "/" + path[2] + "/" + path[3] + "/" + path[4]
	m, ok := d[name]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	dts := make(int64Slice, len(m.Data))
	count := 0
	for ts, _ := range m.Data {
		dts[count] = ts
		count = count + 1
	}
	sort.Sort(dts)
	dstr := make([]MyResp, len(dts))
	for x := range dts {
		dstr[x] = MyResp{fmt.Sprintf("%d", dts[x]),m.Data[dts[x]].Data}
	}
	djson, err := json.Marshal(dstr)
	if err != nil {
		log.Printf("Unable to marshal: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(djson)
}

func alignHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path, "/")[1:]
	if len(path) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if path[0] != "align" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if len(path) < 4 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	m, ok := d[path[1] + "/" + path[2] + "/" + path[3] + "/" + path[4]]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	dts := make(int64Slice, len(m.Data))
	count := 0
	for ts, _ := range m.Data {
		dts[count] = ts
		count++
	}
	sort.Sort(dts)
	dstr := make([]frank.Sample, len(dts))
	for x := range dts {
		dstr[x] = frank.Sample{dts[x], m.Data[dts[x]].Data}
	}
	starttime := (time.Now().Unix()/5 - 100)*5
	endtime   := (time.Now().Unix()/5)*5
	dstr = frank.Align(dstr, 5000, starttime * 1000, endtime * 1000)
	dstr = frank.Diff(dstr)
	djson, err := json.Marshal(dstr)
	if err != nil {
		log.Printf("Unable to marshal: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(djson)
}

func main() {
	var op string
	flag.StringVar(&config.Host, "host", "localhost", "Jolokia Host to connect to")
	flag.IntVar(&config.Port, "port", 7025, "Jolokia Port to connect to")
	flag.StringVar(&config.Keyspace, "keyspace", "Keyspace1", "Keyspace to connect to")
	flag.StringVar(&config.ColumnFamily, "cf", "Standard1", "ColumnFamily to get metrics for")
	flag.StringVar(&op, "op", "Write", "Type of operation to get metrics for: Read or Write")
	flag.Parse()

	switch op {
	case "Write":
		config.Operation = "LifetimeWriteLatencyHistogramMicros"
	case "Read":
		config.Operation = "LifetimeReadLatencyHistogramMicros"
	default:
		fmt.Println("Operation must be Read or Write")
		os.Exit(-1)
	}

	stream := make(chan frank.NamedSample)
	go listener(stream)
	go storer(stream)
	go outputer()

	http.HandleFunc("/raw/", rawHandler)
	http.HandleFunc("/align/", alignHandler)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/static/play.html#/" + config.Host + "/" + config.Keyspace + "/" + config.ColumnFamily + "/" + config.Operation, http.StatusFound)
		return
	})
	http.ListenAndServe(":4270", nil)

	for {
		time.Sleep(100 * time.Second)
	}
}
