package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/cmceniry/frank"
	"github.com/cmceniry/golokia"
	"log"
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
		//fmt.Printf("%v\n", lrlhm)
		return nil, ErrHistConvert
	}
	if len(l) != len(frank.Labels) {
		//fmt.Printf("%d\n%d\n", len(l), len(frank.Labels))
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

func collector(sink chan []float64) {
	for {
		if res, err := getHistogram(config.Keyspace, config.ColumnFamily, config.Operation); err != nil {
			fmt.Printf("Error: %s\n", err)
		} else {
			sink <- res
		}
		time.Sleep(2 * time.Second)
	}
}

// Periodically sweeps out older values
// The second value is the number of entries to try to keep this to
func cleanup(data map[int64][]float64, length int) {
	for {
		time.Sleep(20 * time.Second)
		//fmt.Printf("Running cleanup\n")
		if len(data) > length {
			keys := make(int64Slice, len(data)+2)
			count := 0
			for k, _ := range data {
				keys[count] = k
				count = count + 1
			}
			keys = keys[:count]
			//fmt.Printf("%s\n", keys)
			sort.Sort(keys)
			//fmt.Printf("%s\n", keys)
			//fmt.Printf("%s\n", len(data))
			for i := 0; i<len(keys)-length; i++ {
				delete(data, keys[i])
			}
			//fmt.Printf("%s\n", len(data))
		}
		//fmt.Printf("-Running cleanup\n")
	}
}

var (
	d = make(map[int64][]float64)
)

func storer(source chan []float64) {
	go cleanup(d, 500)
	for {
		chunk := <- source
		//fmt.Printf("%v\n", chunk)
		d[time.Now().UnixNano()/1000000] = chunk
		//fmt.Printf("Current size: %d\n", len(d))
	}
}

func printer(source chan []float64) {
	for {
		data := <- source
		fmt.Printf("%s : %v\n", time.Now(), data)
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
	if path[1] != "localhost" && path[2] != config.Keyspace && path[3] != config.ColumnFamily {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if path[4] != config.Operation {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	dts := make(int64Slice, len(d))
	count := 0
	for ts, _ := range d {
		dts[count] = ts
		count = count + 1
	}
	sort.Sort(dts)
	dstr := make([]MyResp, len(dts))
	for x := range dts {
		dstr[x] = MyResp{fmt.Sprintf("%d", dts[x]),d[dts[x]]}
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
	if path[1] != "localhost" && path[2] != config.Keyspace && path[3] != config.ColumnFamily {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if path[4] != config.Operation {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	dts := make(int64Slice, len(d))
	count := 0
	for ts, _ := range d {
		dts[count] = ts
		count++
	}
	sort.Sort(dts)
	dstr := make([]frank.Sample, len(dts))
	for x := range dts {
		dstr[x] = frank.Sample{dts[x], d[dts[x]]}
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

	stream := make(chan []float64)
	go collector(stream)
	go storer(stream)

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
