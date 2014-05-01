package main

import (
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"github.com/cmceniry/frank"
	"github.com/cmceniry/golokia"
	"net"
	"os"
	"time"
)

type MyConfig struct {
	Host		string
	Port		int
	Keyspace	string
	ColumnFamily	string
	Operation	string
	Destination	string
}

var config = MyConfig{}

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

func collector(sink chan frank.NamedSample) {
	for _ = range time.Tick(5 * time.Second) {
		if res, err := getHistogram(config.Keyspace, config.ColumnFamily, config.Operation); err != nil {
			fmt.Printf("Error in collector: %s\n", err)
		} else {
			name := "localhost/" + config.Keyspace + "/" + config.ColumnFamily + "/" + config.Operation
			select {
			case sink <- frank.NamedSample{frank.Sample{time.Now().UnixNano()/1e6, res}, name}:
				// Normal behavior
			default:
				// Otherwise, don't do anything with the data since we don't want to block
			}
		}
	}
}

func forwarder(src chan frank.NamedSample, dst string) {
	for {
		conn, err := net.Dial("tcp", dst)
		if err != nil {
			fmt.Printf("Error in forwawrder: %s\n", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()
		enc := gob.NewEncoder(conn)
		for {
			err := enc.Encode(<-src)
			if err != nil {
				fmt.Printf("Error in forwawrder: %s\n", err)
				time.Sleep(5 * time.Second)
				break
			}
		}
	}
}

func main() {
	var op string
	flag.StringVar(&config.Host, "host", "localhost", "Jolokia Host to connect to")
	flag.IntVar(&config.Port, "port", 7025, "Jolokia Port to connect to")
	flag.StringVar(&config.Keyspace, "keyspace", "Keyspace1", "Keyspace to connect to")
	flag.StringVar(&config.ColumnFamily, "cf", "Standard1", "ColumnFamily to get metrics for")
	flag.StringVar(&op, "op", "Write", "Type of operation to get metrics for: Read or Write")
	flag.StringVar(&config.Destination, "destination", "127.0.0.1:4271", "TCP IP:Port to forward results to")
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
	go collector(stream)
	go forwarder(stream, config.Destination)

	for {
		time.Sleep(100 * time.Second)
	}
}
