package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cmceniry/frank"
	"github.com/cmceniry/golokia"
	"net"
	"os"
	"time"
	"github.com/gocql/gocql"
)

var (
	ErrHistConvert     = errors.New("Did not convert correctly")
	ErrHistLenMismatch = errors.New("Did not return the right number of values")
	ErrHistEleConvert  = errors.New("Did not convert element correctly")
)

var gclient = golokia.NewClient("localhost", "7025")

type ClusterInfo struct {
	dst string
	Name string
	Nodes []string
	ColumnFamilies [][2]string
}

func getClusterInfo(dst string) (*ClusterInfo, error) {
  ret := &ClusterInfo{dst, "", make([]string, 0), make([][2]string, 0)}

	// Name
  cluster := gocql.NewCluster(dst)
	cluster.ProtoVersion = 1
	cluster.Keyspace = "system"
	cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	q  := session.Query("SELECT cluster_name FROM local LIMIT 1")
	if err != nil {
		return nil, err
	}
	err = q.Scan(&ret.Name)
	if err != nil {
	  return nil, err
	}

  // Skipping host identification for now
  var res1, res2 string
  // iter := session.Query("SELECT host_id FROM local").Iter()
	// for iter.Scan(&res1) {
	// 	ret.Nodes = append(ret.Nodes, res1)
	// }
	// if err := iter.Close(); err != nil {
	// 	return nil, err
	// }
	var iter *gocql.Iter

  iter = session.Query("SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies").Iter()
	for iter.Scan(&res1, &res2) {
		ret.ColumnFamilies = append(ret.ColumnFamilies, [2]string{res1, res2})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return ret, nil
}

func getHistogram(ks, cf, field string) ([]float64, error) {
	bean := fmt.Sprintf("columnfamily=%s,keyspace=%s,type=ColumnFamilies", cf, ks)
	lrlhm, err := gclient.GetAttr("org.apache.cassandra.db", bean, field)
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

func collect(ci *ClusterInfo, keyspace string, columnfamily string, operation string, sink chan frank.NamedSample) {
	if res, err := getHistogram(keyspace, columnfamily, operation); err != nil {
		fmt.Printf("Error in collector(%s,%s,%s): %s\n", keyspace, columnfamily, operation, err)
	} else {
		name := ci.Name + ":" + ci.dst + ":" + keyspace + "." + columnfamily + ":" + operation
		select {
		case sink <- frank.NamedSample{frank.Sample{time.Now().UnixNano()/1e6, res}, name}:
			// Normal behavior
		default:
			// Otherwise, don't do anything with the data since we don't want to block
		}
	}
}

func forward(src chan frank.NamedSample, dst string) {
	for {
		conn, err := net.Dial("tcp", dst)
		if err != nil {
			fmt.Printf("Error in forwawrder: %s\n", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()
		enc := gob.NewEncoder(conn)
		pointsSent := 0
		for {
			err := enc.Encode(<-src)
			if err != nil {
				fmt.Printf("Error in forwawrder: %s\n", err)
				time.Sleep(5 * time.Second)
				break
			}
			pointsSent += 1
			if pointsSent % 100 == 0 {
				fmt.Printf("%d data points sent\n", pointsSent)
			}
		}
	}
}

func main() {
  if len(os.Args) != 3 {
    fmt.Fprintf(os.Stderr, "Invalid command line : must specify target node (ip/name), and central (ip/name:port)")
    os.Exit(-1)
  }
	target := os.Args[1]
	central := os.Args[2]

	ci, err := getClusterInfo(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to get cluster info : %s\n", err)
	}

	stream := make(chan frank.NamedSample)
	go func() {
		for _ = range time.Tick(5 * time.Second) {
			for _, cf := range ci.ColumnFamilies {
				collect(ci, cf[0], cf[1], "LifetimeWriteLatencyHistogramMicros", stream)
				collect(ci, cf[0], cf[1], "LifetimeReadLatencyHistogramMicros", stream)
			}
		}
	}()
	go forward(stream, central)

	for {
		time.Sleep(100 * time.Second)
	}
}
