package main

import (
	"encoding/json"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cmceniry/frank"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
	"github.com/gorilla/mux"
	"github.com/gorilla/handlers"
	"os"
)

type MyResp struct {
	TS string
	Data []float64
}

type frankserver struct {
	U *frank.Utility
	Storer chan frank.NamedSample
	Printer chan frank.NamedSample
	Print bool
}

var (
	ErrHistConvert     = errors.New("Did not convert correctly")
	ErrHistLenMismatch = errors.New("Did not return the right number of values")
	ErrHistEleConvert  = errors.New("Did not convert element correctly")
)

func (f *frankserver) CollectorListen() {
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
					f.Storer <- res
				}
			}(conn)
		}
	}
}

func (f *frankserver) Store() {
	for {
		chunk := <- f.Storer
		if true {
			f.Printer <- chunk
		}
		names := strings.Split(chunk.Name, ":")
		if len(names) != 4 {
			continue
		}
		if err := f.U.AddSample(names[0], names[1], names[2], names[3], chunk.Sample); err != nil {
			if _, err := f.U.NewMeter(names[0], names[1], names[2], names[3]); err != nil {
				continue
			}
			f.U.AddSample(names[0], names[1], names[2], names[3], chunk.Sample)
		}
	}
}

func (f *frankserver) PrintIncoming() {
	for {
		chunk := <- f.Printer
		if f.Print {
			fmt.Printf("%s : %v\n", time.Unix(chunk.TimestampMS/1e3, 0).Format("00:00:00"), chunk.Data)
		}
	}
}

func (f *frankserver) rawHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	m, err := f.U.GetMeter(vars["cluster"], vars["keyspace"], vars["cf"], vars["op"])
	if err != nil {
		fmt.Printf("%v\n", f.U.ClusterNames())
		w.WriteHeader(http.StatusNotFound)
		return
	}
	raw, err := m.Raw()
	dstr := make([]MyResp, len(raw))
	for x, val := range raw {
		dstr[x] = MyResp{fmt.Sprintf("%d", val.TimestampMS),val.Data}
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

func (f *frankserver) alignHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	m, err := f.U.GetMeter(vars["cluster"], vars["keyspace"], vars["cf"], vars["op"])
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	starttime := (time.Now().Unix()/5 - 100)*5
	endtime   := (time.Now().Unix()/5)*5
	dstr, _ := m.Raw()
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

func (f *frankserver) listClusters(w http.ResponseWriter, r *http.Request) {
	c := f.U.ClusterNames()
	cjson, err := json.Marshal(c)
	if err != nil {
		log.Printf("Unable to marshal: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(cjson)
}

func (f *frankserver) showCluster(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ci := make(map[string][]string)
	ci["name"] = []string{vars["cluster"]}
	ci["nodes"] = f.U.NodeNames(vars["cluster"])
	ci["columnfamilies"] = f.U.CFNames(vars["cluster"])
	cijson, err := json.Marshal(ci)
	if err != nil {
		log.Printf("Unable to marshal: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(cijson)
	return
}

func main() {

  f := frankserver{
		frank.NewUtility(),
		make(chan frank.NamedSample),
		make(chan frank.NamedSample),
		false,
	}
	f.U.Load()

	go f.CollectorListen()
	go f.Store()
	go f.PrintIncoming()
	go func(){
		for _ = range time.Tick(30 * time.Second) {
			fmt.Printf("Save\n")
			f.U.Save()
			fmt.Printf("Done\n")
		}
	}()

	r := mux.NewRouter()
	r.HandleFunc("/raw/{cluster}/{keyspace}/{cf}/{op}", f.rawHandler)
	r.HandleFunc("/align/{cluster}/{keyspace}/{cf}/{op}", f.alignHandler)
	r.PathPrefix("/test").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusFound)
		fmt.Fprintf(w, "Welcome to the home page!\n")
		return
	})
	r.HandleFunc("/clusters", f.listClusters)
	r.HandleFunc("/clusters/{cluster}", f.showCluster)
	r.PathPrefix("/static").Handler(http.StripPrefix("/static", http.FileServer(http.Dir("static"))))
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/static/play.html", http.StatusFound)
		return
	})
	http.Handle("/", handlers.LoggingHandler(os.Stdout, r))
	http.ListenAndServe(":4270", nil)

	for {
		time.Sleep(100 * time.Second)
	}
}
