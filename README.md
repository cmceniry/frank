# Frank (working title)

This is a heatmap generator for Cassandra.

Currently only records the last 500 seconds of histogram data for the Keyspace1.Standard1 write values.

## How to run

* Set your GOPATH as appropriate.
* go get http://github.com/cmceniry/frank
* cd $GOPATH/src/github.com/cmceniry/frank/tools/frankserv
* go run serve.go
* Point your browser at http://localhost:4270/static.play.html

