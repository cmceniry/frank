package frank

import (
  "fmt"
)

type Utility struct {
  Clusters map[string]*utilCluster
}

type utilCluster struct {
  Nodes map[string]*utilNode
}

type utilNode struct {
  Meters map[string]*Meter
}

func NewUtility() *Utility {
  return &Utility{make(map[string]*utilCluster)}
}

func (u *Utility) SizeClusters() int {
  return len(u.Clusters)
}

func (u *Utility) NewMeter(cluster string, node string, cf string, op string) (*Meter, error) {
  c, ok := u.Clusters[cluster]
  if !ok {
    c = &utilCluster{make(map[string]*utilNode)}
    u.Clusters[cluster] = c
  }
  n, ok := c.Nodes[node]
  if !ok {
    n = &utilNode{make(map[string]*Meter)}
    c.Nodes[node] = n
  }
  metername := fmt.Sprintf("%s:%s:%s:%s", cluster, node, cf, op)
  if _, ok := n.Meters[metername]; ok {
    return nil, fmt.Errorf("Meter already exists")
  }
  m := &Meter{metername, make(map[int64]Sample)}
  n.Meters[metername] = m
  return m, nil
}

func (u *Utility) SizeNodes() int {
  total := 0
  for _, c := range u.Clusters {
    total += len(c.Nodes)
  }
  return total
}

func (u *Utility) SizeMeters() int {
  total := 0
  for _, c := range u.Clusters {
    for _, n := range c.Nodes {
      total += len(n.Meters)
    }
  }
  return total
}

func (u *Utility) ClusterNames() ([]string) {
  ret := make([]string, 0)
  for cname, _ := range u.Clusters {
    ret = append(ret, cname)
  }
  return ret
}

func (u *Utility) NodeNames() ([]string) {
  ret := make([]string, 0)
  for _, c := range u.Clusters {
    for nname, _ := range c.Nodes {
      ret = append(ret, nname)
    }
  }
  return ret
}

func (u *Utility) MeterNames() ([]string) {
  ret := make([]string, 0)
  for _, c := range u.Clusters {
    for _, n := range c.Nodes {
      for mname, _ := range n.Meters {
        ret = append(ret, mname)
      }
    }
  }
  return ret
}

func (u *Utility) GetMeter(clustername string, nodename string, cf string, op string) (*Meter, error) {
  c, ok := u.Clusters[clustername]
  if !ok {
    return nil, fmt.Errorf("Unable to find cluster %s", clustername)
  }
  n, ok := c.Nodes[nodename]
  if !ok {
    return nil, fmt.Errorf("Unable to find node %s:%s", clustername, nodename)
  }
  mname := fmt.Sprintf("%s:%s:%s:%s", clustername, nodename, cf, op)
  m, ok := n.Meters[mname]
  if !ok {
    return nil, fmt.Errorf("Unable to find meter %s", mname)
  }
  return m, nil
}
