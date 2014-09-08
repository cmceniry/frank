package frank

import (
  "time"
  "fmt"
  "strings"
  "os"
  "encoding/gob"
)

type UtilityConfig struct {
  BackgroundSleep int
  BackgroundPause bool
  BackgroundRunning bool
  SampleThreshold int
  SaveFile string
}

type Utility struct {
  Config UtilityConfig
  Clusters map[string]*utilCluster
}

type utilCluster struct {
  Nodes map[string]*utilNode
}

type utilNode struct {
  Meters map[string]*Meter
}

func NewUtility() *Utility {
  u := &Utility{
    UtilityConfig{
      30,
      false,
      false,
      500,
      "/tmp/frank.sav",
    },
    make(map[string]*utilCluster),
  }
  return u
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

func (u *Utility) NodeNames(clustername string) ([]string) {
  ret := make([]string, 0)
  if c, ok := u.Clusters[clustername]; ok {
    for nname, _ := range c.Nodes {
      ret = append(ret, nname)
    }
  }
  return ret
}

func (u *Utility) CFNames(clustername string) ([]string) {
  ret := make([]string, 0)
  if c, ok := u.Clusters[clustername]; ok {
    for _, n := range c.Nodes {
      for mname, _ := range n.Meters {
        msplit := strings.Split(mname, ":")
        if len(msplit) == 4 {
          cf := msplit[2]
          found := false
          for _, v := range ret {
            if v == cf {
              found = true
            }
          }
          if !found {
            ret = append(ret, cf)
          }
        }
      }
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
  metername := fmt.Sprintf("%s:%s:%s:%s", clustername, nodename, cf, op)
  m, err := u.getMeter(clustername, nodename, metername)
  return m, err
}

func (u *Utility) getMeter(clustername string, nodename string, metername string) (*Meter, error) {
  c, ok := u.Clusters[clustername]
  if !ok {
    return nil, fmt.Errorf("Unable to find cluster %s", clustername)
  }
  n, ok := c.Nodes[nodename]
  if !ok {
    return nil, fmt.Errorf("Unable to find node %s:%s", clustername, nodename)
  }
  m, ok := n.Meters[metername]
  if !ok {
    return nil, fmt.Errorf("Unable to find meter %s", metername)
  }
  return m, nil
}

func (u *Utility) AddSample(clustername string, nodename string, cf string, op string, s Sample) (error) {
  m, err := u.GetMeter(clustername, nodename, cf, op)
  if err != nil {
    return err
  }
  m.Data[s.TimestampMS] = s
  return nil
}

func (u *Utility) CleanupSample(clustername string, nodename string, cf string, op string, length int) (error) {
  m, err := u.GetMeter(clustername, nodename, cf, op)
  if err != nil {
    return err
  }
  m.Cleanup(u.Config.SampleThreshold)
  return nil
}

func (u *Utility) backgroundCleanup() {
  for {
    if !u.Config.BackgroundPause && !u.Config.BackgroundRunning {
      u.Config.BackgroundRunning = true
      for _, c := range u.Clusters {
        for _, n := range c.Nodes {
          for _, m := range n.Meters {
            m.Cleanup(u.Config.SampleThreshold)
          }
        }
      }
      u.Config.BackgroundRunning = false
    }
    time.Sleep(time.Duration(u.Config.BackgroundSleep) * time.Second)
  }
}

func (u *Utility) StartBackgroundClean() {
  go u.backgroundCleanup()
}

func (u *Utility) DeleteMeter(clustername string, nodename string, cf string, op string) {

}

func (u *Utility) Load() (error) {
  fi, err := os.Open(u.Config.SaveFile)
  if err != nil {
    return err
  }
  defer fi.Close()
  dec := gob.NewDecoder(fi)
  var m Meter
  for {
    err := dec.Decode(&m)
    if err != nil {
      break
    }
    names := strings.Split(m.Name, ":")
    u.NewMeter(names[0], names[1], names[2], names[3])
    for _, s := range m.Data {
      u.AddSample(names[0], names[1], names[2], names[3], s)
    }
  }
  return nil
}

func (u *Utility) Save() (error) {
  fi, err := os.Create(u.Config.SaveFile)
  if err != nil {
    return err
  }
  defer fi.Close()
  enc := gob.NewEncoder(fi)
  for _, c := range u.Clusters {
    for _, n := range c.Nodes {
      for _, m := range n.Meters {
        enc.Encode(m)
      }
    }
  }
  return nil
}
