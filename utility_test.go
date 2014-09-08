package frank

import "testing"

func TestNewUtility(t *testing.T) {
  u := NewUtility()
  if u.SizeClusters() != 0 {
    t.Errorf("Utility not initialized correctly: Clusters %d, should be 0", u.SizeClusters())
  }
  if u.SizeNodes() != 0 {
    t.Errorf("Utility not initialized correctly: Nodes %d, should be 0", u.SizeNodes())
  }
}

func TestUtilityNewMeter(t *testing.T) {
  u := NewUtility()
  mname := "TestCluster:localhost:Space1.Test1:ReadHistory"
  m, err := u.NewMeter("TestCluster", "localhost", "Space1.Test1", "ReadHistory")
  if err != nil {
    t.Errorf("NewMeter produced error: %s", err)
  }
  if u.SizeClusters() != 1 {
    t.Errorf("Invalid Cluster Size : %d, should be 0", u.SizeClusters())
  }
  if u.SizeNodes() != 1 {
    t.Errorf("Invlaid Node Size : %d, should be 0", u.SizeNodes())
  }
  if u.ClusterNames()[0] != "TestCluster" {
    t.Errorf("Invalid Cluster Names : %s, should be [\"TestCluster\"]", u.ClusterNames())
  }
  if u.NodeNames("TestCluster")[0] != "localhost" {
    t.Errorf("Invalid Node Names : %s, should be [\"localhost\"]", u.NodeNames("TestCluster"))
  }
  if u.CFNames("TestCluster")[0] != "Space1.Test1" {
    t.Errorf("Invalid CF Names : %s, should be [\"Space1.Test1\"]", u.CFNames("TestCluster"))
  }
  if u.MeterNames()[0] != mname {
    t.Errorf("Invalid Meter Names : %s, should be [\"%s\"]", u.MeterNames(), mname)
  }
  if m.Name != mname {
    t.Errorf("Invalid Meter Name : %s, should be %s", m.Name, mname)
  }
}

func TestUtilityGetMeter(t *testing.T) {
  u := NewUtility()
  m, _ := u.NewMeter("TestCluster", "localhost", "Test1", "ReadHistory")
  mneedle, err := u.GetMeter("TestCluster", "localhost", "Test1", "ReadHistory")
  if err != nil {
    t.Errorf("GetMeter produced error: %s", err)
  }
  if m != mneedle {
    t.Errorf("NewMeter and GetMeter differ : %p versus %p", m, mneedle)
  }
}

func TestUtilitySaveLoad(t * testing.T) {
  u1 := NewUtility()
  _, _ = u1.NewMeter("Test Cluster", "localhost", "system.Test1", "WriteLatency")
  s1 := Sample{1410000000000, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}}
  s2 := Sample{1420000000000, []float64{101.0, 102.0, 103.0, 104.0, 105.0, 106.0}}
  u1.AddSample("Test Cluster", "localhost", "system.Test1", "WriteLatency", s1)
  u1.AddSample("Test Cluster", "localhost", "system.Test1", "WriteLatency", s2)
  if err := u1.Save(); err != nil {
    t.Errorf("Save Error: %s", err)
    return
  }
  u2 := NewUtility()
  u2.Load()
  m, err := u2.GetMeter("Test Cluster", "localhost", "system.Test1", "WriteLatency")
  if err != nil {
    t.Errorf("Meter not found after Load")
    return
  }
  if val, ok := m.Data[1410000000000]; !ok {
    t.Errorf("Sample not found after Load")
    return
  } else {
    if val.Data[0] != 1.0 {
      t.Errorf("Sample not equal after Load")
    }
  }
}
