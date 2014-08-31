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
  mname := "TestCluster:localhost:Test1:ReadHistory"
  m, err := u.NewMeter("TestCluster", "localhost", "Test1", "ReadHistory")
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
  if u.NodeNames()[0] != "localhost" {
    t.Errorf("Invalid Node Names : %s, should be [\"localhost\"]", u.NodeNames())
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
