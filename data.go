package frank

import (
	"sort"
	_ "fmt"
)

type int64Slice []int64
func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Sample struct {
	TimestampMS int64
	Data []float64
}

type NamedSample struct {
	Sample
	Name string
}

type Meter struct {
	Name string
	Data map[int64]Sample
}

func (m *Meter) Cleanup(length int) {
	if len(m.Data) > length {
		keys := make(int64Slice, len(m.Data)+2)
		count := 0
		for k, _ := range m.Data {
			keys[count] = k
			count = count + 1
		}
		keys = keys[:count]
		sort.Sort(keys)
		for i := 0; i<len(keys)-length; i++ {
			delete(m.Data, keys[i])
		}
	}
}

func (m *Meter) Raw() ([]Sample, error) {
	dts := make(int64Slice, len(m.Data))
	count := 0
	for ts, _ := range m.Data {
		dts[count] = ts
		count = count + 1
	}
	sort.Sort(dts)
	ret := make([]Sample, len(dts))
	for x, ts := range dts {
		ret[x] = m.Data[ts]
	}
	return ret, nil
}

func Align(src []Sample, interval int64, starttime int64, endtime int64) []Sample {
	bins := int((endtime-starttime)/interval + 1)
	ret := make([]Sample, bins)
	for x := 0; x < bins; x++ {
		ret[x].TimestampMS = starttime + interval * int64(x)
		ret[x].Data = make([]float64, 91)
	}
	curposition := 0
	curinterval := starttime
	cursrcposition := 0
	//fmt.Println(len(src))
	for curinterval <= endtime && cursrcposition < len(src)-1 {
		//fmt.Println(curinterval-starttime, src[cursrcposition].TimestampMS-starttime, src[cursrcposition+1].TimestampMS-starttime)
		switch {
		case curinterval == src[cursrcposition].TimestampMS:
			for x := 0; x < 91; x++ {
				ret[curposition].Data[x] = src[cursrcposition].Data[x]
			}
			curposition++
			curinterval += interval
			cursrcposition++
		case curinterval > src[cursrcposition].TimestampMS && curinterval < src[cursrcposition+1].TimestampMS:
			var (
				leftts = float64(src[cursrcposition].TimestampMS)
				rightts = float64(src[cursrcposition+1].TimestampMS)
				curts = float64(curinterval)
				lWeight = (curts - leftts) / (rightts - leftts)
				rWeight = (rightts - curts) / (rightts - leftts)
			)
			for x := 0; x < 91; x++ {
				ret[curposition].Data[x] = lWeight * src[cursrcposition].Data[x] + rWeight * src[cursrcposition+1].Data[x]
			}
			curposition++
			curinterval += interval
			cursrcposition++
		case curinterval < src[cursrcposition].TimestampMS:
			curposition++
			curinterval += interval
		case curinterval > src[cursrcposition+1].TimestampMS:
			cursrcposition++
		default:
			cursrcposition++
		}
	}
	return ret
}

func Diff(src []Sample) []Sample {
	ret := make([]Sample, len(src)-1)
	for x := 0; x < len(ret); x++ {
		ret[x].Data = make([]float64, 91)
		ret[x].TimestampMS = src[x].TimestampMS
		for y := 0; y < 91; y++ {
			ret[x].Data[y] = src[x+1].Data[y] - src[x].Data[y]
		}
	}
	return ret
}

/*
func main() {
	tses := []int64{
		1396208317123,
		1396208322456,
		1396208325789,
		1396208329123,
		1396208331456,
		1396208334789,
		1396208338321,
		1396208340654,
		1396208342987,
		1396208344321,
		1396208346654,
		1396208348987,
		1396208350012,
		1396208352345,
		1396208354678,
	}
	data := make([]Sample, 15)
	for idx, ts := range tses {
		cur := make([]float64, 91)
		for x := 0; x < 91; x++ {
			switch x % 3 {
			case 0:
				cur[x] = float64(10*idx*idx + 1000)
			case 1:
				cur[x] = float64(300*idx-10*idx*idx + 1000)
			case 2:
				cur[x] = 0
			}
		}
		data[idx].TimestampMS = ts
		data[idx].Data = cur
	}
	fmt.Println(Diff(Align(data, 5000, 1396208320000, 1396208350000)))
}
*/
