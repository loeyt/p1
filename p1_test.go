package p1

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/oklog/ulid"
)

func TestNewAndMerge(t *testing.T) {
	t1 := New([4]byte{}, []byte("Hello, "))
	t1.Duration = 7

	time.Sleep(10 * time.Millisecond)

	t2 := New([4]byte{}, []byte("world!"))
	t2.Duration = 5

	tm := t2.Merge(t1)

	if tm.ULID.Compare(t1.ULID) != 0 {
		t.Fatalf("wrong tm.ULID, expected %q, got %q", t1.ULID, tm.ULID)
	}
	if tm.Duration < 15 {
		t.Fatalf("wrong tm.Duration, expected > 15ms, got %d", tm.Duration)
	}
	if string(tm.Data) != "Hello, world!" {
		t.Fatalf("wrong tm.Data, expected \"Hello, world!\", got %q", string(tm.Data))
	}
}

func TestMarshalAndRead(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, transmission := range testDataStream {
		p, err := transmission.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		_, err = buf.Write(p)
		if err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < len(testDataStream)+1; n++ {
		transmission, err := ReadTransmission(buf)
		if err == io.EOF {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if transmission.ULID.Compare(testDataStream[n].ULID) != 0 {
			t.Fatalf("expected ULID %q, got %q",
				testDataStream[n].ULID, transmission.ULID)
		}
		if transmission.Duration != testDataStream[n].Duration {
			t.Fatalf("expected Duration %d, got %d",
				testDataStream[n].Duration, transmission.Duration)
		}
		if !bytes.Equal(transmission.Data, testDataStream[n].Data) {
			t.Fatalf("expected Data %q, got %q",
				testDataStream[n].Data, transmission.Data)
		}
	}
}

func TestSplitAndMerge(t *testing.T) {
	ts := testDataStream
	var out []*Transmission
	for {
		n := Split(ts)
		if n <= 0 {
			break
		}
		out = append(out, ts[0].Merge(ts[1:n]...))
		ts = ts[n:]
	}
	if len(out) != 4 {
		t.Fatalf("expected 4 items, got %d", len(out))
	}
	for _, transmission := range out {
		if transmission.Duration != 310 {
			t.Fatalf("expected Duration to be 310, got %d", transmission.Duration)
		}
		if len(transmission.Data) != 806 {
			t.Fatalf("expected Data to be 806 bytes long, got %d bytes", transmission.Data)
		}
	}
}

var testDataStream = []*Transmission{
	{ULID: ulid.MustNew(10, nil), Data: []byte("/KFM5KAIFA-METER\r\n\r\n")},
	{ULID: ulid.MustNew(20, nil), Data: []byte("1-3:0.2.8(42)\r\n")},
	{ULID: ulid.MustNew(30, nil), Data: []byte("0-0:1.0.0(160321104324W)\r\n")},
	{ULID: ulid.MustNew(40, nil), Data: []byte("0-0:96.1.1(5f5f52454441435445445f5f)\r\n")},
	{ULID: ulid.MustNew(50, nil), Data: []byte("1-0:1.8.1(000843.085*kWh)\r\n")},
	{ULID: ulid.MustNew(60, nil), Data: []byte("1-0:1.8.2(001201.241*kWh)\r\n")},
	{ULID: ulid.MustNew(70, nil), Data: []byte("1-0:2.8.1(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(80, nil), Data: []byte("1-0:2.8.2(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(90, nil), Data: []byte("0-0:96.14.0(0002)\r\n")},
	{ULID: ulid.MustNew(100, nil), Data: []byte("1-0:1.7.0(00.347*kW)\r\n")},
	{ULID: ulid.MustNew(110, nil), Data: []byte("1-0:2.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(120, nil), Data: []byte("0-0:96.7.21(00010)\r\n")},
	{ULID: ulid.MustNew(130, nil), Data: []byte("0-0:96.7.9(00007)\r\n")},
	{ULID: ulid.MustNew(140, nil), Data: []byte("1-0:99.97.0(4)(0-0:96.7.19)(000105033123W)(0000358148*s)(000101000001W)(2147483647*s)(000101000007W)(2147483647*s)(000101000001W)(2147483647*s)\r\n")},
	{ULID: ulid.MustNew(150, nil), Data: []byte("1-0:32.32.0(00001)\r\n")},
	{ULID: ulid.MustNew(160, nil), Data: []byte("1-0:52.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(170, nil), Data: []byte("1-0:72.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(180, nil), Data: []byte("1-0:32.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(190, nil), Data: []byte("1-0:52.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(200, nil), Data: []byte("1-0:72.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(210, nil), Data: []byte("0-0:96.13.1()\r\n")},
	{ULID: ulid.MustNew(220, nil), Data: []byte("0-0:96.13.0()\r\n")},
	{ULID: ulid.MustNew(230, nil), Data: []byte("1-0:31.7.0(001*A)\r\n")},
	{ULID: ulid.MustNew(240, nil), Data: []byte("1-0:51.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(250, nil), Data: []byte("1-0:71.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(260, nil), Data: []byte("1-0:21.7.0(00.338*kW)\r\n")},
	{ULID: ulid.MustNew(270, nil), Data: []byte("1-0:22.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(280, nil), Data: []byte("1-0:41.7.0(00.010*kW)\r\n")},
	{ULID: ulid.MustNew(290, nil), Data: []byte("1-0:42.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(300, nil), Data: []byte("1-0:61.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(310, nil), Data: []byte("1-0:62.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(320, nil), Data: []byte("!7508\r\n")},
	{ULID: ulid.MustNew(10010, nil), Data: []byte("/KFM5KAIFA-METER\r\n\r\n")},
	{ULID: ulid.MustNew(10020, nil), Data: []byte("1-3:0.2.8(42)\r\n")},
	{ULID: ulid.MustNew(10030, nil), Data: []byte("0-0:1.0.0(160321104324W)\r\n")},
	{ULID: ulid.MustNew(10040, nil), Data: []byte("0-0:96.1.1(5f5f52454441435445445f5f)\r\n")},
	{ULID: ulid.MustNew(10050, nil), Data: []byte("1-0:1.8.1(000843.085*kWh)\r\n")},
	{ULID: ulid.MustNew(10060, nil), Data: []byte("1-0:1.8.2(001201.241*kWh)\r\n")},
	{ULID: ulid.MustNew(10070, nil), Data: []byte("1-0:2.8.1(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(10080, nil), Data: []byte("1-0:2.8.2(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(10090, nil), Data: []byte("0-0:96.14.0(0002)\r\n")},
	{ULID: ulid.MustNew(10100, nil), Data: []byte("1-0:1.7.0(00.347*kW)\r\n")},
	{ULID: ulid.MustNew(10110, nil), Data: []byte("1-0:2.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(10120, nil), Data: []byte("0-0:96.7.21(00010)\r\n")},
	{ULID: ulid.MustNew(10130, nil), Data: []byte("0-0:96.7.9(00007)\r\n")},
	{ULID: ulid.MustNew(10140, nil), Data: []byte("1-0:99.97.0(4)(0-0:96.7.19)(000105033123W)(0000358148*s)(000101000001W)(2147483647*s)(000101000007W)(2147483647*s)(000101000001W)(2147483647*s)\r\n")},
	{ULID: ulid.MustNew(10150, nil), Data: []byte("1-0:32.32.0(00001)\r\n")},
	{ULID: ulid.MustNew(10160, nil), Data: []byte("1-0:52.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(10170, nil), Data: []byte("1-0:72.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(10180, nil), Data: []byte("1-0:32.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(10190, nil), Data: []byte("1-0:52.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(10200, nil), Data: []byte("1-0:72.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(10210, nil), Data: []byte("0-0:96.13.1()\r\n")},
	{ULID: ulid.MustNew(10220, nil), Data: []byte("0-0:96.13.0()\r\n")},
	{ULID: ulid.MustNew(10230, nil), Data: []byte("1-0:31.7.0(001*A)\r\n")},
	{ULID: ulid.MustNew(10240, nil), Data: []byte("1-0:51.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(10250, nil), Data: []byte("1-0:71.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(10260, nil), Data: []byte("1-0:21.7.0(00.338*kW)\r\n")},
	{ULID: ulid.MustNew(10270, nil), Data: []byte("1-0:22.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(10280, nil), Data: []byte("1-0:41.7.0(00.010*kW)\r\n")},
	{ULID: ulid.MustNew(10290, nil), Data: []byte("1-0:42.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(10300, nil), Data: []byte("1-0:61.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(10310, nil), Data: []byte("1-0:62.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(10320, nil), Data: []byte("!7508\r\n")},
	{ULID: ulid.MustNew(20010, nil), Data: []byte("/KFM5KAIFA-METER\r\n\r\n")},
	{ULID: ulid.MustNew(20020, nil), Data: []byte("1-3:0.2.8(42)\r\n")},
	{ULID: ulid.MustNew(20030, nil), Data: []byte("0-0:1.0.0(160321104334W)\r\n")},
	{ULID: ulid.MustNew(20040, nil), Data: []byte("0-0:96.1.1(5f5f52454441435445445f5f)\r\n")},
	{ULID: ulid.MustNew(20050, nil), Data: []byte("1-0:1.8.1(000843.085*kWh)\r\n")},
	{ULID: ulid.MustNew(20060, nil), Data: []byte("1-0:1.8.2(001201.242*kWh)\r\n")},
	{ULID: ulid.MustNew(20070, nil), Data: []byte("1-0:2.8.1(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(20080, nil), Data: []byte("1-0:2.8.2(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(20090, nil), Data: []byte("0-0:96.14.0(0002)\r\n")},
	{ULID: ulid.MustNew(20100, nil), Data: []byte("1-0:1.7.0(00.348*kW)\r\n")},
	{ULID: ulid.MustNew(20110, nil), Data: []byte("1-0:2.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(20120, nil), Data: []byte("0-0:96.7.21(00010)\r\n")},
	{ULID: ulid.MustNew(20130, nil), Data: []byte("0-0:96.7.9(00007)\r\n")},
	{ULID: ulid.MustNew(20140, nil), Data: []byte("1-0:99.97.0(4)(0-0:96.7.19)(000105033123W)(0000358148*s)(000101000001W)(2147483647*s)(000101000007W)(2147483647*s)(000101000001W)(2147483647*s)\r\n")},
	{ULID: ulid.MustNew(20150, nil), Data: []byte("1-0:32.32.0(00001)\r\n")},
	{ULID: ulid.MustNew(20160, nil), Data: []byte("1-0:52.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(20170, nil), Data: []byte("1-0:72.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(20180, nil), Data: []byte("1-0:32.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(20190, nil), Data: []byte("1-0:52.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(20200, nil), Data: []byte("1-0:72.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(20210, nil), Data: []byte("0-0:96.13.1()\r\n")},
	{ULID: ulid.MustNew(20220, nil), Data: []byte("0-0:96.13.0()\r\n")},
	{ULID: ulid.MustNew(20230, nil), Data: []byte("1-0:31.7.0(001*A)\r\n")},
	{ULID: ulid.MustNew(20240, nil), Data: []byte("1-0:51.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(20250, nil), Data: []byte("1-0:71.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(20260, nil), Data: []byte("1-0:21.7.0(00.337*kW)\r\n")},
	{ULID: ulid.MustNew(20270, nil), Data: []byte("1-0:22.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(20280, nil), Data: []byte("1-0:41.7.0(00.010*kW)\r\n")},
	{ULID: ulid.MustNew(20290, nil), Data: []byte("1-0:42.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(20300, nil), Data: []byte("1-0:61.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(20310, nil), Data: []byte("1-0:62.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(20320, nil), Data: []byte("!5F7F\r\n")},
	{ULID: ulid.MustNew(30010, nil), Data: []byte("/KFM5KAIFA-METER\r\n\r\n")},
	{ULID: ulid.MustNew(30020, nil), Data: []byte("1-3:0.2.8(42)\r\n")},
	{ULID: ulid.MustNew(30030, nil), Data: []byte("0-0:1.0.0(160321104343W)\r\n")},
	{ULID: ulid.MustNew(30040, nil), Data: []byte("0-0:96.1.1(5f5f52454441435445445f5f)\r\n")},
	{ULID: ulid.MustNew(30050, nil), Data: []byte("1-0:1.8.1(000843.085*kWh)\r\n")},
	{ULID: ulid.MustNew(30060, nil), Data: []byte("1-0:1.8.2(001201.243*kWh)\r\n")},
	{ULID: ulid.MustNew(30070, nil), Data: []byte("1-0:2.8.1(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(30080, nil), Data: []byte("1-0:2.8.2(000000.000*kWh)\r\n")},
	{ULID: ulid.MustNew(30090, nil), Data: []byte("0-0:96.14.0(0002)\r\n")},
	{ULID: ulid.MustNew(30100, nil), Data: []byte("1-0:1.7.0(00.348*kW)\r\n")},
	{ULID: ulid.MustNew(30110, nil), Data: []byte("1-0:2.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(30120, nil), Data: []byte("0-0:96.7.21(00010)\r\n")},
	{ULID: ulid.MustNew(30130, nil), Data: []byte("0-0:96.7.9(00007)\r\n")},
	{ULID: ulid.MustNew(30140, nil), Data: []byte("1-0:99.97.0(4)(0-0:96.7.19)(000105033123W)(0000358148*s)(000101000001W)(2147483647*s)(000101000007W)(2147483647*s)(000101000001W)(2147483647*s)\r\n")},
	{ULID: ulid.MustNew(30150, nil), Data: []byte("1-0:32.32.0(00001)\r\n")},
	{ULID: ulid.MustNew(30160, nil), Data: []byte("1-0:52.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(30170, nil), Data: []byte("1-0:72.32.0(00000)\r\n")},
	{ULID: ulid.MustNew(30180, nil), Data: []byte("1-0:32.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(30190, nil), Data: []byte("1-0:52.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(30200, nil), Data: []byte("1-0:72.36.0(00000)\r\n")},
	{ULID: ulid.MustNew(30210, nil), Data: []byte("0-0:96.13.1()\r\n")},
	{ULID: ulid.MustNew(30220, nil), Data: []byte("0-0:96.13.0()\r\n")},
	{ULID: ulid.MustNew(30230, nil), Data: []byte("1-0:31.7.0(001*A)\r\n")},
	{ULID: ulid.MustNew(30240, nil), Data: []byte("1-0:51.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(30250, nil), Data: []byte("1-0:71.7.0(000*A)\r\n")},
	{ULID: ulid.MustNew(30260, nil), Data: []byte("1-0:21.7.0(00.338*kW)\r\n")},
	{ULID: ulid.MustNew(30270, nil), Data: []byte("1-0:22.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(30280, nil), Data: []byte("1-0:41.7.0(00.010*kW)\r\n")},
	{ULID: ulid.MustNew(30290, nil), Data: []byte("1-0:42.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(30300, nil), Data: []byte("1-0:61.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(30310, nil), Data: []byte("1-0:62.7.0(00.000*kW)\r\n")},
	{ULID: ulid.MustNew(30320, nil), Data: []byte("!42E4\r\n")},
}
