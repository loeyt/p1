package p1

import (
	"errors"
	"io"
	"math/rand"
	"sort"

	"github.com/oklog/ulid"
)

type randomSource [4]byte

func (r randomSource) Read(p []byte) (n int, err error) {
	if len(p) != 10 {
		return 0, ulid.ErrDataSize
	}
	copy(p, r[:])
	n, err = rand.Read(p[4:])
	n += 4
	return
}

// Transmission holds data identified by a ULID.
type Transmission struct {
	ULID     ulid.ULID
	Duration uint16
	Data     []byte
}

// New creates a new Transmission from raw data, marked with the given
// identifier.
func New(id [4]byte, data []byte) *Transmission {
	return &Transmission{
		ULID: ulid.MustNew(ulid.Now(), randomSource(id)),
		Data: data,
	}
}

// Merge takes two Transmissions and returns a new Transmission with the
// combined Data, properly calculated Duration and the ULID of the first
// Transmission.
func (t *Transmission) Merge(other ...*Transmission) *Transmission {
	ts := make([]*Transmission, 0, len(other)+1)
	ts, l := append(ts, t), len(t.Data)
	for n := range other {
		ts, l = append(ts, other[n]), l+len(ts[n].Data)
	}
	sort.Slice(ts, func(i, j int) bool {
		return ts[i].ULID.Compare(ts[j].ULID) < 0
	})
	data := make([]byte, 0, l)
	for n := range ts {
		data = append(data, ts[n].Data...)
	}
	d := ts[len(ts)-1].ULID.Time() - ts[0].ULID.Time()
	d += uint64(ts[len(ts)-1].Duration)
	if d > 0xFFFF {
		d = 0xFFFF
	}
	return &Transmission{
		ULID:     ts[0].ULID,
		Duration: uint16(d),
		Data:     data,
	}
}

// MarshalBinary marshals the Transmission in the following binary
// representation:
//
//  16 bytes - ULID
//   2 bytes - Duration (big-endian)
//   2 bytes - Data length (big-endian)
//   N bytes - Data
func (t *Transmission) MarshalBinary() ([]byte, error) {
	l := len(t.Data)
	if l > 0xFFFF {
		return nil, errors.New("transmission too big to marshal")
	}
	buf := make([]byte, l+20)
	err := t.ULID.MarshalBinaryTo(buf[:16])
	if err != nil {
		return nil, err
	}
	buf[16], buf[17] = byte(t.Duration>>8), byte(t.Duration)
	buf[18], buf[19] = byte(l>>8), byte(l)
	copy(buf[20:], t.Data)
	return buf, nil
}

// ReadTransmission reads a single Transmission from the given io.Reader, by
// reading the 20 bytes header, followed by reading the data as specified by
// the header.
func ReadTransmission(r io.Reader) (*Transmission, error) {
	header := make([]byte, 20)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, err
	}
	t := new(Transmission)
	t.ULID.UnmarshalBinary(header[:16])
	t.Duration = uint16(header[16])<<8 + uint16(header[17])
	t.Data = make([]byte, int(header[18])<<8+int(header[19]))
	_, err = io.ReadFull(r, t.Data)
	return t, err
}

// Split marks the point in the transmission slice where the start of the next
// P1 telegram probably is (ignoring the first transmission). If a '/' is
// found at the start of a transmission its index is returned. If the end of a
// transmission is '!' and a `\r\n` with 4 bytes in between the index + 1 is
// returned (possibly out of bounds in ts). Returns 0 if no such recognizable
// split point is found.
func Split(ts []*Transmission) int {
	for n := 1; n < len(ts); n++ {
		if len(ts[n].Data) > 0 && ts[n].Data[0] == '/' {
			return n
		}
		if len(ts[n].Data) >= 7 &&
			ts[n].Data[len(ts[n].Data)-7] == '!' &&
			ts[n].Data[len(ts[n].Data)-2] == '\r' &&
			ts[n].Data[len(ts[n].Data)-1] == '\n' {
			return n + 1
		}
	}
	return 0
}
