package checksum

import (
	"encoding/base64"
	"encoding/binary"
	"testing"
)

func sum(a Algo, data []byte) string {
	h := New(a)
	_, _ = h.Write(data[:3])
	_, _ = h.Write(data[3:])
	return h.Sum().Value
}

func be32(v uint32) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return base64.StdEncoding.EncodeToString(b[:])
}

func TestCheckValues(t *testing.T) {
	data := []byte("123456789")
	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], 0xAE8B14860A799888)
	cases := map[Algo]string{
		CRC32:     be32(0xCBF43926),
		CRC32C:    be32(0xE3069283),
		CRC64NVME: base64.StdEncoding.EncodeToString(b8[:]),
	}
	for a, want := range cases {
		if got := sum(a, data); got != want {
			t.Errorf("%s: got %s want %s", a, got, want)
		}
	}
	if got := sum(SHA1, []byte("abc")); got != "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=" {
		t.Errorf("SHA1: %s", got)
	}
	if FromCRC32C(0xE3069283).Value != be32(0xE3069283) {
		t.Error("FromCRC32C")
	}
}

func TestCombineFull(t *testing.T) {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i * 31 % 251)
	}
	for _, a := range []Algo{CRC32, CRC32C, CRC64NVME} {
		var parts []Part
		for _, p := range [][]byte{data[:3000], data[3000:3001], data[3001:]} {
			h := New(a)
			_, _ = h.Write(p)
			parts = append(parts, Part{Checksum: h.Sum(), Size: int64(len(p))})
		}
		got, ok := CombineFull(a, parts)
		if !ok || got.Value != sum(a, data) {
			t.Errorf("%s: combined %v, want %s", a, got, sum(a, data))
		}
	}
}

func TestValidAndHeaders(t *testing.T) {
	good := New(CRC32).Sum()
	for value, want := range map[string]bool{
		good.Value: true, good.Value + "-3": true, good.Value + "-0": false,
		good.Value + "-x": false, good.Value + "-": false, "AAAA": false, "not base64!": false,
	} {
		if got := (Checksum{Algo: CRC32, Value: value}).Valid(); got != want {
			t.Errorf("Valid(%q) = %v", value, got)
		}
	}
	if a, ok := FromHeader("X-Amz-Checksum-Crc64nvme"); !ok || a != CRC64NVME {
		t.Error("FromHeader")
	}
	if _, ok := FromHeader("x-amz-checksum-algorithm"); ok {
		t.Error("x-amz-checksum-algorithm is not a value header")
	}
	c, ok := MakeComposite(CRC32, []Checksum{good, good})
	if !ok || c.Type() != Composite || c.Value[len(c.Value)-2:] != "-2" {
		t.Errorf("composite: %v", c)
	}
}
