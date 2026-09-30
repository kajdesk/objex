// Package checksum implements the additional S3 checksums (x-amz-checksum-*).
// Values are base64 of the big-endian digest.
package checksum

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"strconv"
	"strings"
)

// Algo is an S3 checksum algorithm.
type Algo string

const (
	CRC32     Algo = "CRC32"
	CRC32C    Algo = "CRC32C"
	CRC64NVME Algo = "CRC64NVME"
	SHA1      Algo = "SHA1"
	SHA256    Algo = "SHA256"
)

// All lists every supported algorithm.
var All = []Algo{CRC32, CRC32C, CRC64NVME, SHA1, SHA256}

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	nvme       = crc64.MakeTable(0x9A6C9329AC4BC9B5)
)

// Castagnoli is the CRC32C table, shared with the storage engine's internal checksum.
func Castagnoli() *crc32.Table { return castagnoli }

// Parse parses an algorithm name, case-insensitively.
func Parse(s string) (Algo, bool) {
	a := Algo(strings.ToUpper(strings.TrimSpace(s)))
	for _, known := range All {
		if a == known {
			return a, true
		}
	}
	return "", false
}

// Header is the request/response header carrying the value, e.g. x-amz-checksum-crc32.
func (a Algo) Header() string { return "x-amz-checksum-" + strings.ToLower(string(a)) }

// XMLTag is the element name used in XML bodies, e.g. ChecksumCRC32.
func (a Algo) XMLTag() string { return "Checksum" + string(a) }

// FromHeader maps a header name to its algorithm.
func FromHeader(h string) (Algo, bool) {
	name, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(h)), "x-amz-checksum-")
	if !ok {
		return "", false
	}
	switch name {
	case "algorithm", "type", "mode":
		return "", false
	}
	return Parse(name)
}

// IsCRC reports whether the algorithm is a CRC (and so supports full-object
// multipart checksums).
func (a Algo) IsCRC() bool { return a == CRC32 || a == CRC32C || a == CRC64NVME }

// DigestSize is the raw digest length in bytes.
func (a Algo) DigestSize() int {
	switch a {
	case CRC32, CRC32C:
		return 4
	case CRC64NVME:
		return 8
	case SHA1:
		return 20
	default:
		return 32
	}
}

// Type is how a multipart object's checksum is formed.
type Type string

const (
	// Composite is a checksum of the part checksums, suffixed "-N".
	Composite Type = "COMPOSITE"
	// FullObject is a checksum of the whole object's bytes.
	FullObject Type = "FULL_OBJECT"
)

// ParseType parses a checksum type name.
func ParseType(s string) (Type, bool) {
	switch t := Type(strings.ToUpper(strings.TrimSpace(s))); t {
	case Composite, FullObject:
		return t, true
	}
	return "", false
}

// DefaultType is the multipart checksum type used when the client does not choose one.
func (a Algo) DefaultType() Type {
	if a == CRC64NVME {
		return FullObject
	}
	return Composite
}

// Checksum is a computed or expected checksum value.
type Checksum struct {
	Algo  Algo   `json:"algo"`
	Value string `json:"value"`
}

// Type reports whether a stored value is composite ("-N" suffix) or full-object.
func (c Checksum) Type() Type {
	if strings.Contains(c.Value, "-") {
		return Composite
	}
	return FullObject
}

func (c Checksum) raw() ([]byte, bool) {
	d, err := base64.StdEncoding.DecodeString(c.Value)
	return d, err == nil && len(d) == c.Algo.DigestSize()
}

// Valid reports whether a client-supplied value is well formed: base64 of a
// digest of the right length, optionally followed by a "-N" part count.
func (c Checksum) Valid() bool {
	digest, suffix, hasSuffix := strings.Cut(c.Value, "-")
	if _, ok := (Checksum{Algo: c.Algo, Value: digest}).raw(); !ok {
		return false
	}
	if !hasSuffix {
		return true
	}
	n, err := strconv.Atoi(suffix)
	return err == nil && n > 0 && suffix[0] != '0'
}

// Hasher computes one algorithm incrementally.
type Hasher struct {
	algo Algo
	h    hash.Hash
}

// New returns a hasher for algo.
func New(algo Algo) *Hasher {
	var h hash.Hash
	switch algo {
	case CRC32:
		h = crc32.NewIEEE()
	case CRC32C:
		h = crc32.New(castagnoli)
	case CRC64NVME:
		h = crc64.New(nvme)
	case SHA1:
		h = sha1.New()
	case SHA256:
		h = sha256.New()
	default:
		panic(fmt.Sprintf("checksum: unknown algorithm %q", algo))
	}
	return &Hasher{algo: algo, h: h}
}

func (h *Hasher) Write(p []byte) (int, error) { return h.h.Write(p) }

// Algo returns the hasher's algorithm.
func (h *Hasher) Algo() Algo { return h.algo }

// Sum returns the checksum of everything written.
func (h *Hasher) Sum() Checksum {
	return Checksum{Algo: h.algo, Value: base64.StdEncoding.EncodeToString(h.h.Sum(nil))}
}

// FromCRC32C builds a CRC32C checksum from a raw value, so the engine's internal
// CRC can double as the S3 checksum without hashing twice.
func FromCRC32C(v uint32) Checksum {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return Checksum{Algo: CRC32C, Value: base64.StdEncoding.EncodeToString(b[:])}
}

// Composite is the checksum of the concatenated raw part digests, suffixed "-N".
func MakeComposite(algo Algo, parts []Checksum) (Checksum, bool) {
	h := New(algo)
	for _, p := range parts {
		raw, ok := p.raw()
		if p.Algo != algo || !ok {
			return Checksum{}, false
		}
		_, _ = h.Write(raw)
	}
	c := h.Sum()
	c.Value = fmt.Sprintf("%s-%d", c.Value, len(parts))
	return c, true
}

// Part is a part's checksum and length, for full-object combination.
type Part struct {
	Checksum Checksum
	Size     int64
}

// CombineFull computes the full-object CRC of a multipart object from its part
// CRCs and sizes, without reading any data.
func CombineFull(algo Algo, parts []Part) (Checksum, bool) {
	poly, bits := crcParams(algo)
	if bits == 0 || len(parts) == 0 {
		return Checksum{}, false
	}
	var acc uint64
	for i, p := range parts {
		raw, ok := p.Checksum.raw()
		if p.Checksum.Algo != algo || !ok {
			return Checksum{}, false
		}
		var v uint64
		for _, b := range raw {
			v = v<<8 | uint64(b)
		}
		if i == 0 {
			acc = v
		} else {
			acc = crcCombine(poly, bits, acc, v, uint64(p.Size))
		}
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], acc)
	return Checksum{Algo: algo, Value: base64.StdEncoding.EncodeToString(b[8-bits/8:])}, true
}

// crcParams returns the reflected polynomial and width for CRC algorithms.
func crcParams(a Algo) (uint64, uint) {
	switch a {
	case CRC32:
		return 0xEDB88320, 32
	case CRC32C:
		return 0x82F63B78, 32
	case CRC64NVME:
		return 0x9A6C9329AC4BC9B5, 64
	}
	return 0, 0
}

// zlib-style CRC combination over GF(2), valid for reflected CRCs with all-ones
// init and final xor (CRC32, CRC32C and CRC64/NVME all are).
func gf2Times(mat *[64]uint64, vec uint64) uint64 {
	var sum uint64
	for i := 0; vec != 0; i, vec = i+1, vec>>1 {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
	}
	return sum
}

func gf2Square(sq, mat *[64]uint64, bits uint) {
	for n := uint(0); n < bits; n++ {
		sq[n] = gf2Times(mat, mat[n])
	}
}

func crcCombine(poly uint64, bits uint, crc1, crc2, len2 uint64) uint64 {
	if len2 == 0 {
		return crc1
	}
	var even, odd [64]uint64
	odd[0] = poly // operator for one zero bit
	row := uint64(1)
	for n := uint(1); n < bits; n++ {
		odd[n] = row
		row <<= 1
	}
	gf2Square(&even, &odd, bits) // two zero bits
	gf2Square(&odd, &even, bits) // four zero bits
	for {
		gf2Square(&even, &odd, bits)
		if len2&1 != 0 {
			crc1 = gf2Times(&even, crc1)
		}
		if len2 >>= 1; len2 == 0 {
			break
		}
		gf2Square(&odd, &even, bits)
		if len2&1 != 0 {
			crc1 = gf2Times(&odd, crc1)
		}
		if len2 >>= 1; len2 == 0 {
			break
		}
	}
	return crc1 ^ crc2
}
