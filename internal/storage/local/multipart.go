package local

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
	bolt "go.etcd.io/bbolt"
)

var errPartNumber = s3err.InvalidArgument.WithMessage("Part number must be an integer between 1 and 10000, inclusive")

func (s *Store) CreateMultipart(_ context.Context, bucket, key string, meta storage.Metadata, algo checksum.Algo, typ checksum.Type) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	if algo != "" {
		if typ == "" {
			typ = algo.DefaultType()
		}
		if typ == checksum.FullObject && !algo.IsCRC() {
			return "", s3err.InvalidRequest.WithMessagef("The FULL_OBJECT checksum type is not supported for %s", algo)
		}
		if typ == checksum.Composite && algo == checksum.CRC64NVME {
			return "", s3err.InvalidRequest.WithMessage("The COMPOSITE checksum type is not supported for CRC64NVME")
		}
	} else if typ != "" {
		return "", s3err.InvalidRequest.WithMessage("The x-amz-checksum-type header requires x-amz-checksum-algorithm")
	}
	id := randomID()
	rec := enc(uploadRecord{Initiated: now(), Meta: meta, Algo: algo, Type: typ})
	return id, s.update(func(tx *bolt.Tx) error {
		if err := requireBucket(tx, bucket); err != nil {
			return err
		}
		return tx.Bucket(uploadsTable).Put(uploadKey(bucket, key, id), rec)
	})
}

func getUpload(tx *bolt.Tx, bucket, key, id string) (uploadRecord, error) {
	if err := requireBucket(tx, bucket); err != nil {
		return uploadRecord{}, err
	}
	v := tx.Bucket(uploadsTable).Get(uploadKey(bucket, key, id))
	if v == nil {
		return uploadRecord{}, s3err.NoSuchUpload
	}
	return dec[uploadRecord](v)
}

func (s *Store) readUpload(bucket, key, id string) (uploadRecord, error) {
	var u uploadRecord
	err := s.view(func(tx *bolt.Tx) error {
		var err error
		u, err = getUpload(tx, bucket, key, id)
		return err
	})
	return u, err
}

// commitPart stores a part, replacing any earlier upload of the same number.
func (s *Store) commitPart(bucket, key, id string, n int, rec *partRecord) (storage.Part, error) {
	var garbage []segment
	err := s.update(func(tx *bolt.Tx) error {
		garbage = nil
		if _, err := getUpload(tx, bucket, key, id); err != nil {
			return err
		}
		t := tx.Bucket(partsTable)
		pk := partKey(id, n)
		if v := t.Get(pk); v != nil {
			old, err := dec[partRecord](v)
			if err != nil {
				return err
			}
			garbage = []segment{old.Blob}
			if err := dropRefs(tx, garbage); err != nil {
				return err
			}
		}
		if err := addRefs(tx, []segment{rec.Blob}); err != nil {
			return err
		}
		return t.Put(pk, enc(rec))
	})
	if err != nil {
		_ = s.removeBlob(rec.Blob.Blob)
		return storage.Part{}, err
	}
	s.reclaim.queue(garbage)
	return rec.part(n), nil
}

func (s *Store) UploadPart(ctx context.Context, bucket, key, id string, n int, body io.Reader, e storage.Expect) (storage.Part, error) {
	if n < 1 || n > storage.MaxPartNumber {
		return storage.Part{}, errPartNumber
	}
	u, err := s.readUpload(bucket, key, id)
	if err != nil {
		return storage.Part{}, err
	}
	if u.Algo != "" {
		sent := e.Algo
		if e.Checksum != nil {
			sent = e.Checksum.Algo
		}
		if sent != "" && sent != u.Algo {
			return storage.Part{}, s3err.InvalidRequest.WithMessagef("Checksum Type mismatch occurred, expected checksum Type: %s, actual checksum Type: %s", strings.ToLower(string(u.Algo)), strings.ToLower(string(sent)))
		}
		e.Algo = u.Algo
	}
	d, err := s.writeBlob(ctx, body, e, storage.MaxPutSize)
	if err != nil {
		return storage.Part{}, err
	}
	return s.commitPart(bucket, key, id, n, &partRecord{ETag: hex.EncodeToString(d.md5), Size: d.seg.Size, Modified: now(), Checksum: d.checksum, Blob: d.seg})
}

func (s *Store) UploadPartCopy(ctx context.Context, srcBucket, srcKey string, rng *[2]int64, cond storage.ReadConditions, bucket, key, id string, n int) (storage.Part, error) {
	if n < 1 || n > storage.MaxPartNumber {
		return storage.Part{}, errPartNumber
	}
	u, err := s.readUpload(bucket, key, id)
	if err != nil {
		return storage.Part{}, err
	}
	src, reader, err := s.openRecord(srcBucket, srcKey)
	if err != nil {
		return storage.Part{}, err
	}
	defer reader.Close()
	if err := cond.Check(src.object(srcKey), true); err != nil {
		return storage.Part{}, err
	}
	start, length := int64(0), src.Size
	if rng != nil {
		if rng[0] > rng[1] || rng[1] >= src.Size {
			return storage.Part{}, s3err.InvalidArgument.WithMessagef("Range specified is not valid for source object of size: %d", src.Size)
		}
		start, length = rng[0], rng[1]-rng[0]+1
	}
	if length > storage.MaxPutSize {
		return storage.Part{}, s3err.EntityTooLarge
	}
	// A whole single-part source is linked rather than copied, when its stored
	// checksum already satisfies the upload.
	whole := start == 0 && length == src.Size && len(src.Segments) == 1 && !src.Multipart
	if whole && (u.Algo == "" || (src.Checksum != nil && src.Checksum.Algo == u.Algo)) {
		dup, err := s.duplicate(src.Segments[0])
		if err != nil {
			return storage.Part{}, err
		}
		var sum *checksum.Checksum
		if u.Algo != "" {
			sum = src.Checksum
		}
		return s.commitPart(bucket, key, id, n, &partRecord{ETag: src.ETag, Size: length, Modified: now(), Checksum: sum, Blob: dup})
	}
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(reader.CopyRange(pw, start, length)) }()
	d, err := s.writeBlob(ctx, pr, storage.Expect{Algo: u.Algo, Size: length}, storage.MaxPutSize)
	pr.Close()
	if err != nil {
		return storage.Part{}, err
	}
	return s.commitPart(bucket, key, id, n, &partRecord{ETag: hex.EncodeToString(d.md5), Size: d.seg.Size, Modified: now(), Checksum: d.checksum, Blob: d.seg})
}

// CompleteMultipart validates the part list and publishes the object in one
// write transaction, so the parts it uses cannot change underneath it.
func (s *Store) CompleteMultipart(_ context.Context, bucket, key, id string, parts []storage.CompletePart, o storage.CompleteOptions) (storage.Object, error) {
	if len(parts) == 0 {
		return storage.Object{}, s3err.MalformedXML.WithMessage("You must specify at least one part")
	}
	for i := 1; i < len(parts); i++ {
		if parts[i-1].Number >= parts[i].Number {
			return storage.Object{}, s3err.InvalidPartOrder
		}
	}
	if o.Checksum != nil && !o.Checksum.Valid() {
		return storage.Object{}, s3err.InvalidRequest.WithMessagef("Value for %s header is invalid.", o.Checksum.Algo.Header())
	}
	var rec *objectRecord
	var garbage []segment
	err := s.db.Update(func(tx *bolt.Tx) error {
		u, err := getUpload(tx, bucket, key, id)
		if err != nil {
			return err
		}
		t := tx.Bucket(partsTable)
		segs := make([]segment, 0, len(parts))
		md5s := make([]byte, 0, 16*len(parts))
		var sums []checksum.Part
		var size int64
		for i, want := range parts {
			v := t.Get(partKey(id, want.Number))
			if v == nil {
				return s3err.InvalidPart
			}
			have, err := dec[partRecord](v)
			if err != nil {
				return err
			}
			if strings.Trim(want.ETag, "\"") != have.ETag {
				return s3err.InvalidPart
			}
			if want.Checksum != nil && (have.Checksum == nil || *want.Checksum != *have.Checksum) {
				return s3err.InvalidPart.WithMessagef("The checksum for part %d did not match", want.Number)
			}
			if i+1 < len(parts) && have.Size < storage.MinPartSize {
				return s3err.EntityTooSmall
			}
			raw, err := hex.DecodeString(have.ETag)
			if err != nil {
				return s3err.Internal(err)
			}
			md5s = append(md5s, raw...)
			if have.Checksum != nil {
				sums = append(sums, checksum.Part{Checksum: *have.Checksum, Size: have.Size})
			}
			segs = append(segs, have.Blob)
			size += have.Size
		}
		if size > storage.MaxObjectSize {
			return s3err.EntityTooLarge
		}
		var sum *checksum.Checksum
		if u.Algo != "" && len(sums) == len(parts) {
			var c checksum.Checksum
			var ok bool
			if u.Type == checksum.FullObject {
				c, ok = checksum.CombineFull(u.Algo, sums)
			} else {
				cs := make([]checksum.Checksum, len(sums))
				for i := range sums {
					cs[i] = sums[i].Checksum
				}
				c, ok = checksum.MakeComposite(u.Algo, cs)
			}
			if ok {
				sum = &c
			}
		}
		// The client's value must match exactly: a composite checksum carries
		// its "-N" part count, a full-object checksum no suffix.
		if o.Checksum != nil && (sum == nil || *sum != *o.Checksum) {
			return s3err.BadDigest.WithMessagef("The %s you specified did not match the calculated checksum.", o.Checksum.Algo)
		}
		etag := md5.Sum(md5s)
		rec = &objectRecord{
			Size: size, ETag: fmt.Sprintf("%s-%d", hex.EncodeToString(etag[:]), len(parts)), Modified: now(),
			Meta: u.Meta, Checksum: sum, Multipart: true, Segments: segs,
		}
		old, err := getObject(tx, bucket, key)
		if err != nil {
			return err
		}
		var current *string
		if old != nil {
			current = &old.ETag
		}
		if err := o.Cond.Check(current); err != nil {
			return err
		}
		// Validation is done; now modify.
		all, err := removeParts(tx, id)
		if err != nil {
			return err
		}
		used := make(map[string]bool, len(segs))
		for _, sg := range segs {
			used[sg.Blob] = true
		}
		for _, sg := range all {
			if !used[sg.Blob] {
				garbage = append(garbage, sg)
			}
		}
		if old != nil {
			garbage = append(garbage, old.Segments...)
		}
		if err := dropRefs(tx, garbage); err != nil {
			return err
		}
		if err := tx.Bucket(uploadsTable).Delete(uploadKey(bucket, key, id)); err != nil {
			return err
		}
		return tx.Bucket(objectsTable).Put(objectKey(bucket, key), enc(rec))
	})
	if err != nil {
		return storage.Object{}, s3err.Internal(err)
	}
	s.reclaim.queue(garbage)
	return rec.object(key), nil
}

func (s *Store) AbortMultipart(_ context.Context, bucket, key, id string) error {
	var garbage []segment
	err := s.update(func(tx *bolt.Tx) error {
		garbage = nil
		if _, err := getUpload(tx, bucket, key, id); err != nil {
			return err
		}
		blobs, err := removeParts(tx, id)
		if err != nil {
			return err
		}
		if err := dropRefs(tx, blobs); err != nil {
			return err
		}
		garbage = blobs
		return tx.Bucket(uploadsTable).Delete(uploadKey(bucket, key, id))
	})
	if err == nil {
		s.reclaim.queue(garbage)
	}
	return err
}

func (s *Store) ListParts(_ context.Context, bucket, key, id string, marker, limit int) (storage.ListPartsResult, error) {
	var res storage.ListPartsResult
	err := s.view(func(tx *bolt.Tx) error {
		u, err := getUpload(tx, bucket, key, id)
		if err != nil {
			return err
		}
		res.Upload = storage.Upload{Key: key, UploadID: id, Initiated: time.Unix(0, u.Initiated).UTC(), ChecksumAlgo: u.Algo, ChecksumType: u.Type}
		prefix := []byte(id)
		c := tx.Bucket(partsTable).Cursor()
		for k, v := c.Seek(partKey(id, marker+1)); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if len(res.Parts) == limit {
				res.Truncated = true
				break
			}
			p, err := dec[partRecord](v)
			if err != nil {
				return err
			}
			n := int(uint32(k[len(k)-4])<<24 | uint32(k[len(k)-3])<<16 | uint32(k[len(k)-2])<<8 | uint32(k[len(k)-1]))
			res.Parts = append(res.Parts, p.part(n))
		}
		if len(res.Parts) > 0 {
			res.NextMarker = res.Parts[len(res.Parts)-1].Number
		}
		return nil
	})
	return res, err
}

func (s *Store) ListUploads(_ context.Context, bucket string, o storage.ListUploadsOptions) (storage.ListUploadsResult, error) {
	var res storage.ListUploadsResult
	err := s.view(func(tx *bolt.Tx) error {
		if err := requireBucket(tx, bucket); err != nil {
			return err
		}
		if o.Limit <= 0 {
			return nil
		}
		base := objectPrefix(bucket)
		c := tx.Bucket(uploadsTable).Cursor()
		var k, v []byte
		switch {
		case o.KeyMarker == "" || o.KeyMarker < o.Prefix:
			k, v = c.Seek(objectKey(bucket, o.Prefix))
		case o.UploadIDMarker == "":
			// Skip every upload of the marker key.
			k, v = c.Seek(successor(append(objectKey(bucket, o.KeyMarker), 0)))
		default:
			k, v = c.Seek(uploadKey(bucket, o.KeyMarker, o.UploadIDMarker))
			if k != nil && bytes.Equal(k, uploadKey(bucket, o.KeyMarker, o.UploadIDMarker)) {
				k, v = c.Next()
			}
		}
		count := 0
		for k != nil && bytes.HasPrefix(k, base) {
			rest := string(k[len(base):])
			sep := strings.LastIndexByte(rest, 0)
			key, uid := rest[:sep], rest[sep+1:]
			if !strings.HasPrefix(key, o.Prefix) {
				break
			}
			if cp, ok := commonPrefix(key, o.Prefix, o.Delimiter); ok {
				if o.KeyMarker == "" || cp > o.KeyMarker {
					if count == o.Limit {
						res.Truncated = true
						break
					}
					res.Prefixes = append(res.Prefixes, cp)
					res.NextKeyMarker, res.NextUploadIDMarker = cp, ""
					count++
				}
				next := successor(objectKey(bucket, cp))
				if next == nil {
					break
				}
				k, v = c.Seek(next)
				continue
			}
			if count == o.Limit {
				res.Truncated = true
				break
			}
			u, err := dec[uploadRecord](v)
			if err != nil {
				return err
			}
			res.Uploads = append(res.Uploads, storage.Upload{Key: key, UploadID: uid, Initiated: time.Unix(0, u.Initiated).UTC(), ChecksumAlgo: u.Algo, ChecksumType: u.Type})
			res.NextKeyMarker, res.NextUploadIDMarker = key, uid
			count++
			k, v = c.Next()
		}
		return nil
	})
	return res, err
}
