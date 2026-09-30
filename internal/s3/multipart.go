package s3

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/kajdesk/objex/internal/auth"
	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
)

func (q *request) uploadID() (string, error) {
	if id := q.q("uploadId"); id != "" {
		return id, nil
	}
	return "", s3err.NoSuchUpload
}

func (q *request) partNumber() (int, error) {
	n, err := strconv.Atoi(q.q("partNumber"))
	if err != nil || n < 1 || n > storage.MaxPartNumber {
		return 0, s3err.InvalidArgument.WithMessage("Part number must be an integer between 1 and 10000, inclusive")
	}
	return n, nil
}

func (h *Handler) createUpload(q *request) error {
	meta, err := metadataFromHeaders(q.r.Header)
	if err != nil {
		return err
	}
	var algo checksum.Algo
	if v := q.header("X-Amz-Checksum-Algorithm"); v != "" {
		a, found := checksum.Parse(v)
		if !found {
			return s3err.InvalidRequest.WithMessage("Invalid checksum algorithm")
		}
		algo = a
	}
	var typ checksum.Type
	if v := q.header("X-Amz-Checksum-Type"); v != "" {
		t, found := checksum.ParseType(v)
		if !found {
			return s3err.InvalidRequest.WithMessage("Invalid checksum type")
		}
		typ = t
	}
	id, err := h.store.CreateMultipart(q.r.Context(), q.bucket, q.key, meta, algo, typ)
	if err != nil {
		return err
	}
	if algo != "" {
		if typ == "" {
			typ = algo.DefaultType()
		}
		q.w.Header().Set("X-Amz-Checksum-Algorithm", string(algo))
		q.w.Header().Set("X-Amz-Checksum-Type", string(typ))
	}
	writeXML(q.w, http.StatusOK, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		XMLNS    string   `xml:"xmlns,attr"`
		Bucket   string
		Key      string
		UploadID string `xml:"UploadId"`
	}{XMLNS: xmlns, Bucket: q.bucket, Key: q.key, UploadID: id})
	return nil
}

func (h *Handler) uploadPart(q *request) error {
	id, err := q.uploadID()
	if err != nil {
		return err
	}
	n, err := q.partNumber()
	if err != nil {
		return err
	}
	body, chunked := q.body()
	e, err := q.expect(chunked)
	if err != nil {
		return err
	}
	p, err := h.store.UploadPart(q.r.Context(), q.bucket, q.key, id, n, body, e)
	if err != nil {
		return err
	}
	q.w.Header().Set("ETag", quote(p.ETag))
	if p.Checksum != nil {
		q.w.Header().Set(p.Checksum.Algo.Header(), p.Checksum.Value)
	}
	return ok(q.w, http.StatusOK)
}

func (h *Handler) uploadPartCopy(q *request) error {
	id, err := q.uploadID()
	if err != nil {
		return err
	}
	n, err := q.partNumber()
	if err != nil {
		return err
	}
	srcBucket, srcKey, err := parseCopySource(q.header("X-Amz-Copy-Source"))
	if err != nil {
		return err
	}
	var rng *[2]int64
	if v := q.header("X-Amz-Copy-Source-Range"); v != "" {
		first, last, suffix, found := parseRange(v)
		if !found || suffix || last < 0 {
			return s3err.InvalidArgument.WithMessage("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy")
		}
		rng = &[2]int64{first, last}
	}
	p, err := h.store.UploadPartCopy(q.r.Context(), srcBucket, srcKey, rng, readConditions(q.r.Header, "X-Amz-Copy-Source-"), q.bucket, q.key, id, n)
	if err != nil {
		return err
	}
	writeXML(q.w, http.StatusOK, copyResult("CopyPartResult", p.LastModified, p.ETag, p.Checksum))
	return nil
}

func (h *Handler) completeUpload(q *request) error {
	id, err := q.uploadID()
	if err != nil {
		return err
	}
	var in struct {
		Parts []struct {
			PartNumber        int
			ETag              string
			ChecksumCRC32     string
			ChecksumCRC32C    string
			ChecksumCRC64NVME string
			ChecksumSHA1      string
			ChecksumSHA256    string
		} `xml:"Part"`
	}
	if err := q.readXML(&in); err != nil {
		return err
	}
	parts := make([]storage.CompletePart, len(in.Parts))
	for i, p := range in.Parts {
		parts[i] = storage.CompletePart{Number: p.PartNumber, ETag: strings.Trim(strings.TrimSpace(p.ETag), `"`)}
		for a, v := range map[checksum.Algo]string{checksum.CRC32: p.ChecksumCRC32, checksum.CRC32C: p.ChecksumCRC32C, checksum.CRC64NVME: p.ChecksumCRC64NVME, checksum.SHA1: p.ChecksumSHA1, checksum.SHA256: p.ChecksumSHA256} {
			if v = strings.TrimSpace(v); v != "" {
				parts[i].Checksum = &checksum.Checksum{Algo: a, Value: v}
			}
		}
	}
	opts := storage.CompleteOptions{Cond: writeConditions(q.r.Header)}
	for _, a := range checksum.All {
		if v := q.header(a.Header()); v != "" {
			opts.Checksum = &checksum.Checksum{Algo: a, Value: strings.TrimSpace(v)}
		}
	}
	o, err := h.store.CompleteMultipart(q.r.Context(), q.bucket, q.key, id, parts, opts)
	if err != nil {
		return err
	}
	out := struct {
		XMLName      xml.Name `xml:"CompleteMultipartUploadResult"`
		XMLNS        string   `xml:"xmlns,attr"`
		Location     string
		Bucket       string
		Key          string
		ETag         string
		Checksums    []checksumXML
		ChecksumType string `xml:",omitempty"`
	}{XMLNS: xmlns, Location: "http://" + q.r.Host + "/" + q.bucket + "/" + auth.EncodePath(q.key),
		Bucket: q.bucket, Key: q.key, ETag: quote(o.ETag), Checksums: checksumElems(o.Checksum)}
	if o.Checksum != nil {
		out.ChecksumType = string(o.Checksum.Type())
	}
	writeXML(q.w, http.StatusOK, out)
	return nil
}

func (h *Handler) abortUpload(q *request) error {
	id, err := q.uploadID()
	if err != nil {
		return err
	}
	if err := h.store.AbortMultipart(q.r.Context(), q.bucket, q.key, id); err != nil {
		return err
	}
	return ok(q.w, http.StatusNoContent)
}

type partXML struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int64
	Checksums    []checksumXML
}

func (h *Handler) listParts(q *request) error {
	id, err := q.uploadID()
	if err != nil {
		return err
	}
	marker := 0
	if v := q.q("part-number-marker"); v != "" {
		if marker, err = strconv.Atoi(v); err != nil || marker < 0 {
			return s3err.InvalidArgument.WithMessage("Invalid part-number-marker")
		}
	}
	limit, err := parseMaxKeys(q.q("max-parts"), "max-parts")
	if err != nil {
		return err
	}
	res, err := h.store.ListParts(q.r.Context(), q.bucket, q.key, id, marker, limit)
	if err != nil {
		return err
	}
	out := struct {
		XMLName              xml.Name `xml:"ListPartsResult"`
		XMLNS                string   `xml:"xmlns,attr"`
		Bucket, Key          string
		UploadID             string `xml:"UploadId"`
		Initiator, Owner     owner
		StorageClass         string
		PartNumberMarker     int
		NextPartNumberMarker int
		MaxParts             int
		IsTruncated          bool
		ChecksumAlgorithm    string    `xml:",omitempty"`
		ChecksumType         string    `xml:",omitempty"`
		Parts                []partXML `xml:"Part"`
	}{XMLNS: xmlns, Bucket: q.bucket, Key: q.key, UploadID: id, Initiator: theOwner, Owner: theOwner, StorageClass: "STANDARD",
		PartNumberMarker: marker, NextPartNumberMarker: res.NextMarker, MaxParts: limit, IsTruncated: res.Truncated}
	if a := res.Upload.ChecksumAlgo; a != "" {
		t := res.Upload.ChecksumType
		if t == "" {
			t = a.DefaultType()
		}
		out.ChecksumAlgorithm, out.ChecksumType = string(a), string(t)
	}
	for _, p := range res.Parts {
		out.Parts = append(out.Parts, partXML{p.Number, iso8601(p.LastModified), quote(p.ETag), p.Size, checksumElems(p.Checksum)})
	}
	writeXML(q.w, http.StatusOK, out)
	return nil
}

func (h *Handler) listUploads(q *request) error {
	urlEncode := q.q("encoding-type") == "url"
	enc := func(s string) string {
		if urlEncode {
			return auth.EncodePath(s)
		}
		return s
	}
	limit, err := parseMaxKeys(q.q("max-uploads"), "max-uploads")
	if err != nil {
		return err
	}
	o := storage.ListUploadsOptions{Prefix: q.q("prefix"), Delimiter: q.q("delimiter"), KeyMarker: q.q("key-marker"), UploadIDMarker: q.q("upload-id-marker"), Limit: limit}
	res, err := h.store.ListUploads(q.r.Context(), q.bucket, o)
	if err != nil {
		return err
	}
	type upload struct {
		Key               string
		UploadID          string `xml:"UploadId"`
		Initiator, Owner  owner
		StorageClass      string
		Initiated         string
		ChecksumAlgorithm string `xml:",omitempty"`
		ChecksumType      string `xml:",omitempty"`
	}
	type commonPrefix struct{ Prefix string }
	out := struct {
		XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
		XMLNS              string   `xml:"xmlns,attr"`
		Bucket             string
		KeyMarker          string
		UploadIDMarker     string `xml:"UploadIdMarker"`
		NextKeyMarker      string
		NextUploadIDMarker string `xml:"NextUploadIdMarker"`
		MaxUploads         int
		IsTruncated        bool
		Prefix             string
		Delimiter          string         `xml:",omitempty"`
		EncodingType       string         `xml:",omitempty"`
		Uploads            []upload       `xml:"Upload"`
		CommonPrefixes     []commonPrefix `xml:"CommonPrefixes"`
	}{XMLNS: xmlns, Bucket: q.bucket, KeyMarker: enc(o.KeyMarker), UploadIDMarker: o.UploadIDMarker, NextKeyMarker: enc(res.NextKeyMarker),
		NextUploadIDMarker: res.NextUploadIDMarker, MaxUploads: limit, IsTruncated: res.Truncated, Prefix: enc(o.Prefix), Delimiter: enc(o.Delimiter)}
	if urlEncode {
		out.EncodingType = "url"
	}
	for _, u := range res.Uploads {
		x := upload{Key: enc(u.Key), UploadID: u.UploadID, Initiator: theOwner, Owner: theOwner, StorageClass: "STANDARD", Initiated: iso8601(u.Initiated)}
		if u.ChecksumAlgo != "" {
			t := u.ChecksumType
			if t == "" {
				t = u.ChecksumAlgo.DefaultType()
			}
			x.ChecksumAlgorithm, x.ChecksumType = string(u.ChecksumAlgo), string(t)
		}
		out.Uploads = append(out.Uploads, x)
	}
	for _, p := range res.Prefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{enc(p)})
	}
	writeXML(q.w, http.StatusOK, out)
	return nil
}
