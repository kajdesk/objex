// Package s3err defines S3 API errors. They are used throughout the server,
// storage included, because every failure is ultimately reported to a client
// as one of these codes.
package s3err

import (
	"errors"
	"fmt"
	"net/http"
)

// Error is an S3 error response.
type Error struct {
	Code    string
	Status  int
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Is matches errors by code, so errors.Is(err, s3err.NoSuchKey) works for any
// message.
func (e *Error) Is(target error) bool {
	var t *Error
	return errors.As(target, &t) && t.Code == e.Code
}

// WithMessage returns a copy of e with a specific message.
func (e *Error) WithMessage(message string) *Error {
	return &Error{Code: e.Code, Status: e.Status, Message: message}
}

// WithMessagef is WithMessage with formatting.
func (e *Error) WithMessagef(format string, args ...any) *Error {
	return e.WithMessage(fmt.Sprintf(format, args...))
}

func def(code string, status int, message string) *Error {
	return &Error{Code: code, Status: status, Message: message}
}

var (
	AccessDenied                  = def("AccessDenied", http.StatusForbidden, "Access Denied")
	AuthorizationHeaderMalformed  = def("AuthorizationHeaderMalformed", http.StatusBadRequest, "The authorization header is malformed.")
	AuthorizationQueryParamsError = def("AuthorizationQueryParametersError", http.StatusBadRequest, "Error parsing the X-Amz-Credential parameter.")
	BadDigest                     = def("BadDigest", http.StatusBadRequest, "The Content-MD5 or checksum you specified did not match what we received.")
	BucketAlreadyOwnedByYou       = def("BucketAlreadyOwnedByYou", http.StatusConflict, "Your previous request to create the named bucket succeeded and you already own it.")
	BucketNotEmpty                = def("BucketNotEmpty", http.StatusConflict, "The bucket you tried to delete is not empty.")
	CORSForbidden                 = def("AccessForbidden", http.StatusForbidden, "CORSResponse: This CORS request is not allowed.")
	EntityTooLarge                = def("EntityTooLarge", http.StatusBadRequest, "Your proposed upload exceeds the maximum allowed object size.")
	EntityTooSmall                = def("EntityTooSmall", http.StatusBadRequest, "Your proposed upload is smaller than the minimum allowed object size.")
	IncompleteBody                = def("IncompleteBody", http.StatusBadRequest, "You did not provide the number of bytes specified by the Content-Length HTTP header.")
	InternalError                 = def("InternalError", http.StatusInternalServerError, "We encountered an internal error. Please try again.")
	InvalidAccessKeyID            = def("InvalidAccessKeyId", http.StatusForbidden, "The AWS access key ID you provided does not exist in our records.")
	InvalidArgument               = def("InvalidArgument", http.StatusBadRequest, "Invalid Argument")
	InvalidBucketName             = def("InvalidBucketName", http.StatusBadRequest, "The specified bucket is not valid.")
	InvalidDigest                 = def("InvalidDigest", http.StatusBadRequest, "The Content-MD5 you specified is not valid.")
	InvalidPart                   = def("InvalidPart", http.StatusBadRequest, "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.")
	InvalidPartNumber             = def("InvalidPartNumber", http.StatusRequestedRangeNotSatisfiable, "The requested partnumber is not satisfiable.")
	InvalidPartOrder              = def("InvalidPartOrder", http.StatusBadRequest, "The list of parts was not in ascending order. Parts must be ordered by part number.")
	InvalidRange                  = def("InvalidRange", http.StatusRequestedRangeNotSatisfiable, "The requested range is not satisfiable.")
	InvalidRequest                = def("InvalidRequest", http.StatusBadRequest, "Invalid Request")
	KeyTooLong                    = def("KeyTooLongError", http.StatusBadRequest, "Your key is too long.")
	MalformedXML                  = def("MalformedXML", http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema.")
	MetadataTooLarge              = def("MetadataTooLarge", http.StatusBadRequest, "Your metadata headers exceed the maximum allowed metadata size.")
	MethodNotAllowed              = def("MethodNotAllowed", http.StatusMethodNotAllowed, "The specified method is not allowed against this resource.")
	MissingContentLength          = def("MissingContentLength", http.StatusLengthRequired, "You must provide the Content-Length HTTP header.")
	NoSuchBucket                  = def("NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist.")
	NoSuchBucketPolicy            = def("NoSuchBucketPolicy", http.StatusNotFound, "The bucket policy does not exist.")
	NoSuchCORSConfiguration       = def("NoSuchCORSConfiguration", http.StatusNotFound, "The CORS configuration does not exist.")
	NoSuchKey                     = def("NoSuchKey", http.StatusNotFound, "The specified key does not exist.")
	NoSuchLifecycleConfiguration  = def("NoSuchLifecycleConfiguration", http.StatusNotFound, "The lifecycle configuration does not exist.")
	NoSuchTagSet                  = def("NoSuchTagSet", http.StatusNotFound, "There is no tag set associated with the bucket.")
	NoSuchUpload                  = def("NoSuchUpload", http.StatusNotFound, "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")
	NotImplemented                = def("NotImplemented", http.StatusNotImplemented, "A header or query you provided implies functionality that is not implemented.")
	NotModified                   = def("NotModified", http.StatusNotModified, "Not Modified")
	PreconditionFailed            = def("PreconditionFailed", http.StatusPreconditionFailed, "At least one of the pre-conditions you specified did not hold.")
	RequestTimeTooSkewed          = def("RequestTimeTooSkewed", http.StatusForbidden, "The difference between the request time and the server's time is too large.")
	SSEConfigurationNotFound      = def("ServerSideEncryptionConfigurationNotFoundError", http.StatusNotFound, "The server side encryption configuration was not found.")
	SignatureDoesNotMatch         = def("SignatureDoesNotMatch", http.StatusForbidden, "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
	XAmzContentSHA256Mismatch     = def("XAmzContentSHA256Mismatch", http.StatusBadRequest, "The provided 'x-amz-content-sha256' header does not match what was computed.")
)

// Internal wraps an unexpected error as an InternalError, keeping the cause
// for logging via errors.Unwrap.
func Internal(err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return err
	}
	return &internalError{cause: err}
}

type internalError struct{ cause error }

func (e *internalError) Error() string   { return "InternalError: " + e.cause.Error() }
func (e *internalError) Unwrap() []error { return []error{InternalError, e.cause} }

// As returns the S3 error for err. Unrecognised errors become InternalError.
func As(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return InternalError
}
