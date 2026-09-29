use std::fmt;

use http::StatusCode;

/// An S3 API error. Used throughout the server (storage included) because every
/// failure ultimately has to be reported to the client as one of these codes.
#[derive(Debug, Clone)]
pub struct S3Error {
    pub code: ErrorCode,
    pub message: String,
}

pub type S3Result<T> = Result<T, S3Error>;

macro_rules! error_codes {
    ($( $name:ident => $status:expr, $msg:expr; )*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum ErrorCode { $( $name, )* }

        impl ErrorCode {
            pub fn as_str(self) -> &'static str {
                match self { $( ErrorCode::$name => stringify!($name), )* }
            }
            pub fn status(self) -> StatusCode {
                match self { $( ErrorCode::$name => $status, )* }
            }
            pub fn default_message(self) -> &'static str {
                match self { $( ErrorCode::$name => $msg, )* }
            }
        }
    };
}

error_codes! {
    AccessDenied => StatusCode::FORBIDDEN, "Access Denied";
    AuthorizationHeaderMalformed => StatusCode::BAD_REQUEST, "The authorization header is malformed.";
    AuthorizationQueryParametersError => StatusCode::BAD_REQUEST, "Error parsing the X-Amz-Credential parameter.";
    BadDigest => StatusCode::BAD_REQUEST, "The Content-MD5 or checksum you specified did not match what we received.";
    BucketAlreadyExists => StatusCode::CONFLICT, "The requested bucket name is not available.";
    BucketAlreadyOwnedByYou => StatusCode::CONFLICT, "Your previous request to create the named bucket succeeded and you already own it.";
    BucketNotEmpty => StatusCode::CONFLICT, "The bucket you tried to delete is not empty.";
    CORSResponse => StatusCode::FORBIDDEN, "This CORS request is not allowed.";
    EntityTooLarge => StatusCode::BAD_REQUEST, "Your proposed upload exceeds the maximum allowed object size.";
    EntityTooSmall => StatusCode::BAD_REQUEST, "Your proposed upload is smaller than the minimum allowed object size.";
    IncompleteBody => StatusCode::BAD_REQUEST, "You did not provide the number of bytes specified by the Content-Length HTTP header.";
    InternalError => StatusCode::INTERNAL_SERVER_ERROR, "We encountered an internal error. Please try again.";
    InvalidAccessKeyId => StatusCode::FORBIDDEN, "The AWS access key ID you provided does not exist in our records.";
    InvalidArgument => StatusCode::BAD_REQUEST, "Invalid Argument";
    InvalidBucketName => StatusCode::BAD_REQUEST, "The specified bucket is not valid.";
    InvalidDigest => StatusCode::BAD_REQUEST, "The Content-MD5 you specified is not valid.";
    InvalidPart => StatusCode::BAD_REQUEST, "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.";
    InvalidPartNumber => StatusCode::RANGE_NOT_SATISFIABLE, "The requested partnumber is not satisfiable.";
    InvalidPartOrder => StatusCode::BAD_REQUEST, "The list of parts was not in ascending order. Parts must be ordered by part number.";
    InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE, "The requested range is not satisfiable.";
    InvalidRequest => StatusCode::BAD_REQUEST, "Invalid Request";
    KeyTooLongError => StatusCode::BAD_REQUEST, "Your key is too long.";
    MalformedXML => StatusCode::BAD_REQUEST, "The XML you provided was not well-formed or did not validate against our published schema.";
    MetadataTooLarge => StatusCode::BAD_REQUEST, "Your metadata headers exceed the maximum allowed metadata size.";
    MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED, "The specified method is not allowed against this resource.";
    MissingContentLength => StatusCode::LENGTH_REQUIRED, "You must provide the Content-Length HTTP header.";
    NoSuchBucket => StatusCode::NOT_FOUND, "The specified bucket does not exist.";
    NoSuchBucketPolicy => StatusCode::NOT_FOUND, "The bucket policy does not exist.";
    NoSuchCORSConfiguration => StatusCode::NOT_FOUND, "The CORS configuration does not exist.";
    NoSuchKey => StatusCode::NOT_FOUND, "The specified key does not exist.";
    NoSuchLifecycleConfiguration => StatusCode::NOT_FOUND, "The lifecycle configuration does not exist.";
    NoSuchTagSet => StatusCode::NOT_FOUND, "There is no tag set associated with the bucket.";
    NoSuchUpload => StatusCode::NOT_FOUND, "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.";
    NotImplemented => StatusCode::NOT_IMPLEMENTED, "A header or query you provided implies functionality that is not implemented.";
    NotModified => StatusCode::NOT_MODIFIED, "Not Modified";
    PreconditionFailed => StatusCode::PRECONDITION_FAILED, "At least one of the pre-conditions you specified did not hold.";
    RequestTimeTooSkewed => StatusCode::FORBIDDEN, "The difference between the request time and the server's time is too large.";
    ServerSideEncryptionConfigurationNotFoundError => StatusCode::NOT_FOUND, "The server side encryption configuration was not found.";
    SignatureDoesNotMatch => StatusCode::FORBIDDEN, "The request signature we calculated does not match the signature you provided. Check your key and signing method.";
    XAmzContentSHA256Mismatch => StatusCode::BAD_REQUEST, "The provided 'x-amz-content-sha256' header does not match what was computed.";
}

impl S3Error {
    pub fn new(code: ErrorCode) -> Self {
        S3Error { code, message: code.default_message().to_string() }
    }

    pub fn msg(code: ErrorCode, message: impl Into<String>) -> Self {
        S3Error { code, message: message.into() }
    }

    pub fn internal(err: impl fmt::Display) -> Self {
        tracing::error!("internal error: {err}");
        S3Error::new(ErrorCode::InternalError)
    }
}

impl From<ErrorCode> for S3Error {
    fn from(code: ErrorCode) -> Self {
        S3Error::new(code)
    }
}

impl From<std::io::Error> for S3Error {
    fn from(e: std::io::Error) -> Self {
        S3Error::internal(e)
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for S3Error {}
