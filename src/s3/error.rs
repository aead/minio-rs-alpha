// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::s3::InvalidMetadata;
use serde_derive::Deserialize;
use std::{convert::Infallible, fmt};
use surf::http::url;

/// A generic S3 error.
#[derive(Debug)]
pub struct Error {
    inner: ErrorKind,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    AccessDenied,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,

    #[doc(hidden)]
    Undefined,
}

#[derive(Debug)]
enum ErrorKind {
    Http(surf::Error),

    Url(url::ParseError),

    Metadata(InvalidMetadata),

    S3(ErrorCode, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownErrorCode(String);

#[derive(Debug, Deserialize, PartialEq)]
struct ErrorResponse {
    #[serde(rename(deserialize = "Code"))]
    code: String,

    #[serde(rename(deserialize = "Message"))]
    message: String,
}

pub(crate) fn from_string(s: impl AsRef<str>) -> Error {
    let result: Result<ErrorResponse, serde_xml_rs::Error> = serde_xml_rs::from_str(s.as_ref());
    let error = match result {
        Ok(response) => match response.code.parse::<ErrorCode>() {
            Ok(code) => ErrorKind::S3(code, response.message),
            Err(error) => ErrorKind::S3(ErrorCode::Undefined, error.to_string()),
        },
        Err(why) => ErrorKind::S3(ErrorCode::Undefined, why.to_string()),
    };
    Error { inner: error }
}

// === Error ===

impl Error {
    pub fn code(&self) -> Option<ErrorCode> {
        match self.inner {
            ErrorKind::S3(code, ..) => Some(code),
            _ => None,
        }
    }
}

impl std::error::Error for Error {}

impl From<Infallible> for Error {
    fn from(err: Infallible) -> Self {
        match err {}
    }
}

impl<T> From<T> for Error
where
    ErrorKind: From<T>,
{
    fn from(value: T) -> Self {
        Self {
            inner: ErrorKind::from(value),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::ErrorKind::*;
        match self.inner {
            Http(ref err) => fmt::Display::fmt(err, f),
            Url(ref err) => fmt::Display::fmt(err, f),
            Metadata(ref err) => fmt::Display::fmt(err, f),
            S3(code, ref msg) => write!(f, "{}: {}", code, msg),
        }
    }
}

// === ErrorKind ===

impl From<InvalidMetadata> for ErrorKind {
    fn from(err: InvalidMetadata) -> Self {
        Self::Metadata(err)
    }
}

impl From<surf::Error> for ErrorKind {
    fn from(err: surf::Error) -> Self {
        Self::Http(err)
    }
}

impl From<url::ParseError> for ErrorKind {
    fn from(err: url::ParseError) -> Self {
        Self::Url(err)
    }
}

impl From<UnknownErrorCode> for ErrorKind {
    fn from(err: UnknownErrorCode) -> Self {
        Self::S3(ErrorCode::Undefined, err.0)
    }
}

// === ErrorCode ===

impl std::str::FromStr for ErrorCode {
    type Err = UnknownErrorCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use self::ErrorCode::*;
        match s {
            "AccessDenied" => Ok(AccessDenied),
            "BucketAlreadyExists" => Ok(BucketAlreadyExists),
            "BucketAlreadyOwnedByYou" => Ok(BucketAlreadyOwnedByYou),
            "BucketNotEmpty" => Ok(BucketNotEmpty),
            _ => Err(UnknownErrorCode(String::from(s))),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::ErrorCode::*;
        let s = match *self {
            AccessDenied => "AccessDenied",
            BucketAlreadyExists => "BucketAlreadyExists",
            BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            BucketNotEmpty => "BucketNotEmpty",

            Undefined => "Undefined",
        };
        write!(f, "{}", s)
    }
}

// === UnknownErrorCode ===

impl fmt::Display for UnknownErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Unknown S3 error code: {}", self.0)
    }
}
