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

use crate::s3::{Etag, InvalidEtag};
use async_std::io::Read;
use std::{
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};
use surf::http::Headers;

pub struct Object {
    name: String,
    content: Content,
    metadata: Metadata,
}

pub struct Metadata {
    etag: Etag,

    size: u64,

    storage_class: StorageClass,
}

pub struct Content {
    inner: surf::Body,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageClass {
    Standard,
    ReducedRedundancy,
    IntelligentTiering,
    Glacier,
    DeepArchive,

    #[doc(hidden)]
    Undefined,
}

pub struct InvalidMetadata {
    _priv: (),
}

pub struct InvalidStorageClass {
    _priv: (),
}

// === Content ===

impl Content {
    #[inline]
    pub fn empty() -> Self {
        Self {
            inner: surf::Body::empty(),
        }
    }

    #[inline]
    pub fn len(&self) -> Option<u64> {
        self.inner.len().map(|n| n as u64)
    }
}

impl Read for Content {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Read::poll_read(Pin::new(&mut self.inner), ctx, buf)
    }
}

// === Object ===

impl Object {
    pub(crate) fn new(name: impl AsRef<str>, metadata: Metadata, body: surf::Body) -> Self {
        Self {
            name: String::from(name.as_ref()),
            content: Content { inner: body },
            metadata,
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn content(&self) -> &Content {
        &self.content
    }

    #[inline]
    pub fn content_mut(&mut self) -> &mut Content {
        &mut self.content
    }

    #[inline]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    #[inline]
    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
}

impl Into<Content> for Object {
    #[inline]
    fn into(self: Self) -> Content {
        self.content
    }
}

impl Into<Metadata> for Object {
    #[inline]
    fn into(self: Self) -> Metadata {
        self.metadata
    }
}

// === Metadata ===

impl Metadata {
    #[inline]
    pub fn etag(&self) -> &Etag {
        &self.etag
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn storage_class(&self) -> StorageClass {
        self.storage_class
    }
}

impl TryFrom<&Headers> for Metadata {
    type Error = InvalidMetadata;

    fn try_from(headers: &Headers) -> Result<Self, Self::Error> {
        use surf::http::headers::*;
        let etag = match headers.get(ETAG) {
            Some(etag) => etag.as_str().parse::<Etag>()?,
            None => return Err(InvalidMetadata::new()),
        };
        let size = match headers.get(CONTENT_LENGTH) {
            Some(size) => size.as_str().parse::<u64>()?,
            None => return Err(InvalidMetadata::new()),
        };
        let storage_class = match headers.get(STORAGE_CLASS) {
            Some(class) => class.as_str().parse::<StorageClass>()?,
            None => StorageClass::Standard,
        };
        Ok(Self {
            etag,
            size,
            storage_class,
        })
    }
}

impl TryFrom<Headers> for Metadata {
    type Error = InvalidMetadata;

    #[inline]
    fn try_from(headers: Headers) -> Result<Self, Self::Error> {
        Metadata::try_from(&headers)
    }
}

// === InvalidMetadata ===

impl InvalidMetadata {
    #[inline]
    fn new() -> Self {
        Self { _priv: () }
    }
}

impl fmt::Debug for InvalidMetadata {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InvalidMetadata").finish()
    }
}

impl fmt::Display for InvalidMetadata {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid metadata")
    }
}

impl From<InvalidEtag> for InvalidMetadata {
    fn from(_: InvalidEtag) -> Self {
        InvalidMetadata::new()
    }
}

impl From<std::num::ParseIntError> for InvalidMetadata {
    fn from(_: std::num::ParseIntError) -> Self {
        InvalidMetadata::new()
    }
}

impl From<InvalidStorageClass> for InvalidMetadata {
    fn from(_: InvalidStorageClass) -> Self {
        InvalidMetadata::new()
    }
}

// === StorageClass ===

impl std::str::FromStr for StorageClass {
    type Err = InvalidStorageClass;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use self::StorageClass::*;
        match s {
            "STANDARD" => Ok(Standard),
            "REDUCED_REDUNDANCY" => Ok(ReducedRedundancy),
            "INTELLIGENT_TIERING" => Ok(IntelligentTiering),
            "GLACIER" => Ok(Glacier),
            "DEEP_ARCHIVE" => Ok(DeepArchive),
            _ => Err(InvalidStorageClass { _priv: () }),
        }
    }
}

// === InvalidStorageClass ===

impl fmt::Debug for InvalidStorageClass {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InvalidStorageClass").finish()
    }
}

const STORAGE_CLASS: &'static str = "X-Amz-Storage-Class";
