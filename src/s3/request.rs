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

use crate::{
    s3::sv4,
    s3::{Credentials, Region, Result},
};
use async_std::io::{BufRead, Cursor, Empty};
use surf::http::{
    headers::{HeaderName, ToHeaderValues},
    Method, Mime, Request, Url,
};

pub trait Payload: BufRead {
    fn len(&self) -> Option<u64> {
        None
    }
}

pub struct Builder {
    inner: surf::http::Request,
    region: Region,
}

impl Builder {
    pub fn new(method: Method, url: Url) -> Self {
        Self {
            inner: surf::http::Request::new(method, url),
            region: Region::default(),
        }
    }

    pub fn region(mut self, region: impl Into<Region>) -> Self {
        self.region = region.into();
        self
    }

    pub fn header(mut self, key: impl Into<HeaderName>, value: impl ToHeaderValues) -> Self {
        self.inner.insert_header(key, value);
        self
    }

    pub fn content_type(mut self, content_type: impl Into<Mime>) -> Self {
        self.inner.set_content_type(content_type.into());
        self
    }

    pub fn sign(
        mut self,
        credentials: &Credentials,
        content: impl Payload + Send + Sync + 'static,
    ) -> Result<Request> {
        let size = match content.len() {
            Some(n) => Some(n as usize),
            None => None,
        };
        self.inner
            .set_body(surf::Body::from_reader(Box::pin(content), size));

        sv4::sign(
            &self.region,
            credentials,
            self.inner,
            sv4::ContentType::Unsigned,
        )
    }

    pub fn sign_bytes(
        mut self,
        credentials: &Credentials,
        content: impl AsRef<[u8]>,
    ) -> Result<Request> {
        self.inner.set_body(surf::Body::from(content.as_ref()));

        sv4::sign(
            &self.region,
            credentials,
            self.inner,
            sv4::ContentType::Unsigned,
        )
    }

    pub fn sign_empty(mut self, credentials: &Credentials) -> Result<Request> {
        self.inner.set_body(surf::Body::empty());
        sv4::sign(
            &self.region,
            credentials,
            self.inner,
            sv4::ContentType::Empty,
        )
    }
}

impl<T> Payload for Cursor<T>
where
    T: AsRef<[u8]> + Unpin,
{
    fn len(&self) -> Option<u64> {
        Some(self.get_ref().as_ref().len() as u64)
    }
}

impl Payload for Empty {
    fn len(&self) -> Option<u64> {
        Some(0)
    }
}
