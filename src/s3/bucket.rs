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

use crate::s3::{
    error,
    request::{Builder, Payload},
    Credentials, Metadata, Object, Region, Result,
};
use surf::{http::Method, Client, StatusCode, Url};

#[derive(Clone, Copy, PartialEq, Eq)]
enum StaticAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
}

pub struct Configuration {
    object_lock: bool,
    acl: StaticAcl,
}

pub struct Bucket {
    name: String,
    region: Region,
    credentials: Credentials,

    client: Client,
}

impl Bucket {
    /// Creates a new bucket with the given name in the specified region.
    ///
    /// It does not try to create the bucket at the S3 backend. Instead,
    /// `new` returns a bucket that may or may not exist. Use [`Bucket::create`]
    /// to create a bucket at the S3 server.
    ///
    /// [`Bucket::create`]: struct.Bucket.html#method.create
    ///
    /// # Example
    /// ```
    /// use minio::s3::{Bucket, Region, Credentials};
    ///
    /// let region = Region::UsEast1;
    /// let credentials = Credentials::from_static(
    ///     "AKIAIOSFODNN7EXAMPLE",
    ///     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    /// );
    ///
    /// let bucket = Bucket::new("my-bucket", region, credentials);
    ///
    /// assert_eq!("my-bucket", bucket.name());
    /// assert_eq!(Region::UsEast1, *bucket.region());
    /// ```
    pub fn new(name: impl AsRef<str>, region: Region, credentials: Credentials) -> Self {
        Self {
            name: String::from(name.as_ref()),
            region: region,
            credentials: credentials,
            client: Client::new(),
        }
    }

    /// Creates a new bucket with the given name in the specified region.
    ///
    /// It returns an error when the bucket creation at the S3 backend fails.
    /// In particular, it returns an error when there is already a bucket with
    /// the given name.
    ///
    /// To create a bucket that may already exists use [`Bucket::new`].
    ///
    /// [`Bucket::new`]: struct.Bucket.html#method.new
    ///
    /// # Example
    /// ```no_run
    /// use minio::s3::{Bucket, bucket::Configuration, Region, Credentials};
    /// use async_std::task;
    ///
    /// let region = Region::UsEast1;
    /// let credentials = Credentials::from_static(
    ///     "AKIAIOSFODNN7EXAMPLE",
    ///     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    /// );
    ///
    /// let bucket_config = Configuration::default();
    /// let bucket = task::block_on(Bucket::create("my-bucket", region, credentials, bucket_config)).unwrap();
    ///
    /// assert_eq!("my-bucket", bucket.name());
    /// assert_eq!(Region::UsEast1, *bucket.region());
    /// ```
    pub async fn create(
        name: impl AsRef<str>,
        region: Region,
        credentials: Credentials,
        config: Configuration,
    ) -> Result<Self> {
        let url = Url::parse(
            format!(
                "{endpoint}/{name}",
                endpoint = region.endpoint(),
                name = name.as_ref()
            )
            .as_str(),
        )?;

        // TODO(aead): Send region as XML body
        //  let body = format!(
        //    r#"<?xml version="1.0" encoding="UTF-8"?><CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>"#,
        //    region.to_string()
        // );
        let request = Builder::new(Method::Put, url)
            .region(region.clone())
            .header("X-Amz-Acl", config.acl.as_str())
            .sign_empty(&credentials)?;

        let client = Client::new();
        match client.send(request).await {
            Ok(response) if StatusCode::Ok == response.status() => Ok(Self {
                name: String::from(name.as_ref()),
                region,
                credentials,
                client,
            }),
            Ok(mut response) => Err(error::from_string(response.body_string().await?)),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn delete(self) -> Result<()> {
        let url = Url::parse(
            format!(
                "{endpoint}/{name}",
                endpoint = self.region.endpoint(),
                name = self.name,
            )
            .as_str(),
        )?;

        let request = Builder::new(Method::Put, url)
            .region(self.region.clone())
            .sign_empty(&self.credentials)?;

        match self.client.send(request).await {
            Ok(response) if StatusCode::NoContent == response.status() => Ok(()),
            Ok(mut response) => Err(error::from_string(response.body_string().await?)),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn get_object(&self, name: &str) -> Result<Object> {
        let url = Url::parse(
            format!(
                "{endpoint}/{bucket}/{name}",
                endpoint = self.region().endpoint(),
                bucket = self.name(),
                name = name
            )
            .as_str(),
        )?;

        let request = Builder::new(Method::Get, url)
            .region(self.region.clone())
            .sign_empty(&self.credentials)?;

        match self.client.send(request).await {
            Ok(mut response) if StatusCode::Ok == response.status() => Ok(Object::new(
                name,
                Metadata::try_from(response.as_ref())?,
                response.take_body(),
            )),
            Ok(mut response) => Err(error::from_string(response.body_string().await?)),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn put_object(
        &self,
        name: &str,
        data: impl Payload + Send + Sync + Unpin + 'static,
    ) -> Result<()> {
        let url = Url::parse(
            format!(
                "{endpoint}/{bucket}/{name}",
                endpoint = self.region().endpoint(),
                bucket = self.name(),
                name = name
            )
            .as_str(),
        )?;

        let request = Builder::new(Method::Put, url)
            .region(self.region.clone())
            .sign(&self.credentials, data)?;

        match self.client.send(request).await {
            Ok(response) if StatusCode::Ok == response.status() => Ok(()),
            Ok(mut response) => Err(error::from_string(response.body_string().await?)),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn put_object_bytes<T: AsRef<[u8]>>(&self, name: &str, data: T) -> Result<()> {
        let url = Url::parse(
            format!(
                "{endpoint}/{bucket}/{name}",
                endpoint = self.region().endpoint(),
                bucket = self.name(),
                name = name
            )
            .as_str(),
        )?;

        let request = Builder::new(Method::Put, url)
            .region(self.region.clone())
            .sign_bytes(&self.credentials, data)?;

        match self.client.send(request).await {
            Ok(response) if StatusCode::Ok == response.status() => Ok(()),
            Ok(mut response) => Err(error::from_string(response.body_string().await?)),
            Err(err) => Err(err.into()),
        }
    }

    #[inline]
    pub fn region(&self) -> &Region {
        &self.region
    }

    #[inline]
    pub fn name(&self) -> &str {
        return self.name.as_str();
    }
}

// === Configuration ===

impl Configuration {
    pub fn public() -> Self {
        Self {
            object_lock: false,
            acl: StaticAcl::Private,
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            object_lock: false,
            acl: StaticAcl::Private,
        }
    }
}

impl StaticAcl {
    pub fn as_str(&self) -> &str {
        use self::StaticAcl::*;
        match *self {
            Private => "private",
            PublicRead => "public-read",
            PublicReadWrite => "public-read-write",
            AuthenticatedRead => "authenticated-read",
        }
    }
}
