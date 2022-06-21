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

pub struct Error {}

#[derive(Debug)]
pub struct Builder {
    inner: Credentials,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
    security_token: Option<String>,
}

impl Credentials {
    /// Examples
    /// ```
    /// use minio::s3::Credentials;
    ///
    /// let credentials: Credentials = Credentials::new()
    ///     .access_key("AKIAIOSFODNN7EXAMPLE")
    ///     .secret_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    ///     .into();
    ///
    /// assert_eq!("AKIAIOSFODNN7EXAMPLE", credentials.access_key().unwrap());
    /// assert_eq!("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", credentials.secret_key().unwrap());
    /// ```
    pub fn new() -> Builder {
        Builder::new()
    }

    ///
    /// Examples
    /// ```
    /// use minio::s3::Credentials;
    ///
    /// let credentials = Credentials::from_static("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    ///
    /// assert_eq!("AKIAIOSFODNN7EXAMPLE", credentials.access_key().unwrap());
    /// assert_eq!("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", credentials.secret_key().unwrap());
    /// ```
    pub fn from_static(access_key: &str, secret_key: &str) -> Self {
        Self {
            access_key: Some(String::from(access_key)),
            secret_key: Some(String::from(secret_key)),
            session_token: None,
            security_token: None,
        }
    }

    /// Examples
    /// ```
    /// use minio::s3::Credentials;
    ///
    /// let credentials = Credentials::anonym();
    ///
    /// assert_eq!(None, credentials.access_key());
    /// assert_eq!(None, credentials.secret_key());
    /// ```
    pub fn anonym() -> Self {
        Self {
            access_key: None,
            secret_key: None,
            session_token: None,
            security_token: None,
        }
    }

    pub const fn is_anonym(&self) -> bool {
        self.access_key.is_none()
            && self.secret_key.is_none()
            && self.session_token.is_none()
            && self.security_token.is_none()
    }

    pub fn access_key(&self) -> Option<&str> {
        self.access_key.as_deref()
    }

    pub fn secret_key(&self) -> Option<&str> {
        self.secret_key.as_deref()
    }

    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    pub fn security_token(&self) -> Option<&str> {
        self.security_token.as_deref()
    }
}

impl From<Builder> for Credentials {
    fn from(builder: Builder) -> Self {
        return builder.inner;
    }
}

impl Builder {
    pub fn new() -> Self {
        Self {
            inner: Credentials::anonym(),
        }
    }

    pub fn access_key(mut self, access_key: impl AsRef<str>) -> Self {
        self.inner.access_key = Some(String::from(access_key.as_ref()));
        self
    }

    pub fn secret_key(mut self, secret_key: impl AsRef<str>) -> Self {
        self.inner.secret_key = Some(String::from(secret_key.as_ref()));
        self
    }

    pub fn security_token(mut self, security_token: impl AsRef<str>) -> Self {
        self.inner.security_token = Some(String::from(security_token.as_ref()));
        self
    }

    pub fn session_token(mut self, session_token: impl AsRef<str>) -> Self {
        self.inner.session_token = Some(String::from(session_token.as_ref()));
        self
    }
}
