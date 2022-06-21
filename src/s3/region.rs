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

use http::Uri;
use std::{convert::TryFrom, fmt, str::FromStr};

/// The S3 region.
///
/// # Examples
/// ```
/// use minio::s3::Region;
///
/// let region = Region::UsEast1;
///
/// let region = "us-east-1".parse().unwrap();
/// assert_eq!(Region::UsEast1, region);
///
/// let region = "localhost:9000".parse::<Region>().unwrap();
/// assert_eq!("localhost:9000", region.endpoint());
///
/// let region = Region::custom_with_region("https://s3.example.com", "us-east-1").unwrap();
/// assert_eq!("https://s3.example.com", region.endpoint());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Region {
    UsEast1,
    UsEast2,
    UsWest1,
    UsWest2,
    Custom { region: Custom },
}

/// A custom S3 `Region`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Custom {
    endpoint: String,
    region: Option<String>,
}

/// A possible error value when converting a string
/// to a `Region`.
pub struct InvalidRegion {
    _priv: (),
}

impl Custom {
    /// Returns the S3 endpoint for this region.
    ///
    /// For custom regions it returns the endpoint unmodified.
    /// Therefore, the returned `&str` might contain the protocol
    /// scheme and port.
    #[inline]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the host name of the S3 region endpoint without
    /// any protocol scheme or port.
    #[inline]
    pub fn host(&self) -> &str {
        let endpoint = match self.endpoint.find("://") {
            Some(n) => &self.endpoint[n + 3..],
            None => &self.endpoint,
        };
        match endpoint.find(":") {
            Some(n) => &endpoint[..n],
            None => endpoint,
        }
    }

    /// Returns either some custom S3 region `&str` or `None`.
    #[inline]
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }
}

impl Region {
    /// Returns a custom S3 `Region` with the given endpoint.
    ///
    /// A custom S3 endpoint has an optional `URI` scheme and
    /// port but always contains a hostname and is structured as
    /// follows:
    /// ```notrust
    /// [<scheme>://]<hostname>[<port>]
    /// ```
    ///
    /// # Example
    /// ```
    /// use minio::s3::Region;
    ///
    /// let region = Region::custom("localhost").unwrap();
    /// assert_eq!("localhost", region.endpoint());
    ///
    /// let region = Region::custom("https://s3.example.com").unwrap();
    /// assert_eq!("https://s3.example.com", region.endpoint());
    ///
    /// let region = Region::custom("192.168.102.55:9000").unwrap();
    /// assert_eq!("192.168.102.55:9000", region.endpoint());
    ///
    /// let region = Region::custom("http://localhost:9000").unwrap();
    /// assert_eq!("http://localhost:9000", region.endpoint());
    /// ```
    pub fn custom(endpoint: impl AsRef<str>) -> Result<Self, InvalidRegion> {
        Ok(Self::Custom {
            region: Custom {
                endpoint: parse_custom_endpoint(endpoint.as_ref())?,
                region: None,
            },
        })
    }

    /// Returns a custom S3 region with the given endpoint
    /// and region `&str`.
    ///
    /// # Example
    /// ```
    /// use minio::s3::Region;
    ///
    /// let region = Region::custom_with_region("https://s3.example.com", "eu-west-1").unwrap();
    /// assert_eq!("https://s3.example.com", region.endpoint());
    ///
    /// let region_str = match region {
    ///    Region::Custom { ref region } => region.region(),
    ///    _ => None,
    /// };
    /// assert_eq!(Some("eu-west-1"), region_str);
    /// ```
    pub fn custom_with_region(
        endpoint: impl AsRef<str>,
        region: impl AsRef<str>,
    ) -> Result<Self, InvalidRegion> {
        Ok(Self::Custom {
            region: Custom {
                endpoint: parse_custom_endpoint(endpoint.as_ref())?,
                region: Some(String::from(region.as_ref())),
            },
        })
    }

    /// Returns the S3 endpoint for this region.
    ///
    /// For custom regions it returns the endpoint unmodified.
    /// Therefore, the returned `&str` might contain the protocol
    /// scheme - for example `https://`.
    ///
    /// # Example
    /// ```
    /// use minio::s3::Region;
    ///
    /// let region = Region::UsEast1;
    /// assert_eq!("https://s3.amazonaws.com", region.endpoint());
    ///
    /// let region = "localhost:9000".parse::<Region>().unwrap();
    /// assert_eq!("localhost:9000",region.endpoint());
    ///
    /// let region = "https://localhost:9000".parse::<Region>().unwrap();
    /// assert_eq!("https://localhost:9000", region.endpoint());
    /// ```
    pub fn endpoint(&self) -> &str {
        use self::Region::*;
        match *self {
            UsEast1 => "https://s3.amazonaws.com", // For us-east-1 there is no s3-us-east-1.amazonaws.com DNS record
            UsEast2 => "https://s3-us-east-2.amazonaws.com",
            UsWest1 => "https://s3-us-west-1.amazonaws.com",
            UsWest2 => "https://s3-us-west-2.amazonaws.com",
            Custom { ref region } => region.endpoint(),
        }
    }

    /// Returns the host name of the S3 region endpoint.
    ///
    /// # Example
    /// ```
    /// use minio::s3::Region;
    ///
    /// let region = Region::UsEast1;
    /// assert_eq!("s3.amazonaws.com", region.host());
    ///
    /// let region = "localhost:9000".parse::<Region>().unwrap();
    /// assert_eq!("localhost", region.host());
    ///
    /// let region = "https://localhost:9000".parse::<Region>().unwrap();
    /// assert_eq!("localhost", region.host());
    /// ```
    pub fn host(&self) -> &str {
        match *self {
            self::Region::Custom { ref region } => region.host(),
            _ => {
                let endpoint = self.endpoint();
                let endpoint = match endpoint.find("://") {
                    Some(n) => &endpoint[n + 3..],
                    None => &endpoint,
                };
                match endpoint.find(":") {
                    Some(n) => &endpoint[..n],
                    None => endpoint,
                }
            }
        }
    }
}

impl Default for Region {
    fn default() -> Self {
        Self::UsEast1
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Region::*;
        match *self {
            UsEast1 => f.write_str("us-east-1"),
            UsEast2 => f.write_str("us-east-2"),
            UsWest1 => f.write_str("us-west-1"),
            UsWest2 => f.write_str("us-west-2"),
            Custom { ref region } => match region.region() {
                Some(region) => f.write_str(region),
                None => Ok(()),
            },
        }
    }
}

impl FromStr for Region {
    type Err = InvalidRegion;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "us-east-1" => Ok(Self::UsEast1),
            "us-east-2" => Ok(Self::UsEast2),
            "us-west-1" => Ok(Self::UsWest1),
            "us-west-2" => Ok(Self::UsWest2),
            _ => Self::custom(s),
        }
    }
}

impl<'a> TryFrom<&'a String> for Region {
    type Error = InvalidRegion;

    #[inline]
    fn try_from(s: &'a String) -> Result<Self, Self::Error> {
        Self::from_str(s.as_str())
    }
}

impl TryFrom<String> for Region {
    type Error = InvalidRegion;

    #[inline]
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(s.as_str())
    }
}

impl fmt::Debug for InvalidRegion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InvalidRegion").finish()
    }
}

impl fmt::Display for InvalidRegion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("invalid S3 region").finish()
    }
}

fn parse_custom_endpoint(endpoint: &str) -> Result<String, InvalidRegion> {
    match Uri::from_str(endpoint) {
        Ok(uri) if uri.scheme().is_some() && uri.host().is_some() && uri.port().is_some() => {
            Ok(format!(
                "{scheme}://{host}:{port}",
                scheme = uri.scheme_str().unwrap(),
                host = uri.host().unwrap(),
                port = uri.port().unwrap().as_str()
            ))
        }
        Ok(uri) if uri.scheme().is_some() && uri.host().is_some() => Ok(format!(
            "{scheme}://{host}",
            scheme = uri.scheme_str().unwrap(),
            host = uri.host().unwrap(),
        )),
        Ok(uri) if uri.host().is_some() && uri.port().is_some() => Ok(format!(
            "{host}:{port}",
            host = uri.host().unwrap(),
            port = uri.port().unwrap().as_str()
        )),
        Ok(uri) if uri.host().is_some() => Ok(String::from(uri.host().unwrap())),
        _ => Err(InvalidRegion { _priv: () }),
    }
}
