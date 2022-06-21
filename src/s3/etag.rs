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

extern crate hex;
use async_std::io::{Read as AsyncRead, Write as AsyncWrite};
use md5::Digest;
use std::{
    convert::TryFrom,
    fmt, io,
    io::Read,
    io::Write,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
};

/// ETag represents a valid S3 ETag.
///
/// Every S3 object has an associated ETag. In certain
/// cases, an S3 ETag is the MD5 fingerprint of the
/// object content. In particlar, the ETag of an object
/// depends upon whether the object has been uploaded
/// using a single or multple S3 API call and whether
/// the object is encrypted resp. the encryption method
/// used.
///
/// In general, objects uploaded using a single S3
/// API call (single-part) have an associated ETag
/// that corresponds to the content MD5 sum.
/// However, this is not the case for objects
/// encrypted using SSE-C or SSE-KMS.
///
/// In contrast, objects uploaded using multiple S3
/// API calls (multi-part) have an associated ETag
/// that does not correspond to the object MD5 sum.
/// Instead, their ETag is computed from the ETags
/// of their object parts.
/// As long as a multi-part object is not committed
/// each object part has its own ETag - computed as
/// specified above for single-part objects.
/// Now, the ETag of a multi-part object is the MD5
/// sum of the concatenation of all part ETags.
///
/// Further, multi-part ETags have a part counter
/// suffix `-N`. An object with a suffix `-N` has
/// consists of `N` parts. An S3 object can consist
/// of at least `1` and at most of `10000` parts.
///
/// # Examples
///
/// ```
/// use minio::s3::Etag;
///
/// let etag = "d41d8cd98f00b204e9800998ecf8427e".parse::<Etag>().unwrap();
/// assert_eq!(None, etag.parts());
///
/// let etag = "d41d8cd98f00b204e9800998ecf8427e-38".parse::<Etag>().unwrap();
/// assert_eq!(Some(38), etag.parts());
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Etag {
    bytes: [u8; 16],
    parts: Option<u16>,
}

/// A possible error when parsing an S3 ETag.
pub struct InvalidEtag {
    _priv: (),
}

/// Wrapper around an md5::MD5 implementing async io::Write.
struct Md5(md5::Md5);

impl Etag {
    /// Computes the `Etag` of the data returned from the reader.
    ///
    /// Continuously reads data from `reader`, until `EOF`, and
    /// computes the `Etag` as MD5 sum of the read data.
    ///
    /// # Example
    ///
    /// ```
    /// use minio::s3::Etag;
    /// use async_std::{task, io::Cursor};
    ///
    /// let mut reader = Cursor::new("Hello World");
    /// let etag = task::block_on(Etag::compute(&mut reader)).unwrap();
    ///
    /// assert_eq!("b10a8db164e0754105b7a99be72e3fe5".parse::<Etag>().unwrap(), etag);
    /// ```
    pub async fn compute<R>(reader: &mut R) -> io::Result<Etag>
    where
        R: AsyncRead + Unpin + ?Sized,
    {
        let mut h = Md5::from(md5::Md5::new());
        async_std::io::copy(reader, &mut h).await?;

        let h: md5::Md5 = h.into();
        Ok(Etag {
            bytes: h.finalize().into(),
            parts: None,
        })
    }

    /// Computes the `Etag` of the data returned from the reader.
    ///
    /// Continuously reads data from `reader`, until `EOF`, and
    /// computes the `Etag` as MD5 sum of the read data.
    ///
    /// # Example
    ///
    /// ```
    /// use minio::s3::Etag;
    ///
    /// let mut reader = "Hello World".as_bytes();
    /// let etag = Etag::compute_blocking(&mut reader).unwrap();
    ///
    /// assert_eq!("b10a8db164e0754105b7a99be72e3fe5".parse::<Etag>().unwrap(), etag);
    /// ```
    pub fn compute_blocking<R>(reader: &mut R) -> io::Result<Etag>
    where
        R: Read + ?Sized,
    {
        let mut h = md5::Md5::new();
        io::copy(reader, &mut h)?;
        Ok(Etag {
            bytes: h.finalize().into(),
            parts: None,
        })
    }

    /// Computes the `Etag` as MD5 sum of the given bytes.
    ///
    /// # Example
    ///
    /// ```
    /// use minio::s3::Etag;
    ///
    /// let bytes = "Hello World";
    /// let etag = Etag::compute_from(bytes);
    ///
    /// assert_eq!("b10a8db164e0754105b7a99be72e3fe5".parse::<Etag>().unwrap(), etag);
    /// ```
    #[inline]
    pub fn compute_from(bytes: impl AsRef<[u8]>) -> Self {
        Self {
            bytes: md5::Md5::digest(bytes).into(),
            parts: None,
        }
    }

    /// Returns `Some` number of parts in case of a multi-part `Etag` or
    /// `None` for single-part `Etag`s.
    ///
    /// # Example
    ///
    /// ```
    /// use minio::s3::Etag;
    ///
    /// let etag = "d41d8cd98f00b204e9800998ecf8427e".parse::<Etag>().unwrap();
    /// assert_eq!(None, etag.parts());
    ///
    /// let etag = "6444b13fe31e91e727a67d2c23417a8f-3".parse::<Etag>().unwrap();
    /// assert_eq!(Some(3), etag.parts());
    /// ```
    #[inline]
    pub fn parts(&self) -> Option<u16> {
        self.parts
    }
}

impl fmt::Display for Etag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.bytes {
            write!(f, "{:x}", byte)?;
        }
        match self.parts {
            Some(n) => write!(f, "-{}", n),
            None => Ok(()),
        }
    }
}

impl fmt::Debug for Etag {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <Self as fmt::Display>::fmt(&self, f)
    }
}

impl FromStr for Etag {
    type Err = InvalidEtag;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut s = s;

        // Some S3 libraries expect quoted ETags.
        // For compatibility we accept quoted ETags
        // as well.
        if s.starts_with("\"") {
            s = &s[1..]
        }
        if s.ends_with("\"") {
            s = &s[..s.len() - 1]
        }

        if s.len() < 32 {
            return Err(Self::Err::new());
        }

        if s.len() == 32 {
            let mut bytes = [0; 16];
            hex::decode_to_slice(s, &mut bytes as &mut [u8])?;
            return Ok(Self { bytes, parts: None });
        }

        let (prefix, suffix) = match s.split_once("-") {
            Some(v) => v,
            None => return Err(Self::Err::new()),
        };
        if prefix.len() != 32 {
            return Err(Self::Err::new());
        }

        let mut bytes = [0; 16];
        hex::decode_to_slice(prefix, &mut bytes as &mut [u8])?;
        match suffix.parse::<u16>() {
            Ok(parts) if parts > 0 && parts <= 10000 => Ok(Self {
                bytes,
                parts: Some(parts),
            }),
            _ => Err(Self::Err::new()),
        }
    }
}

impl TryFrom<String> for Etag {
    type Error = InvalidEtag;

    #[inline]
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(s.as_str())
    }
}

impl<'a> TryFrom<&'a String> for Etag {
    type Error = InvalidEtag;

    #[inline]
    fn try_from(s: &'a String) -> Result<Self, Self::Error> {
        Self::from_str(s)
    }
}

// === InvalidEtag ===

impl InvalidEtag {
    fn new() -> Self {
        Self { _priv: () }
    }
}

impl std::error::Error for InvalidEtag {}

impl fmt::Display for InvalidEtag {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid S3 ETag")
    }
}

impl fmt::Debug for InvalidEtag {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InvalidEtag").finish()
    }
}

impl From<hex::FromHexError> for InvalidEtag {
    fn from(_: hex::FromHexError) -> Self {
        Self::new()
    }
}

// === MD5 ===

impl From<md5::Md5> for Md5 {
    fn from(h: md5::Md5) -> Self {
        Self(h)
    }
}

impl Into<md5::Md5> for Md5 {
    fn into(self) -> md5::Md5 {
        self.0
    }
}

impl AsyncWrite for Md5 {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.0.write(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
