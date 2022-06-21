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

/// The generic S3 `Result` type definition that contains
/// either some value of type `T` or an `s3::Error`.
pub type Result<T> = std::result::Result<T, Error>;

pub use bucket::Bucket;
pub mod bucket;

pub use error::{Error, ErrorCode};

pub use credentials::Credentials;
pub mod credentials;

pub use region::{InvalidRegion, Region};
pub mod region;

pub use etag::{Etag, InvalidEtag};
pub mod etag;

pub use object::{InvalidMetadata, Metadata, Object, StorageClass};
pub mod object;

mod error;
mod request;
mod sv4;
