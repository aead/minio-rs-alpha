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
    s3,
    s3::{Credentials, Region},
};
use hex;
use hmac::{Hmac, Mac};
use md5::Digest;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use sha2::Sha256;
use std::str::FromStr;
use surf::http::{headers, headers::HeaderValue, Request, Url};
use time::{format_description::FormatItem, macros::format_description, OffsetDateTime};

pub fn sign(
    region: &Region,
    credentials: &Credentials,
    mut request: Request,
    kind: ContentType,
) -> s3::Result<Request> {
    let now = time::OffsetDateTime::now_utc();
    let now_datetime = now
        .format(DATETIME)
        .expect("format timestamp using DATE-TIME");

    request.insert_header(AMZ_DATE, HeaderValue::from_str(now_datetime.as_str())?);
    request.insert_header(headers::HOST, HeaderValue::from_str(region.host())?);
    request.insert_header(AMZ_CONTENT_SHA256, HeaderValue::from_str(kind.as_ref())?);

    let canonical_request = canonical_request(
        request.method().to_string(),
        request.url(),
        &request,
        kind.as_ref(),
    );

    let authorization = authorization(
        region,
        &credentials,
        &now,
        canonical_request,
        request.header_names(),
    );
    request.insert_header(
        headers::AUTHORIZATION,
        HeaderValue::from_str(authorization.as_ref())?,
    );
    Ok(request.into())
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ContentType {
    Empty,
    Unsigned,
}

impl AsRef<str> for ContentType {
    fn as_ref(&self) -> &str {
        match *self {
            Self::Empty => "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            Self::Unsigned => "UNSIGNED-PAYLOAD",
        }
    }
}

fn authorization(
    region: &Region,
    credentials: &Credentials,
    now: &time::OffsetDateTime,
    canonical: impl AsRef<str>,
    names: headers::Names,
) -> String {
    let access_key = credentials.access_key().expect("some access key");
    let secret_key = credentials.secret_key().expect("some secret key");

    let string_to_sign = string_to_sign(&now, region, canonical.as_ref());
    let signing_key = signing_key(&now, secret_key, region, "s3");

    let mut hmac =
        Hmac::<Sha256>::new_from_slice(&signing_key).expect("HMAC-SHA256 from signing key");
    hmac.update(string_to_sign.as_bytes());

    format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={headers},Signature={signature}",
            access_key = access_key,
            scope = scope_string(&now, &region),
            headers = signed_header_string(names),
            signature = hex::encode(hmac.finalize().into_bytes()))
}

/// Encode a URI following the specific requirements of the AWS service.
fn uri_encode(string: &str, encode_slash: bool) -> String {
    if encode_slash {
        utf8_percent_encode(string, FRAGMENT_SLASH).to_string()
    } else {
        utf8_percent_encode(string, FRAGMENT).to_string()
    }
}

/// Generate a canonical URI string from the given URL.
fn canonical_uri_string(url: &Url) -> String {
    let decoded = percent_encoding::percent_decode_str(url.path()).decode_utf8_lossy();
    uri_encode(&decoded, false)
}

/// Generate a canonical query string from the query pairs in the given URL.
fn canonical_query_string(url: &Url) -> String {
    let mut keyvalues: Vec<(String, String)> = url
        .query_pairs()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    keyvalues.sort();
    let keyvalues: Vec<String> = keyvalues
        .iter()
        .map(|(k, v)| uri_encode(k, true) + "=" + &uri_encode(v, true))
        .collect();
    keyvalues.join("&")
}

/// Generate a canonical header string from the provided headers.
fn canonical_header_string(request: &Request) -> String {
    let mut keyvalues = request
        .iter()
        .map(|(key, value)| {
            // Values that are not strings are silently dropped (AWS wouldn't
            // accept them anyway)
            key.as_str().to_lowercase() + ":" + value.as_str().trim()
        })
        .collect::<Vec<String>>();
    keyvalues.sort();
    keyvalues.join("\n")
}

/// Generate a signed header string from the provided headers.
fn signed_header_string(keys: surf::http::headers::Names) -> String {
    let mut keys = keys
        .map(|key| key.as_str().to_lowercase())
        .collect::<Vec<String>>();
    keys.sort();
    keys.join(";")
}

/// Generate a canonical request.
fn canonical_request(
    method: impl AsRef<str>,
    url: &Url,
    request: &Request,
    sha256: &str,
) -> String {
    format!(
        "{method}\n{uri}\n{query_string}\n{headers}\n\n{signed}\n{sha256}",
        method = method.as_ref(),
        uri = canonical_uri_string(url),
        query_string = canonical_query_string(url),
        headers = canonical_header_string(request),
        signed = signed_header_string(request.header_names()),
        sha256 = sha256
    )
}

/// Generate an AWS scope string.
fn scope_string(now: &OffsetDateTime, region: &Region) -> String {
    format!(
        "{date}/{region}/s3/aws4_request",
        date = now.format(DATE).expect("format timestamp using DATE"),
        region = region
    )
}

/// Generate the "string to sign" - the value to which the HMAC signing is
/// applied to sign requests.
fn string_to_sign(now: &OffsetDateTime, region: &Region, canonical_req: &str) -> String {
    let mut hasher = Sha256::default();
    hasher.update(canonical_req.as_bytes());
    let string_to = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hash}",
        timestamp = now
            .format(DATETIME)
            .expect("format timestamp using DATE-TIME"),
        scope = scope_string(now, region),
        hash = hex::encode(hasher.finalize().as_slice())
    );
    string_to
}

/// Generate the AWS signing key, derived from the secret key, date, region,
/// and service name.
fn signing_key(now: &OffsetDateTime, secret_key: &str, region: &Region, service: &str) -> Vec<u8> {
    let date = now.format(DATE).expect("format timestamp using DATE");
    let secret_key = format!("AWS4{}", secret_key);

    let mut hmac =
        Hmac::<Sha256>::new_from_slice(secret_key.as_bytes()).expect("HMAC-SHA256 from secret key");
    hmac.update(date.as_bytes());

    let mut hmac = Hmac::<Sha256>::new_from_slice(&hmac.finalize().into_bytes())
        .expect("HMAC-SHA256 from date key");
    hmac.update(region.to_string().as_bytes());

    let mut hmac = Hmac::<Sha256>::new_from_slice(&hmac.finalize().into_bytes())
        .expect("HMAC-SHA256 from region key");
    hmac.update(service.as_bytes());

    let mut hmac = Hmac::<Sha256>::new_from_slice(&hmac.finalize().into_bytes())
        .expect("HMAC-SHA256 from service key");
    hmac.update(b"aws4_request");

    hmac.finalize().into_bytes().to_vec()
}

const DATE: &[FormatItem<'static>] = format_description!("[year][month][day]");
const DATETIME: &[FormatItem<'static>] =
    format_description!("[year][month][day]T[hour][minute][second]Z");

const AMZ_CONTENT_SHA256: &'static str = "X-Amz-Content-Sha256";
const AMZ_DATE: &'static str = "X-Amz-Date";

const FRAGMENT: &AsciiSet = &CONTROLS
    // Reserved URL characters
    .add(b':')
    .add(b'?')
    .add(b'#')
    .add(b'[')
    .add(b']')
    .add(b'@')
    .add(b'!')
    .add(b'$')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b';')
    .add(b'=')
    // Unsafe URL characters
    .add(b'"')
    .add(b' ')
    .add(b'<')
    .add(b'>')
    .add(b'%')
    .add(b'{')
    .add(b'}')
    .add(b'|')
    .add(b'\\')
    .add(b'^')
    .add(b'`');

const FRAGMENT_SLASH: &AsciiSet = &FRAGMENT.add(b'/');
