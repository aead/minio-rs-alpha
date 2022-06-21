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

use minio::s3::Etag;

#[test]
fn parse_etag() {
    let tests = vec![
        "00000000000000000000000000000000",
        "d41d8cd98f00b204e9800998ecf8427e",
        "d41d8cd98f00b204e9800998ecf8427e-1",
        "d41d8cd98f00b204e9800998ecf8427e-10000",
        "\"d41d8cd98f00b204e9800998ecf8427e\"",
        "\"d41d8cd98f00b204e9800998ecf8427e-72\"",
    ];

    for test in tests {
        let etag = test.parse::<Etag>();
        assert!(etag.is_ok(), "Failed to parse ETag: {}", test);
    }

    let tests = vec![
        "000000000000000000000000000000",
        "d41d8cd98f00b204e9800998ecf8427e-",
        "d41d8cd98f00b20 4e9800998ecf8427e",
        "d41d8cd98f00b204e9800998ecf8427e-10001",
        "\"d41d8cd98f00b204e9800998ecf8427e-72-1\"",
    ];

    for test in tests {
        let etag = test.parse::<Etag>();
        assert!(
            etag.is_err(),
            "Parse'd invalid ETag without any error: {}",
            test
        );
    }
}

#[test]
fn compute_blocking() {
    let tests = vec![
        ("", "d41d8cd98f00b204e9800998ecf8427e"),
        ("Hello World", "b10a8db164e0754105b7a99be72e3fe5"),
    ];

    for test in tests {
        let a = test.1.parse::<Etag>().unwrap();
        let b = Etag::compute_blocking(&mut test.0.as_bytes()).unwrap();
        assert!(
            a == b,
            "Computed ETag '{}' does not match expected ETag '{}'",
            a,
            b
        );
    }
}

#[test]
fn compute_from() {
    let tests = vec![
        ("", "d41d8cd98f00b204e9800998ecf8427e"),
        ("Hello World", "b10a8db164e0754105b7a99be72e3fe5"),
    ];

    for test in tests {
        let a = test.1.parse::<Etag>().unwrap();
        let b = Etag::compute_from(test.0.as_bytes());
        assert!(
            a == b,
            "Computed ETag '{}' does not match expected ETag '{}'",
            a,
            b
        );
    }
}
