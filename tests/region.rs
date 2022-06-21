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

use minio::s3::Region;

#[test]
fn custom_endpoint() {
    let regions = vec![
        ("localhost", Region::custom("localhost")),
        ("http://localhost", Region::custom("http://localhost")),
        (
            "http://localhost:9000",
            Region::custom("http://localhost:9000"),
        ),
        ("s3.example.com", Region::custom("s3.example.com")),
        (
            "https://s3.example.com",
            Region::custom("https://s3.example.com"),
        ),
        (
            "http://127.0.0.1:9000",
            Region::custom("http://127.0.0.1:9000"),
        ),
    ];
    for test in regions {
        assert_eq!(test.0, test.1.expect("").endpoint());
    }
}

#[test]
fn custom_host() {
    let regions = vec![
        ("localhost", Region::custom("localhost")),
        ("localhost", Region::custom("http://localhost")),
        ("localhost", Region::custom("http://localhost:9000")),
        ("s3.example.com", Region::custom("s3.example.com")),
        ("s3.example.com", Region::custom("https://s3.example.com")),
        ("127.0.0.1", Region::custom("http://127.0.0.1:9000")),
    ];
    for test in regions {
        assert_eq!(test.0, test.1.expect("").host());
    }
}
