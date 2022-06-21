use async_std::task;
use minio::s3;

#[test]
fn create_bucket() {
    let region = s3::Region::custom_with_region("https://play.min.io:9000", "us-east-1").unwrap();

    let credentials = s3::Credentials::from_static(
        "Q3AM3UQ867SPQQA43P2F",
        "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    );

    let name = "my-bucket";
    let result = task::block_on(s3::Bucket::create(
        name,
        region.clone(),
        credentials.clone(),
        s3::bucket::Configuration::default(),
    ));
    let bucket = match result {
        Ok(bucket) => bucket,
        Err(err) if Some(s3::ErrorCode::BucketAlreadyOwnedByYou) == err.code() => {
            s3::Bucket::new(name, region, credentials)
        }
        Err(err) => panic!("{}", err),
    };
    //assert_eq!("https://play.min.io:9000", bucket.region().endpoint());
}

#[test]
fn get_object() {
    let region = s3::Region::custom_with_region("https://play.min.io:9000", "us-east-1").unwrap();

    let credentials = s3::Credentials::from_static(
        "Q3AM3UQ867SPQQA43P2F",
        "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    );
    let bucket = s3::Bucket::new("my-bucket", region, credentials);
    let mut object = task::block_on(bucket.get_object("test.file")).unwrap();

    task::block_on(async_std::io::copy(
        object.content_mut(),
        &mut async_std::io::stdout(),
    ))
    .unwrap();

    assert_eq!(
        s3::StorageClass::Standard,
        object.metadata().storage_class()
    );
}
