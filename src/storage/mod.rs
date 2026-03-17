use anyhow::{Context, Result};
use s3::creds::Credentials;
use s3::region::Region;
use s3::{Bucket, BucketConfiguration};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::MinioConnectionConfig;

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct BucketInfo {
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<String>,
    pub is_prefix: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
}

// ============================================================================
// StorageClient
// ============================================================================

pub struct StorageClient {
    region: Region,
    credentials: Credentials,
    path_style: bool,
}

impl StorageClient {
    pub fn new(config: &MinioConnectionConfig) -> Result<Self> {
        let endpoint = if config.endpoint.contains("://") {
            format!("{}:{}", config.endpoint, config.port)
        } else {
            format!("http://{}:{}", config.endpoint, config.port)
        };

        let region = Region::Custom {
            region: config.region.clone(),
            endpoint,
        };

        let credentials = Credentials::new(
            Some(&config.access_key),
            Some(&config.secret_key),
            None,
            None,
            None,
        )
        .context("Failed to create S3 credentials")?;

        Ok(Self {
            region,
            credentials,
            path_style: config.path_style,
        })
    }

    fn bucket(&self, name: &str) -> Result<Box<Bucket>> {
        let mut bucket =
            Bucket::new(name, self.region.clone(), self.credentials.clone())
                .context("Failed to create S3 bucket handle")?;
        if self.path_style {
            bucket.set_path_style();
        }
        Ok(bucket)
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let response =
            Bucket::list_buckets(self.region.clone(), self.credentials.clone())
                .await
                .context("Failed to list buckets")?;
        let buckets = response
            .bucket_names()
            .into_iter()
            .map(|name| BucketInfo { name })
            .collect();
        Ok(buckets)
    }

    pub async fn create_bucket(&self, name: &str) -> Result<()> {
        let response = Bucket::create_with_path_style(
            name,
            self.region.clone(),
            self.credentials.clone(),
            BucketConfiguration::default(),
        )
        .await
        .context("Failed to create bucket")?;

        if response.response_code >= 300 {
            anyhow::bail!(
                "Failed to create bucket '{}': HTTP {}",
                name,
                response.response_code
            );
        }
        Ok(())
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        let bucket = self.bucket(name)?;
        let status = bucket.delete().await.context("Failed to delete bucket")?;
        if status >= 300 {
            anyhow::bail!(
                "Failed to delete bucket '{}': HTTP {}",
                name,
                status
            );
        }
        Ok(())
    }

    pub async fn list_objects(
        &self,
        bucket_name: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<Vec<ObjectInfo>> {
        let bucket = self.bucket(bucket_name)?;
        let prefix_str = prefix.unwrap_or("").to_string();
        let delimiter_str = delimiter.map(|d| d.to_string());

        let results = bucket
            .list(prefix_str, delimiter_str)
            .await
            .context("Failed to list objects")?;

        let mut objects = Vec::new();

        for result in &results {
            // Add common prefixes (folders)
            for cp in result.common_prefixes.as_deref().unwrap_or(&[]) {
                objects.push(ObjectInfo {
                    key: cp.prefix.clone(),
                    size: 0,
                    last_modified: None,
                    is_prefix: true,
                });
            }
            // Add objects
            for obj in &result.contents {
                objects.push(ObjectInfo {
                    key: obj.key.clone(),
                    size: obj.size,
                    last_modified: Some(obj.last_modified.clone()),
                    is_prefix: false,
                });
            }
        }

        Ok(objects)
    }

    pub async fn upload_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: &[u8],
        content_type: Option<&str>,
    ) -> Result<()> {
        let mut bucket = self.bucket(bucket_name)?;
        if let Some(ct) = content_type {
            bucket.add_header("Content-Type", ct);
        }
        let response = bucket
            .put_object(key, data)
            .await
            .context("Failed to upload object")?;
        if response.status_code() >= 300 {
            anyhow::bail!(
                "Failed to upload '{}': HTTP {}",
                key,
                response.status_code()
            );
        }
        Ok(())
    }

    pub async fn download_object(&self, bucket_name: &str, key: &str) -> Result<Vec<u8>> {
        let bucket = self.bucket(bucket_name)?;
        let response = bucket
            .get_object(key)
            .await
            .context("Failed to download object")?;
        if response.status_code() >= 300 {
            anyhow::bail!(
                "Failed to download '{}': HTTP {}",
                key,
                response.status_code()
            );
        }
        Ok(response.to_vec())
    }

    pub async fn delete_object(&self, bucket_name: &str, key: &str) -> Result<()> {
        let bucket = self.bucket(bucket_name)?;
        let response = bucket
            .delete_object(key)
            .await
            .context("Failed to delete object")?;
        if response.status_code() >= 300 {
            anyhow::bail!(
                "Failed to delete '{}': HTTP {}",
                key,
                response.status_code()
            );
        }
        Ok(())
    }

    pub async fn object_metadata(&self, bucket_name: &str, key: &str) -> Result<ObjectMeta> {
        let bucket = self.bucket(bucket_name)?;
        let (head, status) = bucket
            .head_object(key)
            .await
            .context("Failed to get object metadata")?;
        if status >= 300 {
            anyhow::bail!(
                "Failed to get metadata for '{}': HTTP {}",
                key,
                status
            );
        }
        Ok(ObjectMeta {
            key: key.to_string(),
            size: head.content_length.unwrap_or(0) as u64,
            content_type: head.content_type,
            last_modified: head.last_modified,
            etag: head.e_tag,
        })
    }

    pub async fn presign_get_url(&self, bucket_name: &str, key: &str, expiry_secs: u32) -> Result<String> {
        let bucket = self.bucket(bucket_name)?;
        let url = bucket
            .presign_get(key, expiry_secs, None)
            .await
            .context("Failed to generate presigned URL")?;
        Ok(url)
    }

    pub async fn health_check(&self) -> Result<()> {
        self.list_buckets()
            .await
            .map(|_| ())
            .context("Storage health check failed")
    }
}

// ============================================================================
// StorageRegistry
// ============================================================================

pub struct StorageRegistry {
    clients: RwLock<HashMap<String, Arc<StorageClient>>>,
}

impl StorageRegistry {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(&self, name: String, client: Arc<StorageClient>) {
        self.clients.write().await.insert(name, client);
    }

    pub async fn get(&self, name: &str) -> Option<Arc<StorageClient>> {
        self.clients.read().await.get(name).cloned()
    }

    pub async fn remove(&self, name: &str) {
        self.clients.write().await.remove(name);
    }

    pub async fn list_names(&self) -> Vec<String> {
        self.clients.read().await.keys().cloned().collect()
    }
}
