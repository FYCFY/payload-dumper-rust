use crate::PartitionUpdate;
use crate::args::Args;
use crate::payload::payload_dumper::partitions_total_size;
use crate::progress::{LogLevel, LogSink, ProgressReporter};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer

pub async fn verify_partitions_hash(
    partitions: &[&PartitionUpdate],
    args: &Args,
    progress: &Arc<dyn ProgressReporter>,
    logger: &Arc<dyn LogSink>,
) -> Result<Vec<String>> {
    if args.no_verify {
        return Ok(vec![]);
    }

    let out_dir = &args.out;
    let mut failed_verifications = Vec::new();
    let total_size = partitions_total_size(partitions.iter().copied());

    progress.overall_status(&format!(
        "Verifying hashes for {} partitions",
        partitions.len()
    ));
    progress.stage_started("verify", None, Some(total_size));

    // Process partitions in parallel
    let tasks: Vec<_> = partitions
        .iter()
        .map(|partition| {
            let partition = (*partition).clone();
            let out_dir = out_dir.clone();
            let progress = Arc::clone(progress);
            let logger = Arc::clone(logger);

            tokio::spawn(async move {
                let partition_name = partition.partition_name.clone();
                let out_path = out_dir.join(format!("{}.img", partition_name));

                let expected_hash = partition
                    .new_partition_info
                    .as_ref()
                    .and_then(|info| info.hash.as_ref());

                progress.partition_started(&partition_name, 0, None);
                progress.partition_progress(&partition_name, 0, 1);

                let result = verify_partition_hash(
                    &partition_name,
                    &out_path,
                    expected_hash,
                    &progress,
                    &logger,
                )
                .await;

                (partition_name, result)
            })
        })
        .collect();

    // Wait for all verification tasks
    let results = futures::future::join_all(tasks).await;

    for result in results {
        match result {
            Ok((partition_name, Ok(true))) => {
                progress.partition_finished(&partition_name);
            }
            Ok((partition_name, Ok(false))) => {
                progress.partition_finished(&partition_name);
                failed_verifications.push(partition_name);
            }
            Ok((partition_name, Err(e))) => {
                logger.log(
                    LogLevel::Error,
                    &format!("Error verifying hash for {}: {}", partition_name, e),
                );
                failed_verifications.push(partition_name);
            }
            Err(e) => {
                logger.log(
                    LogLevel::Error,
                    &format!("Verification task panicked: {}", e),
                );
            }
        }
    }

    progress.stage_progress("verify", None, total_size, total_size);
    progress.stage_finished("verify", None);
    progress.overall_status("Hash verification completed");

    Ok(failed_verifications)
}

async fn verify_partition_hash(
    partition_name: &str,
    out_path: &PathBuf,
    expected_hash: Option<&Vec<u8>>,
    progress: &Arc<dyn ProgressReporter>,
    logger: &Arc<dyn LogSink>,
) -> Result<bool> {
    let Some(expected) = expected_hash else {
        progress.partition_finished(partition_name);
        return Ok(true);
    };

    if expected.is_empty() {
        progress.partition_finished(partition_name);
        return Ok(true);
    }

    let mut file = File::open(out_path)
        .await
        .with_context(|| format!("Failed to open {} for hash verification", partition_name))?;

    let file_size = file.metadata().await.map(|m| m.len()).unwrap_or(0);

    progress.stage_started("verify", Some(partition_name), Some(file_size));
    let result = async {
        // Hash the file
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let mut processed = 0u64;

        progress.partition_started(partition_name, 0, Some(file_size));

        loop {
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
            processed += bytes_read as u64;
            progress.partition_bytes(partition_name, processed, file_size);
        }

        let hash = hasher.finalize().to_vec();
        let matches = hash.as_slice() == expected.as_slice();

        if matches {
            progress.partition_finished(partition_name);
        } else {
            progress.partition_finished(partition_name);
            logger.log(
                LogLevel::Warn,
                &format!("{} hash mismatch: expected {:x?}", partition_name, expected),
            );
        }

        Ok::<bool, anyhow::Error>(matches)
    }
    .await;

    progress.stage_finished("verify", Some(partition_name));
    result
}
