use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use crate::args::Args;
use crate::payload::payload_dumper::{
    AsyncPayloadRead, PayloadReader, dump_partition, partitions_total_size,
};
use crate::progress::{LogLevel, LogSink, ProgressReporter};
use crate::readers::local_reader::LocalAsyncPayloadReader;
use crate::readers::remote_zip_reader::RemoteAsyncZipPayloadReader;
use crate::utils::format_elapsed_time;
use crate::verify::verify_partitions_hash;
use crate::{DeltaArchiveManifest, PartitionUpdate};

/// information about the data range needed for a partition
#[derive(Debug, Clone)]
pub struct PartitionDataRange {
    pub min_offset: u64,
    // pub max_offset: u64,
    pub total_bytes: u64,
    // pub operation_count: usize,
}

/// calculate the min/max data offsets for all operations in a partition
pub fn calculate_partition_range(
    partition: &PartitionUpdate,
    data_offset: u64,
) -> Option<PartitionDataRange> {
    let mut min_offset = u64::MAX;
    let mut max_offset = 0u64;
    let mut ops_with_data = 0;

    for op in &partition.operations {
        // only consider operations that actually read from payload data
        if let (Some(offset), Some(length)) = (op.data_offset, op.data_length) {
            if length > 0 {
                let abs_offset = data_offset + offset;
                let end_offset = abs_offset + length;

                min_offset = min_offset.min(abs_offset);
                max_offset = max_offset.max(end_offset);
                ops_with_data += 1;
            }
        }
    }

    if ops_with_data == 0 || min_offset == u64::MAX {
        return None;
    }

    Some(PartitionDataRange {
        min_offset,
        //  max_offset,
        total_bytes: max_offset - min_offset,
        //  operation_count: ops_with_data,
    })
}

async fn download_partition_data_to_path(
    zip_reader: &RemoteAsyncZipPayloadReader,
    range: &PartitionDataRange,
    temp_dir_path: &PathBuf,
    partition_name: &str,
    progress: &Arc<dyn ProgressReporter>,
    global_downloaded: &Arc<AtomicU64>,
    global_total: u64,
) -> Result<PathBuf> {
    progress.stage_started("download", Some(partition_name), Some(range.total_bytes));
    progress.partition_started(partition_name, 0, Some(range.total_bytes));

    let temp_path = temp_dir_path.join(format!("{}.prefetch", partition_name));
    let mut file = File::create(&temp_path).await?;

    // open a reader for downloading
    let mut reader = zip_reader.open_reader().await?;
    let mut stream = reader
        .read_range(range.min_offset, range.total_bytes)
        .await?;

    const BUFFER_SIZE: usize = 256 * 1024; // 256 KB buffer for reading
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut downloaded = 0u64;
    let total = range.total_bytes;

    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            break; // end of stream
        }

        // write what we read
        file.write_all(&buffer[..n]).await?;

        downloaded += n as u64;
        progress.partition_bytes(partition_name, downloaded, total);
        if global_total > 0 {
            let agg = global_downloaded.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
            let capped = agg.min(global_total);
            progress.stage_progress("download", None, capped, global_total);
        }
    }

    file.flush().await?;
    drop(file);

    progress.partition_finished(partition_name);
    progress.stage_finished("download", Some(partition_name));
    if global_total > 0 {
        let agg = global_downloaded.load(Ordering::Relaxed).min(global_total);
        progress.stage_progress("download", None, agg, global_total);
    }

    Ok(temp_path)
}

/// wrapper reader that translates offsets from absolute to relative
struct OffsetTranslatingReader {
    inner: LocalAsyncPayloadReader,
    base_offset: u64,
}

impl OffsetTranslatingReader {
    async fn new(path: PathBuf, base_offset: u64) -> Result<Self> {
        let inner = LocalAsyncPayloadReader::new(path).await?;
        Ok(Self { inner, base_offset })
    }
}

#[async_trait::async_trait]
impl AsyncPayloadRead for OffsetTranslatingReader {
    async fn open_reader(&self) -> Result<Box<dyn PayloadReader>> {
        // open the inner reader
        let inner_reader = self.inner.open_reader().await?;

        // wrap it with offset translation
        Ok(Box::new(OffsetTranslatingPayloadReader {
            inner: inner_reader,
            base_offset: self.base_offset,
        }))
    }
}

struct OffsetTranslatingPayloadReader {
    inner: Box<dyn PayloadReader>,
    base_offset: u64,
}

#[async_trait::async_trait]
impl PayloadReader for OffsetTranslatingPayloadReader {
    async fn read_range(
        &mut self,
        offset: u64,
        length: u64,
    ) -> Result<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send + '_>>> {
        // translate absolute offset to relative offset in temp file
        if offset < self.base_offset {
            return Err(anyhow!(
                "Offset {} is before base offset {}",
                offset,
                self.base_offset
            ));
        }

        let relative_offset = offset - self.base_offset;
        self.inner.read_range(relative_offset, length).await
    }
}

pub async fn prefetch_and_extract(
    url: String,
    manifest: DeltaArchiveManifest,
    data_offset: u64,
    args: Arc<Args>,
    partitions_to_extract: Vec<PartitionUpdate>,
    progress: Arc<dyn ProgressReporter>,
    logger: Arc<dyn LogSink>,
) -> Result<()> {
    let start_time = Instant::now();

    progress.overall_status("Initializing prefetch mode...");

    // mktmp
    let temp_dir = TempDir::new()?;
    logger.log(
        LogLevel::Info,
        &format!("Created temporary directory: {:?}", temp_dir.path()),
    );

    // calculate ranges for all partitions
    let mut partition_info: HashMap<String, PartitionDataRange> = HashMap::new();
    let mut total_download_size = 0u64;

    for partition in &partitions_to_extract {
        if let Some(range) = calculate_partition_range(partition, data_offset) {
            total_download_size += range.total_bytes;
            partition_info.insert(partition.partition_name.clone(), range);
        }
    }

    logger.log(
        LogLevel::Info,
        &format!(
            "Total data to download: {:.2} MB across {} partitions",
            total_download_size as f64 / 1024.0 / 1024.0,
            partition_info.len()
        ),
    );
    progress.stage_started("download", None, Some(total_download_size));

    let thread_count = if args.no_parallel {
        1
    } else if let Some(threads) = args.threads {
        threads
    } else {
        num_cpus::get()
    };

    // get block size for extraction
    let block_size = manifest.block_size.unwrap_or(4096) as u64;
    let total_process_size = partitions_total_size(partitions_to_extract.iter());

    // download and extract partitions as soon as each download completes
    progress.overall_status("Downloading and extracting partitions...");
    progress.stage_started("process", None, Some(total_process_size));

    let download_semaphore = Arc::new(Semaphore::new(thread_count));
    let extract_semaphore = Arc::new(Semaphore::new(thread_count));
    let mut combined_tasks = Vec::new();
    let global_downloaded = Arc::new(AtomicU64::new(0));
    let download_completed = Arc::new(AtomicU64::new(0));
    let download_done_flag = Arc::new(AtomicBool::new(false));
    let download_total_parts = partition_info.len() as u64;

    for partition in &partitions_to_extract {
        let partition_name = partition.partition_name.clone();

        if let Some(range) = partition_info.get(&partition_name) {
            let range = range.clone();
            let temp_dir_path = temp_dir.path().to_path_buf();
            let partition = partition.clone();
            let args = Arc::clone(&args);
            let progress = Arc::clone(&progress);
            let logger = Arc::clone(&logger);

            let url = url.clone();
            let user_agent = args.user_agent.clone();
            let download_semaphore = Arc::clone(&download_semaphore);
            let extract_semaphore = Arc::clone(&extract_semaphore);
            let global_downloaded = Arc::clone(&global_downloaded);
            let download_completed = Arc::clone(&download_completed);
            let download_done_flag = Arc::clone(&download_done_flag);

            // spawn combined download + extract task
            let task = tokio::spawn(async move {
                // download
                let temp_path = {
                    let _permit = download_semaphore.acquire().await.unwrap();

                    let zip_reader = RemoteAsyncZipPayloadReader::new(url, user_agent.as_deref())
                        .await
                        .map_err(|e| (partition_name.clone(), e))?;

                    let temp_path = download_partition_data_to_path(
                        &zip_reader,
                        &range,
                        &temp_dir_path,
                        &partition_name,
                        &progress,
                        &global_downloaded,
                        total_download_size,
                    )
                    .await
                    .map_err(|e| (partition_name.clone(), e))?;

                    // release download permit before extraction
                    drop(_permit);

                    let finished = download_completed.fetch_add(1, Ordering::Relaxed) + 1;
                    if finished == download_total_parts
                        && !download_done_flag.swap(true, Ordering::Relaxed)
                    {
                        let agg = if total_download_size > 0 {
                            global_downloaded
                                .load(Ordering::Relaxed)
                                .min(total_download_size)
                        } else {
                            0
                        };
                        progress.stage_finished("download", None);
                        if total_download_size > 0 {
                            progress.stage_progress("download", None, agg, total_download_size);
                        }
                    }

                    temp_path
                };

                // extract immediately after download completes
                let _permit = extract_semaphore.acquire().await.unwrap();

                let reader = OffsetTranslatingReader::new(temp_path, range.min_offset)
                    .await
                    .map(|r| Arc::new(r) as Arc<dyn AsyncPayloadRead>)
                    .map_err(|e| (partition_name.clone(), e))?;

                dump_partition(
                    &partition,
                    data_offset,
                    block_size,
                    &args,
                    &reader,
                    &progress,
                    &logger,
                )
                .await
                .map_err(|e| (partition_name, e))
            });

            combined_tasks.push(task);
        }
    }

    // wait for all download+extract tasks to complete
    let results = futures::future::join_all(combined_tasks).await;

    let mut failed_partitions = Vec::new();
    for result in results {
        match result {
            Ok(Ok(())) => {}
            Ok(Err((partition_name, error))) => {
                logger.log(
                    LogLevel::Error,
                    &format!("Failed to process partition {}: {}", partition_name, error),
                );
                failed_partitions.push(partition_name);
            }
            Err(e) => {
                logger.log(LogLevel::Error, &format!("Task panicked: {}", e));
            }
        }
    }

    progress.overall_status("Download & extract stage complete");
    progress.stage_finished("process", None);
    if !download_done_flag.swap(true, Ordering::Relaxed) {
        let agg = if total_download_size > 0 {
            global_downloaded
                .load(Ordering::Relaxed)
                .min(total_download_size)
        } else {
            0
        };
        progress.stage_finished("download", None);
        if total_download_size > 0 {
            progress.stage_progress("download", None, agg, total_download_size);
        }
    }

    if !args.no_verify {
        logger.log(LogLevel::Info, "Verifying partition hashes...");

        let partitions_to_verify: Vec<&PartitionUpdate> = partitions_to_extract
            .iter()
            .filter(|p| !failed_partitions.contains(&p.partition_name))
            .collect();

        match verify_partitions_hash(&partitions_to_verify, &args, &progress, &logger).await {
            Ok(failed_verifications) => {
                if !failed_verifications.is_empty() {
                    logger.log(
                        LogLevel::Warn,
                        &format!(
                            "Hash verification failed for {} partitions.",
                            failed_verifications.len()
                        ),
                    );
                    failed_partitions.extend(failed_verifications);
                }
            }
            Err(e) => {
                logger.log(
                    LogLevel::Error,
                    &format!("Error during hash verification: {}", e),
                );
            }
        }
    } else {
        logger.log(LogLevel::Info, "Skipping hash verification");
    }

    let elapsed_time = format_elapsed_time(start_time.elapsed());

    if failed_partitions.is_empty() {
        progress.done(&format!(
            "All partitions extracted successfully! (in {})",
            elapsed_time
        ));
    } else {
        progress.done(&format!(
            "Completed with {} failed partitions. (in {})",
            failed_partitions.len(),
            elapsed_time
        ));
    }

    Ok(())
}
