use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time;

#[derive(Clone, Copy, Debug)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

pub trait LogSink: Send + Sync {
    fn log(&self, level: LogLevel, message: &str);
}

pub struct JsonLogSink;

impl LogSink for JsonLogSink {
    fn log(&self, level: LogLevel, message: &str) {
        let lvl = match level {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        };
        println!(
            "{}",
            json!({
                "event": "log",
                "level": lvl,
                "message": message
            })
        );
    }
}

pub struct NullLogSink;

impl LogSink for NullLogSink {
    fn log(&self, _level: LogLevel, _message: &str) {}
}

pub trait ProgressReporter: Send + Sync {
    fn overall_status(&self, message: &str);
    fn overall_progress(&self, _done: u64, _total: u64) {}
    fn partition_started(&self, name: &str, total_ops: u64, total_bytes: Option<u64>);
    fn partition_progress(&self, name: &str, completed_ops: u64, total_ops: u64);
    fn partition_bytes(&self, name: &str, done: u64, total: u64);
    fn partition_finished(&self, name: &str);
    fn done(&self, message: &str);
    fn stage_started(&self, _stage: &str, _name: Option<&str>, _total: Option<u64>) {}
    fn stage_progress(&self, _stage: &str, _name: Option<&str>, _done: u64, _total: u64) {}
    fn stage_finished(&self, _stage: &str, _name: Option<&str>) {}
}

/// 以 JSON 行形式输出的进度回调，便于宿主进程解析。
pub struct JsonProgressReporter;

impl JsonProgressReporter {
    pub fn new() -> Self {
        Self
    }
}

impl ProgressReporter for JsonProgressReporter {
    fn overall_status(&self, message: &str) {
        println!(
            "{}",
            json!({
                "event": "overall_status",
                "message": message
            })
        );
    }

    fn overall_progress(&self, done: u64, total: u64) {
        println!(
            "{}",
            json!({
                "event": "overall_progress",
                "done": done,
                "total": total
            })
        );
    }

    fn partition_started(&self, name: &str, total_ops: u64, total_bytes: Option<u64>) {
        println!(
            "{}",
            json!({
                "event": "partition_started",
                "name": name,
                "total_ops": total_ops,
                "total_bytes": total_bytes
            })
        );
    }

    fn partition_progress(&self, name: &str, completed_ops: u64, total_ops: u64) {
        println!(
            "{}",
            json!({
                "event": "partition_progress",
                "name": name,
                "completed_ops": completed_ops,
                "total_ops": total_ops
            })
        );
    }

    fn partition_bytes(&self, name: &str, done: u64, total: u64) {
        println!(
            "{}",
            json!({
                "event": "partition_bytes",
                "name": name,
                "done": done,
                "total": total
            })
        );
    }

    fn partition_finished(&self, name: &str) {
        println!(
            "{}",
            json!({
                "event": "partition_finished",
                "name": name
            })
        );
    }

    fn done(&self, message: &str) {
        println!(
            "{}",
            json!({
                "event": "done",
                "message": message
            })
        );
    }

    fn stage_started(&self, stage: &str, name: Option<&str>, total: Option<u64>) {
        let mut obj = json!({
            "event": "stage",
            "stage": stage,
            "status": "start",
        });
        if let Some(n) = name {
            obj["name"] = json!(n);
        }
        if let Some(t) = total {
            obj["total"] = json!(t);
        }
        println!("{}", obj);
    }

    fn stage_progress(&self, stage: &str, name: Option<&str>, done: u64, total: u64) {
        let mut obj = json!({
            "event": "stage_progress",
            "stage": stage,
            "done": done,
            "total": total,
        });
        if let Some(n) = name {
            obj["name"] = json!(n);
        }
        println!("{}", obj);
    }

    fn stage_finished(&self, stage: &str, name: Option<&str>) {
        let mut obj = json!({
            "event": "stage",
            "stage": stage,
            "status": "finish",
        });
        if let Some(n) = name {
            obj["name"] = json!(n);
        }
        println!("{}", obj);
    }
}

pub struct NullProgressReporter;

impl ProgressReporter for NullProgressReporter {
    fn overall_status(&self, _message: &str) {}
    fn partition_started(&self, _name: &str, _total_ops: u64, _total_bytes: Option<u64>) {}
    fn partition_progress(&self, _name: &str, _completed_ops: u64, _total_ops: u64) {}
    fn partition_bytes(&self, _name: &str, _done: u64, _total: u64) {}
    fn partition_finished(&self, _name: &str) {}
    fn done(&self, _message: &str) {}
    fn stage_started(&self, _stage: &str, _name: Option<&str>, _total: Option<u64>) {}
    fn stage_progress(&self, _stage: &str, _name: Option<&str>, _done: u64, _total: u64) {}
    fn stage_finished(&self, _stage: &str, _name: Option<&str>) {}
}

/// CLI 适配器：将进度事件映射到 indicatif 进度条，便于复用同一接口。
pub struct CliProgressReporter {
    mp: Arc<MultiProgress>,
    main_pb: ProgressBar,
    partitions: Mutex<HashMap<String, ProgressBar>>,
}

impl CliProgressReporter {
    pub fn new() -> Self {
        let mp = Arc::new(MultiProgress::new());
        let main_pb = mp.add(ProgressBar::new_spinner());
        main_pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.blue} {msg}")
                .unwrap(),
        );
        main_pb.enable_steady_tick(time::Duration::from_millis(300));

        Self {
            mp,
            main_pb,
            partitions: Mutex::new(HashMap::new()),
        }
    }

    fn ensure_partition_bar(&self, name: &str) -> ProgressBar {
        let mut map = self.partitions.lock().unwrap();
        map.entry(name.to_string())
            .or_insert_with(|| {
                let pb = self.mp.add(ProgressBar::new(100));
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/white}] {percent}% - {msg}")
                        .unwrap()
                        .progress_chars("▰▱ "),
                );
                pb.enable_steady_tick(time::Duration::from_secs(1));
                pb
            })
            .clone()
    }
}

impl ProgressReporter for CliProgressReporter {
    fn overall_status(&self, message: &str) {
        self.main_pb.set_message(message.to_string());
    }

    fn overall_progress(&self, done: u64, total: u64) {
        if total == 0 {
            return;
        }
        let pct = ((done as f64 / total as f64) * 100.0).round() as u64;
        self.main_pb.set_message(format!(
            "{:>3}% {:.2}/{:.2} MiB",
            pct.min(100),
            done as f64 / 1024.0 / 1024.0,
            total as f64 / 1024.0 / 1024.0
        ));
    }

    fn partition_started(&self, name: &str, total_ops: u64, total_bytes: Option<u64>) {
        let pb = self.ensure_partition_bar(name);
        let msg = if let Some(total) = total_bytes {
            format!(
                "Processing {} ({} ops, {:.2} MiB)",
                name,
                total_ops,
                total as f64 / 1024.0 / 1024.0
            )
        } else {
            format!("Processing {} ({} ops)", name, total_ops)
        };
        pb.set_message(msg);
        pb.set_position(0);
    }

    fn partition_progress(&self, name: &str, completed_ops: u64, total_ops: u64) {
        if total_ops == 0 {
            return;
        }
        let pb = self.ensure_partition_bar(name);
        let pct = ((completed_ops as f64 / total_ops as f64) * 100.0).round() as u64;
        pb.set_position(pct.min(100));
    }

    fn partition_bytes(&self, name: &str, done: u64, total: u64) {
        if total == 0 {
            return;
        }
        let pb = self.ensure_partition_bar(name);
        let pct = ((done as f64 / total as f64) * 100.0).round() as u64;
        pb.set_position(pct.min(100));
        pb.set_message(format!(
            "{} ({:.2}/{:.2} MiB)",
            name,
            done as f64 / 1024.0 / 1024.0,
            total as f64 / 1024.0 / 1024.0
        ));
    }

    fn partition_finished(&self, name: &str) {
        let pb = self.ensure_partition_bar(name);
        pb.finish_with_message(format!("✓ {} done", name));
    }

    fn done(&self, message: &str) {
        self.main_pb.finish_with_message(message.to_string());
    }

    fn stage_started(&self, stage: &str, name: Option<&str>, total: Option<u64>) {
        let mut msg = format!("stage {} started", stage);
        if let Some(n) = name {
            msg = format!("{} ({})", msg, n);
        }
        if let Some(t) = total {
            msg = format!("{} total {:.2} MiB", msg, t as f64 / 1024.0 / 1024.0);
        }
        self.main_pb.set_message(msg);
    }

    fn stage_progress(&self, stage: &str, name: Option<&str>, done: u64, total: u64) {
        if total == 0 {
            return;
        }
        let pct = ((done as f64 / total as f64) * 100.0).round() as u64;
        let mut msg = format!("{} {:>3}%", stage, pct.min(100));
        if let Some(n) = name {
            msg = format!("{} {}", msg, n);
        }
        self.main_pb.set_message(msg);
    }

    fn stage_finished(&self, stage: &str, name: Option<&str>) {
        let mut msg = format!("stage {} finished", stage);
        if let Some(n) = name {
            msg = format!("{} ({})", msg, n);
        }
        self.main_pb.set_message(msg);
    }
}

/// CLI 日志适配器：后续可替换为全局日志记录器。
pub struct CliLogSink;

impl LogSink for CliLogSink {
    fn log(&self, level: LogLevel, message: &str) {
        match level {
            LogLevel::Error => eprintln!("[ERROR] {}", message),
            LogLevel::Warn => eprintln!("[WARN] {}", message),
            LogLevel::Info => println!("[INFO] {}", message),
            LogLevel::Debug => println!("[DEBUG] {}", message),
        }
    }
}

/// 包装器：在转发分区事件的同时计算全局进度（基于分区 size 估算）。
pub struct AggregatingProgress {
    inner: Arc<dyn ProgressReporter>,
    totals: HashMap<String, u64>,
    total_bytes: u64,
    done_bytes: Mutex<HashMap<String, u64>>,
}

impl AggregatingProgress {
    pub fn new(inner: Arc<dyn ProgressReporter>, totals: HashMap<String, u64>) -> Self {
        let total_bytes = totals.values().copied().sum();
        Self {
            inner,
            totals,
            total_bytes,
            done_bytes: Mutex::new(HashMap::new()),
        }
    }

    fn update_overall(&self, name: &str, done_for_partition: u64) {
        if self.total_bytes == 0 {
            return;
        }
        let done: u64 = {
            let mut map = self.done_bytes.lock().unwrap();
            let prev = *map.get(name).unwrap_or(&0);
            let updated = done_for_partition.max(prev);
            map.insert(name.to_string(), updated);
            map.values().copied().sum()
        };
        self.inner
            .overall_progress(done.min(self.total_bytes), self.total_bytes);
    }
}

impl ProgressReporter for AggregatingProgress {
    fn overall_status(&self, message: &str) {
        self.inner.overall_status(message);
    }

    fn overall_progress(&self, done: u64, total: u64) {
        self.inner.overall_progress(done, total);
    }

    fn partition_started(&self, name: &str, total_ops: u64, total_bytes: Option<u64>) {
        self.inner.partition_started(name, total_ops, total_bytes);
        if let Some(total) = self.totals.get(name) {
            self.update_overall(name, 0.min(*total));
        }
    }

    fn partition_progress(&self, name: &str, completed_ops: u64, total_ops: u64) {
        self.inner
            .partition_progress(name, completed_ops, total_ops);
        if let (Some(total), true) = (self.totals.get(name), total_ops > 0) {
            let done = ((*total as f64) * (completed_ops as f64 / total_ops as f64)) as u64;
            self.update_overall(name, done.min(*total));
        }
    }

    fn partition_bytes(&self, name: &str, done: u64, total: u64) {
        self.inner.partition_bytes(name, done, total);
        if let Some(part_total) = self.totals.get(name) {
            let est = if total > 0 {
                let base = (*part_total as f64) * (done as f64 / total as f64);
                // 预下载阶段下载的数据块通常远小于分区总大小，直接映射会让整体进度提前完成。
                // 当 total 远小于分区大小时，仅按 50% 权重计入整体进度，由后续解包/校验阶段补齐。
                let weight = if total < *part_total && total.saturating_mul(2) < *part_total {
                    0.5
                } else {
                    1.0
                };
                (base * weight) as u64
            } else {
                done.min(*part_total)
            };
            self.update_overall(name, est.min(*part_total));
        }
    }

    fn partition_finished(&self, name: &str) {
        self.inner.partition_finished(name);
        if let Some(total) = self.totals.get(name) {
            self.update_overall(name, *total);
        }
    }

    fn done(&self, message: &str) {
        self.inner.done(message);
    }

    fn stage_started(&self, stage: &str, name: Option<&str>, total: Option<u64>) {
        self.inner.stage_started(stage, name, total);
    }

    fn stage_progress(&self, stage: &str, name: Option<&str>, done: u64, total: u64) {
        self.inner.stage_progress(stage, name, done, total);
    }

    fn stage_finished(&self, stage: &str, name: Option<&str>) {
        self.inner.stage_finished(stage, name);
    }
}
