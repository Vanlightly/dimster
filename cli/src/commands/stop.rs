use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;

use crate::commands::find_coordinator;
use crate::kubectl::Kubectl;

pub fn run(kubectl: &Kubectl, stop: &AtomicBool) -> Result<()> {
    let coord_pod = find_coordinator(kubectl)?;

    println!("==> Sending stop to coordinator pod: {}", coord_pod);
    let response = kubectl
        .exec_curl(&coord_pod, "POST", "http://localhost:8080/stop", None)
        .unwrap_or_default();

    if !response.is_empty() {
        println!("    {}", response);
    } else {
        println!(
            "    Stop signal sent. The current workload will finish and results will be saved."
        );
    }

    println!("==> Waiting for benchmark to complete...");
    for _ in 0..120 {
        let status = kubectl
            .exec_curl(&coord_pod, "GET", "http://localhost:8080/status", None)
            .unwrap_or_else(|_| r#"{"state":"UNKNOWN"}"#.to_string());
        if status.contains("\"IDLE\"") || status.contains("\"FAILED\"") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
        if stop.load(Ordering::Relaxed) {
            println!("==> Interrupted. Benchmark is still stopping on the coordinator.");
            return Ok(());
        }
    }

    println!("==> Stop complete.");
    Ok(())
}
