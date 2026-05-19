use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;

use crate::kubectl::{ChildGuard, Kubectl};

pub fn run(
    kubectl: &Kubectl,
    grafana_port: u16,
    prometheus_port: u16,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    println!("==> Checking monitoring is running...");
    kubectl.run(&["get", "deployment", "grafana"])?;
    kubectl.run(&["get", "deployment", "prometheus"])?;

    kubectl.run(&[
        "wait", "--for=condition=available", "deployment/grafana", "--timeout=60s",
    ])?;
    kubectl.run(&[
        "wait", "--for=condition=available", "deployment/prometheus", "--timeout=60s",
    ])?;

    println!("==> Grafana    http://localhost:{}", grafana_port);
    println!("    Prometheus  http://localhost:{}", prometheus_port);
    println!("    Press Ctrl+C to stop");

    let prom_forward = format!("{}:9090", prometheus_port);
    let grafana_forward = format!("{}:3000", grafana_port);

    let prom_child = kubectl.spawn_inherit(&[
        "port-forward", "svc/prometheus", &prom_forward,
    ])?;
    let mut _prom_guard = ChildGuard::new(prom_child);

    let grafana_child = kubectl.spawn_inherit(&[
        "port-forward", "svc/grafana", &grafana_forward,
    ])?;
    let mut _grafana_guard = ChildGuard::new(grafana_child);

    // Wait for Ctrl+C
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Guards will kill children on drop
    println!("\n==> Stopping port-forwards...");
    Ok(())
}
