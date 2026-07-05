use anyhow::{Result, bail};
use serde::Deserialize;

use crate::kubectl::Kubectl;

pub fn run(kubectl: &Kubectl) -> Result<()> {
    // Discover worker pods
    println!("==> Discovering worker pods...");
    let pod_output = kubectl.run(&[
        "get",
        "pods",
        "-l",
        "app=benchmark-worker",
        "--field-selector=status.phase=Running",
        "-o",
        "jsonpath={range .items[*]}{.metadata.name}{\" \"}{.status.podIP}{\"\\n\"}{end}",
    ])?;

    let pods: Vec<(&str, &str)> = pod_output
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            Some((parts.next()?, parts.next()?))
        })
        .collect();

    if pods.is_empty() {
        bail!("No worker pods found. Run 'dimster deploy' first.");
    }
    println!("    Found {} worker(s)", pods.len());

    // Query /health on each worker
    for (pod_name, _ip) in &pods {
        println!("\n==> Worker: {}", pod_name);
        match kubectl.exec_curl(pod_name, "GET", "http://localhost:8080/health", None) {
            Ok(response) => {
                print_health(&response);
            }
            Err(e) => {
                println!("    Error querying health: {}", e);
            }
        }
    }

    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientHealth {
    producers: ClientCounts,
    consumers: ClientCounts,
    failed_clients: Vec<ClientStatus>,
    omitted_failed_clients: i64,
}

#[derive(Deserialize)]
struct ClientCounts {
    total: i64,
    running: i64,
    pending: i64,
    recreating: i64,
    failed: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientStatus {
    name: String,
    client_type: String,
    status: String,
    error: Option<String>,
}

fn print_health(json: &str) {
    let parsed: Result<ClientHealth, _> = serde_json::from_str(json);
    match parsed {
        Ok(health) => {
            if health.producers.total + health.consumers.total == 0 {
                println!("    No clients registered");
                return;
            }

            print_counts("Producers", &health.producers);
            print_counts("Consumers", &health.consumers);

            for client in &health.failed_clients {
                match &client.error {
                    Some(err) => println!(
                        "    FAILED: {} ({}) - {} - {}",
                        client.name, client.client_type, client.status, err
                    ),
                    None => println!(
                        "    FAILED: {} ({}) - {}",
                        client.name, client.client_type, client.status
                    ),
                }
            }

            if health.omitted_failed_clients > 0 {
                println!(
                    "    ... {} additional failed client(s) omitted",
                    health.omitted_failed_clients
                );
            }
        }
        Err(_) => {
            // Fallback: print raw response
            println!("    {}", json);
        }
    }
}

fn print_counts(label: &str, counts: &ClientCounts) {
    println!(
        "    {}: total={} running={} pending={} recreating={} failed={}",
        label, counts.total, counts.running, counts.pending, counts.recreating, counts.failed
    );
}
