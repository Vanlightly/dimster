use anyhow::{Result, bail};

use crate::kubectl::Kubectl;

pub fn run(kubectl: &Kubectl) -> Result<()> {
    // Discover worker pods
    println!("==> Discovering worker pods...");
    let pod_output = kubectl.run(&[
        "get", "pods", "-l", "app=benchmark-worker",
        "--field-selector=status.phase=Running",
        "-o", "jsonpath={range .items[*]}{.metadata.name}{\" \"}{.status.podIP}{\"\\n\"}{end}",
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
                if response.trim() == "[]" {
                    println!("    No clients registered");
                } else {
                    print_health(&response);
                }
            }
            Err(e) => {
                println!("    Error querying health: {}", e);
            }
        }
    }

    Ok(())
}

fn print_health(json: &str) {
    // Parse as JSON array and print each entry
    let entries: Result<Vec<serde_json::Value>, _> = serde_json::from_str(json);
    match entries {
        Ok(clients) => {
            for client in &clients {
                let name = client["name"].as_str().unwrap_or("?");
                let client_type = client["clientType"].as_str().unwrap_or("?");
                let status = client["status"].as_str().unwrap_or("?");
                let error = client["error"].as_str();

                if let Some(err) = error {
                    println!("    {} ({}) — {} — {}", name, client_type, status, err);
                } else {
                    println!("    {} ({}) — {}", name, client_type, status);
                }
            }
        }
        Err(_) => {
            // Fallback: print raw response
            println!("    {}", json);
        }
    }
}
