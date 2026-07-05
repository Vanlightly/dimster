use std::path::Path;

use anyhow::{Result, bail};

use crate::kubectl::Kubectl;

pub fn run(kubectl: &Kubectl, output_dir: &str) -> Result<()> {
    // Find running Kafka pods
    let pod_output = kubectl.run(&[
        "get",
        "pods",
        "-l",
        "app=kafka",
        "--field-selector=status.phase=Running",
        "-o",
        "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
    ])?;

    let pods: Vec<&str> = pod_output
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim())
        .collect();

    if pods.is_empty() {
        bail!("No running Kafka pods found.");
    }

    println!("==> Found {} Kafka pod(s): {}", pods.len(), pods.join(", "));

    let mut total_files = 0;

    for pod in &pods {
        // Create per-pod output directory
        let pod_dir = format!("{}/{}", output_dir, pod);
        std::fs::create_dir_all(&pod_dir)?;

        // List log files on the pod
        let file_list = kubectl.run(&[
            "exec",
            pod,
            "--",
            "find",
            "/opt/kafka/logs",
            "-type",
            "f",
            "(",
            "-name",
            "*.log",
            "-o",
            "-name",
            "*.log.*",
            ")",
        ]);

        let files: Vec<&str> = match &file_list {
            Ok(output) => output
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| l.trim())
                .collect(),
            Err(e) => {
                eprintln!("    Warning: could not list log files on {}: {}", pod, e);
                continue;
            }
        };

        if files.is_empty() {
            println!("    {}: no log files found", pod);
            continue;
        }

        println!("    {}: downloading {} log file(s)...", pod, files.len());

        for remote_path in &files {
            let filename = Path::new(remote_path)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy();
            let local_path = format!("{}/{}", pod_dir, filename);

            if kubectl.copy_file_from_pod(pod, remote_path, &local_path) {
                total_files += 1;
            } else {
                eprintln!(
                    "    Warning: failed to download {} from {}",
                    remote_path, pod
                );
            }
        }
    }

    println!(
        "==> Downloaded {} log file(s) to {}/",
        total_files, output_dir
    );
    Ok(())
}
