use anyhow::{Result, bail};

use crate::kubectl::Kubectl;

pub fn run(kubectl: &Kubectl, component: &str, follow: bool, previous: bool, tail: Option<u32>) -> Result<()> {
    let label = match component {
        "worker" | "workers" => "app=benchmark-worker",
        "coordinator" => "app=benchmark-coordinator",
        "kafka" => "app=kafka",
        other => bail!("Unknown component '{}'. Use: worker, coordinator, kafka", other),
    };

    // Find pods
    let pod_output = kubectl.run(&[
        "get", "pods", "-l", label,
        "--field-selector=status.phase=Running",
        "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
    ])?;

    let pods: Vec<&str> = pod_output
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim())
        .collect();

    if pods.is_empty() {
        bail!("No running {} pods found.", component);
    }

    let mut args = vec!["logs"];

    if pods.len() > 1 {
        args.push("-l");
        args.push(label);
        args.push("--prefix");
    } else {
        args.push(pods[0]);
    }

    if follow {
        args.push("-f");
    }

    if previous {
        args.push("-p");
    }

    let tail_str;
    if let Some(n) = tail {
        tail_str = format!("{}", n);
        args.push("--tail");
        args.push(&tail_str);
    }

    println!("==> Streaming logs for {} ({} pod(s))...", component, pods.len());
    kubectl.run_inherit(&args)?;

    Ok(())
}
