use std::path::Path;
use std::process::Command;

use anyhow::{Result, bail};

pub fn run(files: &[String], output: Option<&str>, script_dir: &Path) -> Result<()> {
    if files.len() < 2 || files.len() > 4 {
        bail!("2-4 labeled files required: LABEL=FILE LABEL=FILE [LABEL=FILE] [LABEL=FILE]");
    }

    let entries: Vec<(&str, &str)> = files
        .iter()
        .map(|f| parse_labeled_file(f))
        .collect::<Result<Vec<_>>>()?;

    let output_dir = match output {
        Some(d) => d.to_string(),
        None => {
            let parent = Path::new(entries[0].1)
                .parent()
                .unwrap_or(Path::new("."));
            parent.to_string_lossy().to_string()
        }
    };

    std::fs::create_dir_all(&output_dir)?;

    let script = script_dir
        .join("charts/generate_compare_charts.py")
        .to_string_lossy()
        .to_string();

    let labels: Vec<&str> = entries.iter().map(|(l, _)| *l).collect();
    println!("==> Comparing {}...", labels.join(" vs "));

    let mut cmd = Command::new("python3");
    cmd.arg(&script);

    for (label, file) in &entries {
        cmd.args(["--entry", &format!("{}={}", label, file)]);
    }
    cmd.args(["--output", &output_dir]);

    let status = cmd.status()?;

    if !status.success() {
        bail!("Comparison chart generation failed");
    }

    println!("==> Comparison charts saved to {}/", output_dir);
    Ok(())
}

fn parse_labeled_file(arg: &str) -> Result<(&str, &str)> {
    let Some((label, file)) = arg.split_once('=') else {
        bail!("Invalid format '{}': expected LABEL=FILE (e.g. t1=results.json)", arg);
    };
    if label.is_empty() {
        bail!("Empty label in '{}'", arg);
    }
    if !Path::new(file).is_file() {
        bail!("File not found: {}", file);
    }
    Ok((label, file))
}
