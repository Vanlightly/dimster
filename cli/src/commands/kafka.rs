use anyhow::Result;

use crate::kubectl::Kubectl;

pub fn run(kubectl: &Kubectl, broker_id: u32, command: Option<Vec<String>>) -> Result<()> {
    let pod = format!("kafka-{}", broker_id);

    if let Some(cmd_parts) = command {
        let full_cmd = cmd_parts.join(" ");
        println!("==> Running on {}: {}", pod, full_cmd);
        let shell_cmd = format!("unset KAFKA_OPTS && /opt/kafka/bin/{}", full_cmd);
        kubectl.run_inherit(&["exec", &pod, "-c", "kafka", "--", "bash", "-c", &shell_cmd])?;
    } else {
        println!(
            "==> Opening shell on {} (KAFKA_OPTS unset, Kafka tools in /opt/kafka/bin/)",
            pod
        );
        kubectl.run_inherit(&[
            "exec",
            "-it",
            &pod,
            "-c",
            "kafka",
            "--",
            "bash",
            "-c",
            "unset KAFKA_OPTS && export PATH=\"/opt/kafka/bin:$PATH\" && exec bash",
        ])?;
    }
    Ok(())
}
