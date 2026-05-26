use crate::test_support::*;

#[tokio::test]
async fn broker_eval_context_runs_locked_clio_super_session_suite() -> Result<()> {
    use std::process::Command;

    let _env = setup_test_env()?;
    let output = Command::new(env!("CARGO_BIN_EXE_jcode"))
        .args([
            "broker",
            "eval-context",
            "--suite",
            "clio-super-session-v1",
            "--json",
            "--no-log",
        ])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "eval-context should pass. stdout: {stdout}, stderr: {stderr}"
    );

    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["suite"], "clio-super-session-v1");
    assert_eq!(report["passed"], true);
    assert_eq!(report["probe_count"], 14);
    assert_eq!(report["failed_count"], 0);
    assert_eq!(
        report["group_results"]["authority_conflict"]["passed"],
        true
    );
    assert!(
        report["duration_ms"].as_u64().is_some(),
        "suite report should include duration_ms: {report}"
    );
    let probes = report["probes"]
        .as_array()
        .expect("probes should be an array");
    let first_probe = probes
        .first()
        .expect("suite should report at least one probe");
    assert!(
        first_probe["duration_ms"].as_u64().is_some(),
        "probe report should include duration_ms: {first_probe}"
    );
    assert!(
        first_probe["packet_budget"]["total_items"]
            .as_u64()
            .is_some(),
        "probe report should include packet_budget.total_items: {first_probe}"
    );
    assert!(
        first_probe["packet_budget"]["serialized_chars"]
            .as_u64()
            .is_some(),
        "probe report should include packet_budget.serialized_chars: {first_probe}"
    );
    assert!(
        first_probe["packet_budget"]["slot_item_counts"]
            .as_object()
            .is_some(),
        "probe report should include packet_budget.slot_item_counts: {first_probe}"
    );

    Ok(())
}
