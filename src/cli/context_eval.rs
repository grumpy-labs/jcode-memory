use crate::protocol::{ClioContextPacketItem, ClioContextPacketV1, ServerEvent};
use crate::server::Client;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct ContextEvalSuite {
    name: String,
    version: u32,
    #[serde(default = "default_minimum_pass_rate")]
    minimum_pass_rate: f64,
    #[serde(default)]
    required_groups: Vec<String>,
    probes: Vec<ContextEvalProbe>,
}

#[derive(Debug, Deserialize)]
struct ContextEvalProbe {
    name: String,
    group: String,
    #[serde(default)]
    description: String,
    #[serde(default = "default_probe_source")]
    source: ContextEvalProbeSource,
    #[serde(default)]
    packet: Option<ClioContextPacketV1>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default = "default_probe_limit")]
    limit: usize,
    #[serde(default)]
    include_provenance: bool,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    working_dir: Option<String>,
    #[serde(default)]
    assertions: Vec<ContextAssertion>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ContextEvalProbeSource {
    Fixture,
    LiveBroker,
}

impl ContextEvalProbeSource {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "fixture" => Ok(Self::Fixture),
            "live-broker" | "live_broker" => Ok(Self::LiveBroker),
            other => anyhow::bail!(
                "unknown context eval mode {other:?}; expected fixture or live-broker"
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fixture => "fixture",
            Self::LiveBroker => "live_broker",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContextAssertion {
    PacketVersion {
        equals: String,
    },
    SlotNonEmpty {
        slot: String,
    },
    SlotFieldContains {
        slot: String,
        field: String,
        contains: String,
    },
    SlotFieldEquals {
        slot: String,
        field: String,
        equals: Value,
    },
    SlotForbidsFieldContains {
        slot: String,
        field: String,
        contains: String,
    },
    SlotFieldMaxChars {
        slot: String,
        field: String,
        max_chars: usize,
    },
}

#[derive(Debug, Serialize)]
struct ContextEvalReport {
    suite: String,
    version: u32,
    suite_path: String,
    mode: String,
    timestamp: String,
    provider: ContextEvalProviderReport,
    passed: bool,
    pass_rate: f64,
    required_pass_rate: f64,
    probe_count: usize,
    passed_count: usize,
    failed_count: usize,
    group_results: BTreeMap<String, ContextEvalGroupReport>,
    probes: Vec<ContextEvalProbeReport>,
}

#[derive(Debug, Serialize)]
struct ContextEvalGroupReport {
    passed: bool,
    required: bool,
    pass_rate: f64,
    probe_count: usize,
    passed_count: usize,
    failed_count: usize,
}

#[derive(Debug, Serialize)]
struct ContextEvalProbeReport {
    name: String,
    group: String,
    description: String,
    source: String,
    passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ContextEvalProviderReport {
    kind: String,
    socket_path: Option<String>,
    working_dirs: Vec<String>,
    git_hash: String,
    version: String,
}

#[derive(Default)]
struct GroupAccumulator {
    probe_count: usize,
    passed_count: usize,
}

fn default_minimum_pass_rate() -> f64 {
    1.0
}

fn default_probe_source() -> ContextEvalProbeSource {
    ContextEvalProbeSource::Fixture
}

fn default_probe_limit() -> usize {
    8
}

pub(crate) async fn run_context_eval(
    suite: String,
    suite_path: Option<String>,
    json: bool,
    mode: String,
    log: Option<String>,
    no_log: bool,
) -> Result<()> {
    let mode = ContextEvalProbeSource::parse(&mode)?;
    let suite_path = resolve_suite_path(&suite, suite_path.as_deref())?;
    let suite = load_suite(&suite_path)?;
    let report = evaluate_suite(suite, &suite_path, mode).await?;

    if !no_log {
        append_run_log(&report, log.as_deref())?;
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human_report(&report);
    }

    if report.passed {
        Ok(())
    } else {
        anyhow::bail!(
            "context eval suite {} failed: {}/{} probes passed",
            report.suite,
            report.passed_count,
            report.probe_count
        );
    }
}

fn resolve_suite_path(suite: &str, suite_path: Option<&str>) -> Result<PathBuf> {
    if let Some(path) = suite_path.map(str::trim).filter(|path| !path.is_empty()) {
        return Ok(PathBuf::from(path));
    }

    let relative = Path::new("tests")
        .join("fixtures")
        .join("context_evals")
        .join(suite)
        .join("suite.json");
    let cwd_candidate = std::env::current_dir()?.join(&relative);
    if cwd_candidate.is_file() {
        return Ok(cwd_candidate);
    }

    let manifest_candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    Ok(manifest_candidate)
}

fn load_suite(path: &Path) -> Result<ContextEvalSuite> {
    let data = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read context eval suite {}", path.display()))?;
    serde_json::from_str(&data)
        .with_context(|| format!("failed to parse context eval suite {}", path.display()))
}

async fn evaluate_suite(
    suite: ContextEvalSuite,
    suite_path: &Path,
    mode: ContextEvalProbeSource,
) -> Result<ContextEvalReport> {
    if suite.probes.is_empty() {
        anyhow::bail!("context eval suite {} has no probes", suite.name);
    }

    let selected_probes: Vec<_> = suite
        .probes
        .into_iter()
        .filter(|probe| probe.source == mode)
        .collect();
    if selected_probes.is_empty() {
        anyhow::bail!(
            "context eval suite {} has no probes for mode {}",
            suite.name,
            mode.as_str()
        );
    }

    let required_groups = suite
        .required_groups
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut probe_reports = Vec::with_capacity(selected_probes.len());
    let mut groups: BTreeMap<String, GroupAccumulator> = BTreeMap::new();
    let mut working_dirs = std::collections::BTreeSet::new();

    for probe in selected_probes {
        if let Some(working_dir) = &probe.working_dir {
            working_dirs.insert(working_dir.clone());
        }
        let failures = evaluate_probe(&probe, mode).await?;
        let passed = failures.is_empty();
        let group = groups.entry(probe.group.clone()).or_default();
        group.probe_count += 1;
        if passed {
            group.passed_count += 1;
        }
        probe_reports.push(ContextEvalProbeReport {
            name: probe.name,
            group: probe.group,
            description: probe.description,
            source: probe.source.as_str().to_string(),
            passed,
            failures,
        });
    }

    let probe_count = probe_reports.len();
    let passed_count = probe_reports.iter().filter(|probe| probe.passed).count();
    let failed_count = probe_count.saturating_sub(passed_count);
    let pass_rate = passed_count as f64 / probe_count as f64;
    let mut group_results = BTreeMap::new();

    for (name, group) in groups {
        let failed_count = group.probe_count.saturating_sub(group.passed_count);
        let pass_rate = if group.probe_count == 0 {
            0.0
        } else {
            group.passed_count as f64 / group.probe_count as f64
        };
        group_results.insert(
            name.clone(),
            ContextEvalGroupReport {
                passed: failed_count == 0,
                required: required_groups.contains(&name),
                pass_rate,
                probe_count: group.probe_count,
                passed_count: group.passed_count,
                failed_count,
            },
        );
    }

    let required_groups_passed = required_groups.iter().all(|group| {
        group_results
            .get(group)
            .map(|result| result.passed)
            .unwrap_or(false)
    });
    let passed = pass_rate >= suite.minimum_pass_rate && required_groups_passed;

    Ok(ContextEvalReport {
        suite: suite.name,
        version: suite.version,
        suite_path: suite_path.display().to_string(),
        mode: mode.as_str().to_string(),
        timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        provider: ContextEvalProviderReport {
            kind: mode.as_str().to_string(),
            socket_path: (mode == ContextEvalProbeSource::LiveBroker)
                .then(|| crate::server::socket_path().display().to_string()),
            working_dirs: working_dirs.into_iter().collect(),
            git_hash: env!("JCODE_GIT_HASH").to_string(),
            version: env!("JCODE_VERSION").to_string(),
        },
        passed,
        pass_rate,
        required_pass_rate: suite.minimum_pass_rate,
        probe_count,
        passed_count,
        failed_count,
        group_results,
        probes: probe_reports,
    })
}

async fn evaluate_probe(
    probe: &ContextEvalProbe,
    mode: ContextEvalProbeSource,
) -> Result<Vec<String>> {
    let packet = packet_for_probe(probe, mode).await?;
    let mut failures = Vec::new();
    for assertion in &probe.assertions {
        if let Err(error) = evaluate_assertion(&packet, assertion) {
            failures.push(error);
        }
    }
    Ok(failures)
}

async fn packet_for_probe(
    probe: &ContextEvalProbe,
    mode: ContextEvalProbeSource,
) -> Result<ClioContextPacketV1> {
    match mode {
        ContextEvalProbeSource::Fixture => probe
            .packet
            .clone()
            .with_context(|| format!("fixture probe {} is missing packet", probe.name)),
        ContextEvalProbeSource::LiveBroker => fetch_live_broker_packet(probe).await,
    }
}

async fn fetch_live_broker_packet(probe: &ContextEvalProbe) -> Result<ClioContextPacketV1> {
    let query = probe
        .query
        .as_ref()
        .map(|query| query.trim())
        .filter(|query| !query.is_empty())
        .with_context(|| format!("live broker probe {} is missing query", probe.name))?;
    let mut client = Client::connect().await.with_context(|| {
        format!(
            "failed to connect to broker socket {}",
            crate::server::socket_path().display()
        )
    })?;
    if probe.working_dir.is_some() || probe.session_id.is_some() {
        client
            .subscribe_with_info(
                probe.working_dir.clone(),
                None,
                probe.session_id.clone(),
                false,
                false,
            )
            .await
            .with_context(|| format!("failed to subscribe live broker probe {}", probe.name))?;
    }
    let event = client
        .get_broker_context_with_options(
            probe.session_id.clone(),
            Some(query.to_string()),
            probe.limit,
            probe.include_provenance,
        )
        .await
        .with_context(|| format!("failed live broker context probe {}", probe.name))?;
    match event {
        ServerEvent::BrokerContext {
            packet: Some(packet),
            ..
        } => Ok(packet),
        ServerEvent::BrokerContext { .. } => {
            anyhow::bail!("live broker probe {} returned no packet", probe.name)
        }
        ServerEvent::Error { message, .. } => {
            anyhow::bail!(
                "live broker probe {} returned error: {}",
                probe.name,
                message
            )
        }
        other => anyhow::bail!(
            "live broker probe {} returned unexpected event {:?}",
            probe.name,
            other
        ),
    }
}

fn evaluate_assertion(
    packet: &ClioContextPacketV1,
    assertion: &ContextAssertion,
) -> Result<(), String> {
    match assertion {
        ContextAssertion::PacketVersion { equals } => {
            if &packet.version == equals {
                Ok(())
            } else {
                Err(format!(
                    "packet version mismatch: expected {equals:?}, got {:?}",
                    packet.version
                ))
            }
        }
        ContextAssertion::SlotNonEmpty { slot } => {
            let items = slot_items(packet, slot)?;
            if items.is_empty() {
                Err(format!("slot {slot:?} is empty"))
            } else {
                Ok(())
            }
        }
        ContextAssertion::SlotFieldContains {
            slot,
            field,
            contains,
        } => {
            let items = slot_items(packet, slot)?;
            if items.iter().any(|item| {
                item_field_text(item, field).is_some_and(|text| text.contains(contains))
            }) {
                Ok(())
            } else {
                Err(format!(
                    "no item in slot {slot:?} has field {field:?} containing {contains:?}"
                ))
            }
        }
        ContextAssertion::SlotFieldEquals {
            slot,
            field,
            equals,
        } => {
            let items = slot_items(packet, slot)?;
            if items
                .iter()
                .any(|item| item_field_value(item, field).is_some_and(|value| value == *equals))
            {
                Ok(())
            } else {
                Err(format!(
                    "no item in slot {slot:?} has field {field:?} equal to {equals}"
                ))
            }
        }
        ContextAssertion::SlotForbidsFieldContains {
            slot,
            field,
            contains,
        } => {
            let items = slot_items(packet, slot)?;
            if items.iter().any(|item| {
                item_field_text(item, field).is_some_and(|text| text.contains(contains))
            }) {
                Err(format!(
                    "slot {slot:?} has forbidden field {field:?} text containing {contains:?}"
                ))
            } else {
                Ok(())
            }
        }
        ContextAssertion::SlotFieldMaxChars {
            slot,
            field,
            max_chars,
        } => {
            let items = slot_items(packet, slot)?;
            if items.is_empty() {
                return Err(format!("slot {slot:?} is empty"));
            }
            let oversized = items.iter().find_map(|item| {
                item_field_text(item, field).and_then(|text| {
                    let count = text.chars().count();
                    (count > *max_chars).then_some((item.item.id.clone(), count))
                })
            });
            if let Some((id, count)) = oversized {
                Err(format!(
                    "item {id:?} field {field:?} is {count} chars, over max {max_chars}"
                ))
            } else {
                Ok(())
            }
        }
    }
}

fn slot_items<'a>(
    packet: &'a ClioContextPacketV1,
    slot: &str,
) -> Result<&'a [ClioContextPacketItem], String> {
    match slot {
        "active_task" => Ok(&packet.active_task),
        "authority" => Ok(&packet.authority),
        "lineage" => Ok(&packet.lineage),
        "vault_evidence" => Ok(&packet.vault_evidence),
        "durable_memory" => Ok(&packet.durable_memory),
        "session_evidence" => Ok(&packet.session_evidence),
        "artifact_refs" => Ok(&packet.artifact_refs),
        "conflicts" => Ok(&packet.conflicts),
        "skill_hints" => Ok(&packet.skill_hints),
        "tool_hints" => Ok(&packet.tool_hints),
        _ => Err(format!("unknown context packet slot {slot:?}")),
    }
}

fn item_field_value(item: &ClioContextPacketItem, field: &str) -> Option<Value> {
    let value = serde_json::to_value(item).ok()?;
    get_dotted_value(&value, field).cloned()
}

fn item_field_text(item: &ClioContextPacketItem, field: &str) -> Option<String> {
    let value = item_field_value(item, field)?;
    match value {
        Value::Null => None,
        Value::String(value) => Some(value),
        other => Some(other.to_string()),
    }
}

fn get_dotted_value<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    field
        .split('.')
        .try_fold(value, |current, segment| current.get(segment))
}

fn append_run_log(report: &ContextEvalReport, log: Option<&str>) -> Result<()> {
    let log_path = match log.map(str::trim).filter(|value| !value.is_empty()) {
        Some(log) => PathBuf::from(log),
        None => default_log_path(&report.suite)?,
    };
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create context eval log directory {}",
                parent.display()
            )
        })?;
    }
    let mut line = serde_json::to_string(report)?;
    line.push('\n');
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("failed to open context eval log {}", log_path.display()))?
        .write_all(line.as_bytes())
        .with_context(|| format!("failed to write context eval log {}", log_path.display()))
}

fn default_log_path(suite: &str) -> Result<PathBuf> {
    let home = dirs::home_dir().context("failed to resolve home directory for eval log")?;
    Ok(home
        .join(".local")
        .join("state")
        .join("clio")
        .join("context-evals")
        .join(suite)
        .join("runs.jsonl"))
}

fn print_human_report(report: &ContextEvalReport) {
    println!(
        "context eval {} [{}]: {} ({}/{} probes, pass_rate={:.2}, required={:.2})",
        report.suite,
        report.mode,
        if report.passed { "passed" } else { "failed" },
        report.passed_count,
        report.probe_count,
        report.pass_rate,
        report.required_pass_rate
    );
    for probe in &report.probes {
        println!(
            "- {} [{}:{}]: {}",
            probe.name,
            probe.source,
            probe.group,
            if probe.passed { "passed" } else { "failed" }
        );
        for failure in &probe.failures {
            println!("  - {failure}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dotted_value_reads_nested_metadata() {
        let value = json!({"metadata": {"logical_super_session_id": "clio-super-session"}});
        assert_eq!(
            get_dotted_value(&value, "metadata.logical_super_session_id"),
            Some(&json!("clio-super-session"))
        );
    }

    #[test]
    fn context_eval_mode_accepts_hyphen_or_underscore() {
        assert_eq!(
            ContextEvalProbeSource::parse("live-broker").unwrap(),
            ContextEvalProbeSource::LiveBroker
        );
        assert_eq!(
            ContextEvalProbeSource::parse("live_broker").unwrap(),
            ContextEvalProbeSource::LiveBroker
        );
    }
}
