use crate::protocol::{ClioContextPacketItem, ClioContextPacketV1, ServerEvent};
use crate::server::Client;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

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
    #[serde(default)]
    tool_query: Option<String>,
    #[serde(default = "default_probe_limit")]
    limit: usize,
    #[serde(default)]
    include_provenance: bool,
    #[serde(default)]
    expect_no_prefetch: bool,
    #[serde(default)]
    max_duration_ms: Option<u64>,
    #[serde(default)]
    packet_budget_limits: ContextPacketBudgetLimits,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    working_dir: Option<String>,
    #[serde(default)]
    sync_user: Option<String>,
    #[serde(default)]
    sync_assistant: Option<String>,
    #[serde(default)]
    transcript_user: Option<String>,
    #[serde(default)]
    transcript_assistant: Option<String>,
    #[serde(default)]
    assertions: Vec<ContextAssertion>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ContextEvalProbeSource {
    Fixture,
    LiveBroker,
    InstalledProvider,
}

impl ContextEvalProbeSource {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "fixture" => Ok(Self::Fixture),
            "live-broker" | "live_broker" => Ok(Self::LiveBroker),
            "installed-provider" | "installed_provider" => Ok(Self::InstalledProvider),
            other => anyhow::bail!(
                "unknown context eval mode {other:?}; expected fixture, live-broker, or installed-provider"
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fixture => "fixture",
            Self::LiveBroker => "live_broker",
            Self::InstalledProvider => "installed_provider",
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
    SlotFirstFieldContains {
        slot: String,
        field: String,
        contains: String,
    },
    SlotFieldEquals {
        slot: String,
        field: String,
        equals: Value,
    },
    SlotFirstFieldEquals {
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
    JsonFieldEquals {
        field: String,
        equals: Value,
    },
    JsonFieldContains {
        field: String,
        contains: String,
    },
    JsonFieldForbidsContains {
        field: String,
        contains: String,
    },
    JsonFieldMin {
        field: String,
        min: f64,
    },
}

#[derive(Debug, Serialize)]
struct ContextEvalReport {
    suite: String,
    version: u32,
    suite_path: String,
    mode: String,
    timestamp: String,
    duration_ms: u64,
    provider: ContextEvalProviderReport,
    review: ContextEvalReviewReport,
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
struct ContextEvalReviewReport {
    recommendation: String,
    failure_class_counts: BTreeMap<String, usize>,
    top_failures: Vec<ContextEvalReviewFailure>,
}

#[derive(Debug, Serialize)]
struct ContextEvalReviewFailure {
    probe: String,
    group: String,
    classification: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct ContextEvalProbeReport {
    name: String,
    group: String,
    description: String,
    source: String,
    duration_ms: u64,
    packet_budget: ContextPacketBudgetReport,
    passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ContextPacketBudgetReport {
    total_items: usize,
    serialized_chars: usize,
    slot_item_counts: BTreeMap<String, usize>,
    slot_serialized_chars: BTreeMap<String, usize>,
}

#[derive(Debug, Default, Deserialize)]
struct ContextPacketBudgetLimits {
    #[serde(default)]
    max_total_items: Option<usize>,
    #[serde(default)]
    max_serialized_chars: Option<usize>,
    #[serde(default)]
    max_slot_items: BTreeMap<String, usize>,
    #[serde(default)]
    max_slot_serialized_chars: BTreeMap<String, usize>,
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
    let suite_started = Instant::now();
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
        let probe_started = Instant::now();
        let evaluation = evaluate_probe(&probe, mode).await?;
        let duration_ms = elapsed_millis(probe_started);
        let mut failures = evaluation.failures;
        failures.extend(duration_failures(
            &probe.name,
            probe.max_duration_ms,
            duration_ms,
        ));
        failures.extend(budget_failures(
            &probe.name,
            &probe.packet_budget_limits,
            &evaluation.packet_budget,
        ));
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
            duration_ms,
            packet_budget: evaluation.packet_budget,
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
    let review = review_summary(passed, &group_results, &probe_reports);

    Ok(ContextEvalReport {
        suite: suite.name,
        version: suite.version,
        suite_path: suite_path.display().to_string(),
        mode: mode.as_str().to_string(),
        timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        duration_ms: elapsed_millis(suite_started),
        provider: ContextEvalProviderReport {
            kind: mode.as_str().to_string(),
            socket_path: match mode {
                ContextEvalProbeSource::Fixture => None,
                ContextEvalProbeSource::LiveBroker => {
                    Some(crate::server::socket_path().display().to_string())
                }
                ContextEvalProbeSource::InstalledProvider => {
                    std::env::var("JCODE_BROKER_SOCKET").ok()
                }
            },
            working_dirs: working_dirs.into_iter().collect(),
            git_hash: env!("JCODE_GIT_HASH").to_string(),
            version: env!("JCODE_VERSION").to_string(),
        },
        review,
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

struct ContextProbeEvaluation {
    failures: Vec<String>,
    packet_budget: ContextPacketBudgetReport,
}

enum ContextProbeOutput {
    Packet(ClioContextPacketV1),
    Json(Value),
}

async fn evaluate_probe(
    probe: &ContextEvalProbe,
    mode: ContextEvalProbeSource,
) -> Result<ContextProbeEvaluation> {
    let output = output_for_probe(probe, mode).await?;
    let packet_budget = output_budget_report(&output)?;
    let mut failures = Vec::new();
    for assertion in &probe.assertions {
        if let Err(error) = evaluate_assertion(&output, assertion) {
            failures.push(error);
        }
    }
    Ok(ContextProbeEvaluation {
        failures,
        packet_budget,
    })
}

fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn duration_failures(name: &str, max_duration_ms: Option<u64>, duration_ms: u64) -> Vec<String> {
    let Some(max_duration_ms) = max_duration_ms else {
        return Vec::new();
    };
    if duration_ms > max_duration_ms {
        vec![format!(
            "probe {name:?} took {duration_ms}ms, over max_duration_ms {max_duration_ms}"
        )]
    } else {
        Vec::new()
    }
}

fn budget_failures(
    name: &str,
    limits: &ContextPacketBudgetLimits,
    report: &ContextPacketBudgetReport,
) -> Vec<String> {
    let mut failures = Vec::new();
    if let Some(max_total_items) = limits.max_total_items {
        if report.total_items > max_total_items {
            failures.push(format!(
                "probe {name:?} packet has {} items, over max_total_items {max_total_items}",
                report.total_items
            ));
        }
    }
    if let Some(max_serialized_chars) = limits.max_serialized_chars {
        if report.serialized_chars > max_serialized_chars {
            failures.push(format!(
                "probe {name:?} packet serialized {} chars, over max_serialized_chars {max_serialized_chars}",
                report.serialized_chars
            ));
        }
    }
    for (slot, max_slot_items) in &limits.max_slot_items {
        match report.slot_item_counts.get(slot) {
            Some(count) if count > max_slot_items => failures.push(format!(
                "probe {name:?} slot {slot:?} has {count} items, over max_slot_items {max_slot_items}"
            )),
            Some(_) => {}
            None => failures.push(format!(
                "probe {name:?} slot {slot:?} has no item-count budget data"
            )),
        }
    }
    for (slot, max_slot_serialized_chars) in &limits.max_slot_serialized_chars {
        match report.slot_serialized_chars.get(slot) {
            Some(count) if count > max_slot_serialized_chars => failures.push(format!(
                "probe {name:?} slot {slot:?} serialized {count} chars, over max_slot_serialized_chars {max_slot_serialized_chars}"
            )),
            Some(_) => {}
            None => failures.push(format!(
                "probe {name:?} slot {slot:?} has no serialized-char budget data"
            )),
        }
    }
    failures
}

fn packet_budget_report(packet: &ClioContextPacketV1) -> Result<ContextPacketBudgetReport> {
    let mut total_items = 0;
    let mut slot_item_counts = BTreeMap::new();
    let mut slot_serialized_chars = BTreeMap::new();

    for (slot, items) in packet_slot_slices(packet) {
        total_items += items.len();
        slot_item_counts.insert(slot.to_string(), items.len());
        let serialized = serde_json::to_string(items)
            .with_context(|| format!("failed to serialize context packet slot {slot}"))?;
        slot_serialized_chars.insert(slot.to_string(), serialized.chars().count());
    }

    let serialized_chars = serde_json::to_string(packet)
        .context("failed to serialize context packet for budget report")?
        .chars()
        .count();

    Ok(ContextPacketBudgetReport {
        total_items,
        serialized_chars,
        slot_item_counts,
        slot_serialized_chars,
    })
}

fn output_budget_report(output: &ContextProbeOutput) -> Result<ContextPacketBudgetReport> {
    match output {
        ContextProbeOutput::Packet(packet) => packet_budget_report(packet),
        ContextProbeOutput::Json(value) => Ok(ContextPacketBudgetReport {
            total_items: 0,
            serialized_chars: serde_json::to_string(value)
                .context("failed to serialize installed-provider eval result")?
                .chars()
                .count(),
            slot_item_counts: BTreeMap::new(),
            slot_serialized_chars: BTreeMap::new(),
        }),
    }
}

async fn output_for_probe(
    probe: &ContextEvalProbe,
    mode: ContextEvalProbeSource,
) -> Result<ContextProbeOutput> {
    match mode {
        ContextEvalProbeSource::Fixture => probe
            .packet
            .clone()
            .map(ContextProbeOutput::Packet)
            .with_context(|| format!("fixture probe {} is missing packet", probe.name)),
        ContextEvalProbeSource::LiveBroker => fetch_live_broker_packet(probe)
            .await
            .map(ContextProbeOutput::Packet),
        ContextEvalProbeSource::InstalledProvider => {
            fetch_installed_provider_result(probe).map(ContextProbeOutput::Json)
        }
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

fn fetch_installed_provider_result(probe: &ContextEvalProbe) -> Result<Value> {
    let query = probe
        .query
        .as_ref()
        .map(|query| query.trim())
        .filter(|query| !query.is_empty())
        .with_context(|| format!("installed-provider probe {} is missing query", probe.name))?;
    let python =
        std::env::var("HERMES_JCODE_GRAPH_SMOKE_PYTHON").unwrap_or_else(|_| "python3".to_string());
    let script = std::env::var("HERMES_JCODE_GRAPH_SMOKE_SCRIPT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("scripts")
                .join("hermes_jcode_graph_smoke.py")
        });

    let mut command = Command::new(&python);
    command
        .arg(&script)
        .arg("--query")
        .arg(query)
        .arg("--limit")
        .arg(probe.limit.to_string())
        .arg("--json");

    if let Some(working_dir) = probe
        .working_dir
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        command.arg("--working-dir").arg(working_dir);
    }
    if let Some(session_id) = probe
        .session_id
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        command.arg("--session-id").arg(session_id);
    }
    if let Some(tool_query) = probe
        .tool_query
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        command.arg("--tool-query").arg(tool_query);
    }
    if probe.expect_no_prefetch {
        command.arg("--expect-no-prefetch");
    }
    if let Some(sync_user) = probe
        .sync_user
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        command.arg("--sync-user").arg(sync_user);
    }
    if let Some(sync_assistant) = probe
        .sync_assistant
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        command.arg("--sync-assistant").arg(sync_assistant);
    }
    if let Some(transcript_user) = probe
        .transcript_user
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        command.arg("--transcript-user").arg(transcript_user);
    }
    if let Some(transcript_assistant) = probe
        .transcript_assistant
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        command
            .arg("--transcript-assistant")
            .arg(transcript_assistant);
    }

    let output = command.output().with_context(|| {
        format!(
            "failed to run installed-provider smoke script {} with {python}",
            script.display()
        )
    })?;
    if !output.status.success() {
        anyhow::bail!(
            "installed-provider probe {} failed with status {} stdout={} stderr={}",
            probe.name,
            output.status,
            truncate_for_error(&String::from_utf8_lossy(&output.stdout), 1200),
            truncate_for_error(&String::from_utf8_lossy(&output.stderr), 1200)
        );
    }
    serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "installed-provider probe {} returned invalid JSON stdout={}",
            probe.name,
            truncate_for_error(&String::from_utf8_lossy(&output.stdout), 1200)
        )
    })
}

fn truncate_for_error(value: &str, max_chars: usize) -> String {
    let mut out = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

fn evaluate_assertion(
    output: &ContextProbeOutput,
    assertion: &ContextAssertion,
) -> Result<(), String> {
    match assertion {
        ContextAssertion::PacketVersion { equals } => {
            let packet = output_packet(output)?;
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
            let packet = output_packet(output)?;
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
            let packet = output_packet(output)?;
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
        ContextAssertion::SlotFirstFieldContains {
            slot,
            field,
            contains,
        } => {
            let packet = output_packet(output)?;
            let item = first_slot_item(packet, slot)?;
            match item_field_text(item, field) {
                Some(text) if text.contains(contains) => Ok(()),
                Some(text) => Err(format!(
                    "first item in slot {slot:?} has field {field:?} text {text:?}, which does not contain {contains:?}"
                )),
                None => Err(format!(
                    "first item in slot {slot:?} has no field {field:?}"
                )),
            }
        }
        ContextAssertion::SlotFieldEquals {
            slot,
            field,
            equals,
        } => {
            let packet = output_packet(output)?;
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
        ContextAssertion::SlotFirstFieldEquals {
            slot,
            field,
            equals,
        } => {
            let packet = output_packet(output)?;
            let item = first_slot_item(packet, slot)?;
            match item_field_value(item, field) {
                Some(value) if value == *equals => Ok(()),
                Some(value) => Err(format!(
                    "first item in slot {slot:?} has field {field:?} value {value}, expected {equals}"
                )),
                None => Err(format!(
                    "first item in slot {slot:?} has no field {field:?}"
                )),
            }
        }
        ContextAssertion::SlotForbidsFieldContains {
            slot,
            field,
            contains,
        } => {
            let packet = output_packet(output)?;
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
            let packet = output_packet(output)?;
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
        ContextAssertion::JsonFieldEquals { field, equals } => {
            let value = output_json_field_value(output, field)?;
            if value == equals {
                Ok(())
            } else {
                Err(format!(
                    "json field {field:?} mismatch: expected {equals}, got {value}"
                ))
            }
        }
        ContextAssertion::JsonFieldContains { field, contains } => {
            let text = output_json_field_text(output, field)?;
            if text.contains(contains) {
                Ok(())
            } else {
                Err(format!(
                    "json field {field:?} text does not contain {contains:?}"
                ))
            }
        }
        ContextAssertion::JsonFieldForbidsContains { field, contains } => {
            let text = output_json_field_text(output, field)?;
            if text.contains(contains) {
                Err(format!(
                    "json field {field:?} text unexpectedly contains {contains:?}"
                ))
            } else {
                Ok(())
            }
        }
        ContextAssertion::JsonFieldMin { field, min } => {
            let value = output_json_field_value(output, field)?;
            let number = value
                .as_f64()
                .ok_or_else(|| format!("json field {field:?} is not numeric: {value}"))?;
            if number >= *min {
                Ok(())
            } else {
                Err(format!(
                    "json field {field:?} is {number}, below minimum {min}"
                ))
            }
        }
    }
}

fn output_packet(output: &ContextProbeOutput) -> Result<&ClioContextPacketV1, String> {
    match output {
        ContextProbeOutput::Packet(packet) => Ok(packet),
        ContextProbeOutput::Json(_) => {
            Err("packet assertion used with installed-provider JSON result".to_string())
        }
    }
}

fn output_json(output: &ContextProbeOutput) -> Result<&Value, String> {
    match output {
        ContextProbeOutput::Json(value) => Ok(value),
        ContextProbeOutput::Packet(_) => {
            Err("json assertion used with packet probe result".to_string())
        }
    }
}

fn output_json_field_value<'a>(
    output: &'a ContextProbeOutput,
    field: &str,
) -> Result<&'a Value, String> {
    let value = output_json(output)?;
    get_dotted_value(value, field).ok_or_else(|| format!("json field {field:?} is missing"))
}

fn output_json_field_text(output: &ContextProbeOutput, field: &str) -> Result<String, String> {
    let value = output_json_field_value(output, field)?;
    match value {
        Value::Null => Ok("null".to_string()),
        Value::String(value) => Ok(value.clone()),
        other => Ok(other.to_string()),
    }
}

fn slot_items<'a>(
    packet: &'a ClioContextPacketV1,
    slot: &str,
) -> Result<&'a [ClioContextPacketItem], String> {
    for (name, items) in packet_slot_slices(packet) {
        if name == slot {
            return Ok(items);
        }
    }
    Err(format!("unknown context packet slot {slot:?}"))
}

fn first_slot_item<'a>(
    packet: &'a ClioContextPacketV1,
    slot: &str,
) -> Result<&'a ClioContextPacketItem, String> {
    slot_items(packet, slot)?
        .first()
        .ok_or_else(|| format!("slot {slot:?} is empty"))
}

fn packet_slot_slices(
    packet: &ClioContextPacketV1,
) -> [(&'static str, &[ClioContextPacketItem]); 10] {
    [
        ("active_task", &packet.active_task),
        ("authority", &packet.authority),
        ("lineage", &packet.lineage),
        ("vault_evidence", &packet.vault_evidence),
        ("durable_memory", &packet.durable_memory),
        ("session_evidence", &packet.session_evidence),
        ("artifact_refs", &packet.artifact_refs),
        ("conflicts", &packet.conflicts),
        ("skill_hints", &packet.skill_hints),
        ("tool_hints", &packet.tool_hints),
    ]
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
        .try_fold(value, |current, segment| match current {
            Value::Array(items) => segment
                .parse::<usize>()
                .ok()
                .and_then(|index| items.get(index)),
            _ => current.get(segment),
        })
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
    print!("{}", human_report(report));
}

fn human_report(report: &ContextEvalReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        &mut out,
        "context eval review: {} [{}]",
        report.suite, report.mode
    );
    let _ = writeln!(
        &mut out,
        "Outcome: {} ({}/{} probes, pass_rate={:.2}, required={:.2}, duration_ms={})",
        if report.passed { "passed" } else { "failed" },
        report.passed_count,
        report.probe_count,
        report.pass_rate,
        report.required_pass_rate,
        report.duration_ms
    );
    let _ = writeln!(&mut out);
    let _ = writeln!(&mut out, "Provider/config under test");
    let _ = writeln!(&mut out, "- kind: {}", report.provider.kind);
    let _ = writeln!(&mut out, "- version: {}", report.provider.version);
    let _ = writeln!(&mut out, "- git_hash: {}", report.provider.git_hash);
    let _ = writeln!(
        &mut out,
        "- socket: {}",
        report.provider.socket_path.as_deref().unwrap_or("n/a")
    );
    let _ = writeln!(
        &mut out,
        "- working_dirs: {}",
        if report.provider.working_dirs.is_empty() {
            "[]".to_string()
        } else {
            report.provider.working_dirs.join(", ")
        }
    );
    let _ = writeln!(&mut out);
    let _ = writeln!(&mut out, "Probe pass/fail table");
    for probe in &report.probes {
        let _ = writeln!(
            &mut out,
            "- {} {:<7} group={} duration_ms={} packet_items={} packet_chars={}",
            if probe.passed { "PASS" } else { "FAIL" },
            probe.name,
            probe.group,
            probe.duration_ms,
            probe.packet_budget.total_items,
            probe.packet_budget.serialized_chars
        );
    }
    let _ = writeln!(&mut out);
    let _ = writeln!(&mut out, "Top failures");
    if report.review.top_failures.is_empty() {
        let _ = writeln!(&mut out, "- none");
    } else {
        for failure in &report.review.top_failures {
            let _ = writeln!(
                &mut out,
                "- {} [{}]: {} ({})",
                failure.probe, failure.group, failure.message, failure.classification
            );
        }
    }
    let _ = writeln!(&mut out);
    let _ = writeln!(&mut out, "Recommendation");
    let _ = writeln!(&mut out, "- {}", review_recommendation(report));
    out
}

fn classify_failure(failure: &str) -> &'static str {
    if failure.contains("max_duration_ms") {
        "latency_or_ux"
    } else if failure.contains("max_total_items")
        || failure.contains("max_serialized_chars")
        || failure.contains("max_slot_items")
        || failure.contains("max_slot_serialized_chars")
    {
        "budget_or_rendering"
    } else if failure.contains("source_path")
        || failure.contains("line_start")
        || failure.contains("line_end")
        || failure.contains("span")
        || failure.contains("provenance")
    {
        "source_trace_or_retrieval"
    } else if failure.contains("json field")
        || failure.contains("prefetch")
        || failure.contains("installed-provider")
    {
        "provider_adapter_or_rendering"
    } else if failure.contains("slot") || failure.contains("packet version") {
        "context_contract_structure"
    } else {
        "provider_or_probe"
    }
}

fn review_summary(
    passed: bool,
    group_results: &BTreeMap<String, ContextEvalGroupReport>,
    probes: &[ContextEvalProbeReport],
) -> ContextEvalReviewReport {
    let mut failure_class_counts = BTreeMap::new();
    let mut top_failures = Vec::new();
    for probe in probes {
        for failure in &probe.failures {
            let classification = classify_failure(failure).to_string();
            *failure_class_counts
                .entry(classification.clone())
                .or_insert(0) += 1;
            if top_failures.len() < 5 {
                top_failures.push(ContextEvalReviewFailure {
                    probe: probe.name.clone(),
                    group: probe.group.clone(),
                    classification,
                    message: failure.clone(),
                });
            }
        }
    }

    ContextEvalReviewReport {
        recommendation: recommendation_for(passed, group_results, &failure_class_counts)
            .to_string(),
        failure_class_counts,
        top_failures,
    }
}

fn review_recommendation(report: &ContextEvalReport) -> &str {
    report.review.recommendation.as_str()
}

fn recommendation_for(
    passed: bool,
    group_results: &BTreeMap<String, ContextEvalGroupReport>,
    failure_class_counts: &BTreeMap<String, usize>,
) -> &'static str {
    if passed {
        return "continue with jcode as incumbent for this mode; no remediation needed.";
    }
    let required_group_failed = group_results
        .values()
        .any(|group| group.required && !group.passed);

    if required_group_failed
        || failure_class_counts.contains_key("source_trace_or_retrieval")
        || failure_class_counts.contains_key("context_contract_structure")
    {
        "run one focused jcode remediation pass before deeper investment; if the same gate repeats, start the provider bake-off."
    } else if failure_class_counts.contains_key("latency_or_ux")
        || failure_class_counts.contains_key("budget_or_rendering")
    {
        "treat this as blocking UX/context-budget work before adoption; remediate once, then rerun the locked suite."
    } else {
        "review whether this is a provider bug or bad probe; update the probe only with a progress-log note."
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
        assert_eq!(
            ContextEvalProbeSource::parse("installed-provider").unwrap(),
            ContextEvalProbeSource::InstalledProvider
        );
        assert_eq!(
            ContextEvalProbeSource::parse("installed_provider").unwrap(),
            ContextEvalProbeSource::InstalledProvider
        );
    }

    #[test]
    fn clio_suite_locks_installed_provider_checkpoint_correction_probe() {
        let suite_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("context_evals")
            .join("clio-super-session-v1")
            .join("suite.json");
        let suite = load_suite(&suite_path).unwrap();

        let probe = suite
            .probes
            .iter()
            .find(|probe| probe.name == "installed_provider_current_correction_over_checkpoint")
            .expect(
                "suite should lock installed-provider current-correction-over-checkpoint coverage",
            );

        assert_eq!(probe.source, ContextEvalProbeSource::InstalledProvider);
        assert_eq!(probe.group, "authority_conflict");
        assert!(
            probe
                .assertions
                .iter()
                .any(|assertion| matches!(
                    assertion,
                    ContextAssertion::JsonFieldContains { field, contains }
                        if field == "prefetch"
                            && contains.contains("Current user request and latest correction override stored broker context")
                )),
            "probe should assert the provider-rendered correction precedence guard"
        );
        assert!(
            probe.assertions.iter().any(|assertion| matches!(
                assertion,
                ContextAssertion::JsonFieldContains { field, contains }
                    if field == "tool_items" && contains.contains("compression_checkpoint")
            )),
            "probe should assert explicit broker-tool checkpoint evidence remains available"
        );
    }

    #[test]
    fn json_assertions_read_nested_installed_provider_result() {
        let output = ContextProbeOutput::Json(json!({
            "prefetch_has_context": false,
            "prefetch_diagnostics": {"last_prefetch_item_count": 0},
            "tool_item_count": 17
        }));

        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldEquals {
                    field: "prefetch_has_context".to_string(),
                    equals: json!(false),
                }
            )
            .is_ok()
        );
        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldEquals {
                    field: "prefetch_diagnostics.last_prefetch_item_count".to_string(),
                    equals: json!(0),
                }
            )
            .is_ok()
        );
        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldMin {
                    field: "tool_item_count".to_string(),
                    min: 1.0,
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn json_assertions_read_array_indexed_installed_provider_result() {
        let output = ContextProbeOutput::Json(json!({
            "tool_items": [
                {
                    "kind": "vault_chunk",
                    "source_path": "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md",
                    "line_start": 199,
                    "metadata": {"relationship": "backlink"}
                }
            ]
        }));

        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldContains {
                    field: "tool_items.0.source_path".to_string(),
                    contains: "Ghostty Terminal".to_string(),
                }
            )
            .is_ok()
        );
        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldForbidsContains {
                    field: "tool_items".to_string(),
                    contains: "OpenClaw-Stack/Honcho-Provider-Plan.md".to_string(),
                }
            )
            .is_ok()
        );
        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldForbidsContains {
                    field: "tool_items".to_string(),
                    contains: "Ghostty Terminal".to_string(),
                }
            )
            .is_err()
        );
        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldEquals {
                    field: "tool_items.0.line_start".to_string(),
                    equals: json!(199),
                }
            )
            .is_ok()
        );
        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::JsonFieldEquals {
                    field: "tool_items.0.metadata.relationship".to_string(),
                    equals: json!("backlink"),
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn slot_first_field_assertions_check_only_the_first_item() {
        let packet: ClioContextPacketV1 = serde_json::from_value(json!({
            "version": "clio_context_packet_v1",
            "authority": [
                {
                    "id": "vault:first",
                    "kind": "vault_chunk",
                    "scope": "vault",
                    "source_path": "TaskNotes/Prompt in Progress.md",
                    "relevance": {"rank": 1}
                },
                {
                    "id": "vault:second",
                    "kind": "vault_chunk",
                    "scope": "vault",
                    "source_path": "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md",
                    "relevance": {"rank": 2}
                }
            ]
        }))
        .unwrap();
        let output = ContextProbeOutput::Packet(packet);

        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::SlotFirstFieldContains {
                    slot: "authority".to_string(),
                    field: "source_path".to_string(),
                    contains: "Prompt in Progress.md".to_string(),
                }
            )
            .is_ok()
        );
        assert!(
            evaluate_assertion(
                &output,
                &ContextAssertion::SlotFirstFieldContains {
                    slot: "authority".to_string(),
                    field: "source_path".to_string(),
                    contains: "Ghostty Terminal".to_string(),
                }
            )
            .is_err()
        );
    }

    #[test]
    fn duration_failures_trip_when_probe_exceeds_max_duration() {
        assert!(duration_failures("fast_probe", Some(100), 100).is_empty());
        assert_eq!(
            duration_failures("slow_probe", Some(100), 101),
            vec!["probe \"slow_probe\" took 101ms, over max_duration_ms 100".to_string()]
        );
        assert!(duration_failures("unbounded_probe", None, 10_000).is_empty());
    }

    #[test]
    fn budget_failures_trip_for_total_and_slot_caps() {
        let mut slot_item_counts = BTreeMap::new();
        slot_item_counts.insert("authority".to_string(), 4);
        let mut slot_serialized_chars = BTreeMap::new();
        slot_serialized_chars.insert("authority".to_string(), 1_201);
        let report = ContextPacketBudgetReport {
            total_items: 12,
            serialized_chars: 9_001,
            slot_item_counts,
            slot_serialized_chars,
        };
        let limits = ContextPacketBudgetLimits {
            max_total_items: Some(10),
            max_serialized_chars: Some(9_000),
            max_slot_items: BTreeMap::from([("authority".to_string(), 3)]),
            max_slot_serialized_chars: BTreeMap::from([("authority".to_string(), 1_200)]),
        };

        assert_eq!(
            budget_failures("budget_probe", &limits, &report),
            vec![
                "probe \"budget_probe\" packet has 12 items, over max_total_items 10".to_string(),
                "probe \"budget_probe\" packet serialized 9001 chars, over max_serialized_chars 9000".to_string(),
                "probe \"budget_probe\" slot \"authority\" has 4 items, over max_slot_items 3".to_string(),
                "probe \"budget_probe\" slot \"authority\" serialized 1201 chars, over max_slot_serialized_chars 1200".to_string(),
            ]
        );
    }

    #[test]
    fn human_report_includes_review_sections_and_failure_classification() {
        let group_results = BTreeMap::from([(
            "latency".to_string(),
            ContextEvalGroupReport {
                passed: false,
                required: false,
                pass_rate: 0.0,
                probe_count: 1,
                passed_count: 0,
                failed_count: 1,
            },
        )]);
        let probes = vec![
            ContextEvalProbeReport {
                name: "fast_probe".to_string(),
                group: "lineage".to_string(),
                description: "Fast path".to_string(),
                source: "live_broker".to_string(),
                duration_ms: 10,
                packet_budget: ContextPacketBudgetReport {
                    total_items: 1,
                    serialized_chars: 100,
                    slot_item_counts: BTreeMap::new(),
                    slot_serialized_chars: BTreeMap::new(),
                },
                passed: true,
                failures: Vec::new(),
            },
            ContextEvalProbeReport {
                name: "slow_probe".to_string(),
                group: "latency".to_string(),
                description: "Slow path".to_string(),
                source: "live_broker".to_string(),
                duration_ms: 101,
                packet_budget: ContextPacketBudgetReport {
                    total_items: 2,
                    serialized_chars: 200,
                    slot_item_counts: BTreeMap::new(),
                    slot_serialized_chars: BTreeMap::new(),
                },
                passed: false,
                failures: vec![
                    "probe \"slow_probe\" took 101ms, over max_duration_ms 100".to_string(),
                ],
            },
        ];
        let review = review_summary(false, &group_results, &probes);
        let report = ContextEvalReport {
            suite: "clio-super-session-v1".to_string(),
            version: 1,
            suite_path: "/tmp/suite.json".to_string(),
            mode: "live_broker".to_string(),
            timestamp: "2026-05-26T00:00:00Z".to_string(),
            duration_ms: 123,
            provider: ContextEvalProviderReport {
                kind: "live_broker".to_string(),
                socket_path: Some("/tmp/jcode.sock".to_string()),
                working_dirs: vec!["/tmp/project".to_string()],
                git_hash: "abc12345".to_string(),
                version: "v0.test".to_string(),
            },
            review,
            passed: false,
            pass_rate: 0.5,
            required_pass_rate: 0.9,
            probe_count: 2,
            passed_count: 1,
            failed_count: 1,
            group_results,
            probes,
        };

        let rendered = human_report(&report);

        assert!(rendered.contains("Provider/config under test"));
        assert!(rendered.contains("Probe pass/fail table"));
        assert!(rendered.contains("Top failures"));
        assert!(rendered.contains("latency_or_ux"));
        assert!(rendered.contains("Recommendation"));

        let json_report = serde_json::to_value(&report).unwrap();
        assert_eq!(
            json_report["review"]["top_failures"][0]["classification"],
            json!("latency_or_ux")
        );
    }
}
