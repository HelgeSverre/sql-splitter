//! Fixture-backed orchestration for the migration contract spike.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};

use super::artifact::{replace_json, write_json_new};
use super::canonical::{digest_rows, encode_row, CanonicalRow, CANONICAL_ENCODING_VERSION};
use super::connection::{
    CancellationToken, KeyTuple, KeysetPage, ReadSession, SourceConnectionFactory,
    TargetConnectionFactory,
};
use super::fixture::{InMemorySource, InMemoryTarget};
use super::journal::{ChunkRecord, ChunkState, MigrationState, ResumeBinding};
use super::model::{ColumnMeta, DbValue, Identifier, QualifiedTable};
use super::plan::{
    MigrationPlan, OperationKind, PlanOperation, ReviewedPlan, UnsupportedObjectReport,
    PLAN_SCHEMA_VERSION,
};
use super::verify::{verify_keyed_rows, KeyedRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpikeArtifacts {
    pub plan: PathBuf,
    pub state: PathBuf,
    pub plan_hash: String,
    pub copied_rows: usize,
}

/// Execute the Phase 1–5 contract path against deterministic in-memory adapters.
pub fn run_fixture_spike(directory: impl AsRef<Path>) -> anyhow::Result<SpikeArtifacts> {
    let directory = directory.as_ref();
    std::fs::create_dir_all(directory)
        .with_context(|| format!("create spike artifact directory {}", directory.display()))?;
    let plan_path = directory.join("migration-plan.json");
    let state_path = directory.join("migration-state.json");

    let table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("accounts")?,
    };
    let columns = vec![column("id", 0, "bigint")?, column("name", 1, "text")?];
    let rows = vec![
        vec![DbValue::Unsigned(1), DbValue::Text("Ada".into())],
        vec![DbValue::Unsigned(2), DbValue::Text("Grace".into())],
        vec![DbValue::Unsigned(3), DbValue::Text("Linus".into())],
    ];
    let source = InMemorySource::new("fixture-source", "fixture-db");
    source.add_table(table.clone(), columns.clone(), rows);
    let target = InMemoryTarget::default();

    let copy = PlanOperation::new(
        OperationKind::CopyTable,
        Some(table.clone()),
        Vec::new(),
        BTreeMap::new(),
    )?;
    let verify = PlanOperation::new(
        OperationKind::VerifyTable,
        Some(table.clone()),
        vec![copy.id.clone()],
        BTreeMap::new(),
    )?;
    let reviewed = ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        migration_id: "fixture-phase-1-5".into(),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: "fixture-source".into(),
        target_endpoint_identity: "fixture-target".into(),
        source_catalog_fingerprint: "fixture-source-schema-v1".into(),
        target_catalog_fingerprint: "empty-target-v1".into(),
        source_catalog: None,
        target_catalog: None,
        consistency_mode: "consistent_snapshot".into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: "same-dialect-exact".into(),
        capabilities: BTreeMap::from([
            ("consistent_snapshot".into(), "fixture_supported".into()),
            ("server_read_only".into(), "fixture_supported".into()),
        ]),
        operations: vec![copy.clone(), verify],
        unsupported_objects: UnsupportedObjectReport::default(),
    })?;
    reviewed.plan.validate_for_execution()?;
    write_json_new(&plan_path, &reviewed)?;

    let cancellation = CancellationToken::default();
    let snapshot = source.capture_snapshot()?;
    let mut reader = source.open_reader(&snapshot, cancellation.clone())?;
    if !reader.read_only_evidence().server_enforced {
        return Err(anyhow!("source read-only evidence is not server enforced"));
    }
    let binding = ResumeBinding {
        migration_id: reviewed.plan.migration_id.clone(),
        plan_hash: reviewed.plan_hash.to_string(),
        approval_reference: "fixture-spike-only".into(),
        tool_version: reviewed.plan.tool_version.clone(),
        source_endpoint: reviewed.plan.source_endpoint_identity.clone(),
        target_endpoint: reviewed.plan.target_endpoint_identity.clone(),
        snapshot_identity: snapshot.snapshot_id.clone(),
        source_schema_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
        target_schema_fingerprint: reviewed.plan.target_catalog_fingerprint.clone(),
        conversion_policy: reviewed.plan.conversion_policy.clone(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
    };
    let mut state = MigrationState::new(binding.clone());
    write_json_new(&state_path, &state)?;

    let projection: Vec<_> = columns.iter().map(|column| column.name.clone()).collect();
    let key = vec![projection[0].clone()];
    let mut after = None;
    let mut expected = Vec::new();
    let mut chunk_id = 0;
    loop {
        let batch = reader.select_page(&KeysetPage {
            table: table.clone(),
            projection: projection.clone(),
            key: key.clone(),
            after: after.clone(),
            limit: 2,
        })?;
        if batch.is_empty() {
            break;
        }
        chunk_id += 1;
        let canonical_rows: Vec<_> = batch
            .rows()
            .iter()
            .map(|row| CanonicalRow {
                table: "public.accounts",
                columns: &["id", "name"],
                key: &row[..1],
                values: row,
            })
            .collect();
        let digest = hex::encode(digest_rows(canonical_rows.iter()));
        let final_key = KeyTuple::new(vec![
            batch.rows().last().expect("non-empty batch")[0].clone()
        ]);
        state.prepare(ChunkRecord {
            chunk_id,
            operation_id: copy.id.to_string(),
            start_key: after.as_ref().map(|key: &KeyTuple| key.0.clone()),
            final_key: final_key.0.clone(),
            row_count: batch.len() as u64,
            canonical_digest: digest,
            target_transaction_intent: format!("fixture-chunk-{chunk_id}"),
            state: ChunkState::Prepared,
        })?;
        replace_json(&state_path, &state)?;
        let mut writer = target.open_writer(cancellation.clone())?;
        writer.begin()?;
        if let Err(error) = writer.insert(&table, &batch).and_then(|()| writer.commit()) {
            let _ = writer.rollback();
            return Err(error.into());
        }
        state.commit(chunk_id)?;
        replace_json(&state_path, &state)?;
        expected.extend(batch.rows().iter().cloned());
        after = Some(final_key);
    }

    let mut verifier = target.open_verifier(cancellation)?;
    let target_batch = verifier.select_page(&KeysetPage {
        table,
        projection: projection.clone(),
        key: key.clone(),
        after: None,
        limit: u32::MAX,
    })?;
    let independently_read = read_all(
        reader.as_mut(),
        &QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new("accounts")?,
        },
        &projection,
        &key,
    )?;
    verify_manifest(&state, &independently_read)?;
    let report = verify_keyed_rows(
        key_rows(&independently_read)?,
        key_rows(target_batch.rows())?,
    );
    if !report.is_exact() {
        return Err(anyhow!("strict fixture verification failed: {report:?}"));
    }
    state.validate_resume(&binding)?;
    Ok(SpikeArtifacts {
        plan: plan_path,
        state: state_path,
        plan_hash: reviewed.plan_hash.to_string(),
        copied_rows: expected.len(),
    })
}

fn column(name: &str, ordinal: u32, vendor_type: &str) -> anyhow::Result<ColumnMeta> {
    Ok(ColumnMeta {
        name: Identifier::new(name)?,
        ordinal,
        vendor_type: vendor_type.into(),
        nullable: false,
        collation: None,
        precision: None,
        scale: None,
        timezone_semantics: None,
    })
}

fn key_rows(rows: &[Vec<DbValue>]) -> anyhow::Result<Vec<KeyedRow<Vec<u8>, Vec<u8>>>> {
    rows.iter()
        .map(|row| {
            let canonical = CanonicalRow {
                table: "public.accounts",
                columns: &["id", "name"],
                key: &row[..1],
                values: row,
            };
            Ok(KeyedRow {
                key: encode_row(&CanonicalRow {
                    values: &[],
                    ..canonical.clone()
                }),
                value: encode_row(&canonical),
            })
        })
        .collect()
}

fn read_all(
    reader: &mut dyn ReadSession,
    table: &QualifiedTable,
    projection: &[Identifier],
    key: &[Identifier],
) -> anyhow::Result<Vec<Vec<DbValue>>> {
    let mut rows = Vec::new();
    let mut after = None;
    loop {
        let batch = reader.select_page(&KeysetPage {
            table: table.clone(),
            projection: projection.to_vec(),
            key: key.to_vec(),
            after: after.clone(),
            limit: 2,
        })?;
        if batch.is_empty() {
            return Ok(rows);
        }
        after = Some(KeyTuple::new(vec![batch
            .rows()
            .last()
            .expect("non-empty batch")[0]
            .clone()]));
        rows.extend(batch.rows().iter().cloned());
    }
}

fn verify_manifest(state: &MigrationState, rows: &[Vec<DbValue>]) -> anyhow::Result<()> {
    let mut offset = 0usize;
    for chunk in &state.chunks {
        let count = usize::try_from(chunk.row_count).context("chunk row count exceeds usize")?;
        let end = offset
            .checked_add(count)
            .filter(|end| *end <= rows.len())
            .ok_or_else(|| anyhow!("chunk manifest exceeds source snapshot"))?;
        let canonical: Vec<_> = rows[offset..end]
            .iter()
            .map(|row| CanonicalRow {
                table: "public.accounts",
                columns: &["id", "name"],
                key: &row[..1],
                values: row,
            })
            .collect();
        if hex::encode(digest_rows(canonical.iter())) != chunk.canonical_digest {
            return Err(anyhow!("chunk {} canonical digest drift", chunk.chunk_id));
        }
        if rows[end - 1][..1] != chunk.final_key {
            return Err(anyhow!("chunk {} final key drift", chunk.chunk_id));
        }
        offset = end;
    }
    if offset != rows.len() {
        return Err(anyhow!(
            "chunk manifest does not cover the complete source snapshot"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vertical_spike_copies_journals_and_verifies() {
        let directory = tempfile::tempdir().unwrap();
        let artifacts = run_fixture_spike(directory.path()).unwrap();
        assert_eq!(artifacts.copied_rows, 3);
        let state: MigrationState = super::super::artifact::read_json(artifacts.state).unwrap();
        assert_eq!(state.chunks.len(), 2);
        assert!(state
            .chunks
            .iter()
            .all(|chunk| chunk.state == ChunkState::Committed));
    }

    #[test]
    fn manifest_digest_drift_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let artifacts = run_fixture_spike(directory.path()).unwrap();
        let mut state: MigrationState = super::super::artifact::read_json(artifacts.state).unwrap();
        state.chunks[0].canonical_digest = "tampered".into();
        let rows = vec![
            vec![DbValue::Unsigned(1), DbValue::Text("Ada".into())],
            vec![DbValue::Unsigned(2), DbValue::Text("Grace".into())],
            vec![DbValue::Unsigned(3), DbValue::Text("Linus".into())],
        ];
        assert!(verify_manifest(&state, &rows).is_err());
    }
}
