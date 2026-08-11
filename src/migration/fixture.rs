use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::connection::{
    CancellationToken, Capability, CapabilitySet, ConnectionError, ConnectionResult, KeysetPage,
    QualifiedTable, ReadOnlyEvidence, ReadSession, SnapshotToken, SourceConnectionFactory,
    TargetConnectionFactory, VerificationSession, WriteSession,
};
use super::model::{ColumnMeta, DbValue, Identifier, RowBatch};

#[derive(Debug, Clone)]
struct FixtureTable {
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<DbValue>>,
}

type Tables = HashMap<QualifiedTable, FixtureTable>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePoint {
    Read,
    Insert,
    Commit,
}

#[derive(Debug, Clone)]
pub struct InMemorySource {
    endpoint_identity: String,
    database_identity: String,
    tables: Arc<Mutex<Tables>>,
    failure: Arc<Mutex<Option<FailurePoint>>>,
}

impl InMemorySource {
    pub fn new(endpoint_identity: impl Into<String>, database_identity: impl Into<String>) -> Self {
        Self {
            endpoint_identity: endpoint_identity.into(),
            database_identity: database_identity.into(),
            tables: Arc::new(Mutex::new(HashMap::new())),
            failure: Arc::new(Mutex::new(None)),
        }
    }

    pub fn add_table(
        &self,
        table: QualifiedTable,
        columns: Vec<ColumnMeta>,
        rows: Vec<Vec<DbValue>>,
    ) {
        self.tables
            .lock()
            .expect("fixture table lock must not be poisoned")
            .insert(table, FixtureTable { columns, rows });
    }

    pub fn fail_once(&self, point: FailurePoint) {
        *self
            .failure
            .lock()
            .expect("fixture failure lock must not be poisoned") = Some(point);
    }
}

impl SourceConnectionFactory for InMemorySource {
    fn capabilities(&self) -> CapabilitySet {
        fixture_capabilities()
    }

    fn capture_snapshot(&self) -> ConnectionResult<SnapshotToken> {
        Ok(SnapshotToken {
            endpoint_identity: self.endpoint_identity.clone(),
            database_identity: self.database_identity.clone(),
            snapshot_id: "fixture-snapshot-v1".to_owned(),
            consistency_mode: "fixture_immutable_snapshot".to_owned(),
            server_version: "fixture-v1".to_owned(),
            lifecycle_id: "fixture-lifecycle-v1".to_owned(),
        })
    }

    fn open_reader(
        &self,
        snapshot: &SnapshotToken,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn ReadSession>> {
        if snapshot.endpoint_identity != self.endpoint_identity
            || snapshot.database_identity != self.database_identity
        {
            return Err(ConnectionError::SnapshotMismatch);
        }
        let tables = self
            .tables
            .lock()
            .expect("fixture table lock must not be poisoned")
            .clone();
        Ok(Box::new(FixtureReader {
            snapshot: snapshot.clone(),
            evidence: ReadOnlyEvidence {
                server_enforced: true,
                description: "deterministic fixture denies all source writes".to_owned(),
            },
            tables,
            cancellation,
            failure: Arc::clone(&self.failure),
        }))
    }
}

struct FixtureReader {
    snapshot: SnapshotToken,
    evidence: ReadOnlyEvidence,
    tables: Tables,
    cancellation: CancellationToken,
    failure: Arc<Mutex<Option<FailurePoint>>>,
}

impl ReadSession for FixtureReader {
    fn read_only_evidence(&self) -> &ReadOnlyEvidence {
        &self.evidence
    }

    fn snapshot(&self) -> &SnapshotToken {
        &self.snapshot
    }

    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.cancellation.check()?;
        take_failure(&self.failure, FailurePoint::Read)?;
        select_page(&self.tables, request)
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryTarget {
    committed: Arc<Mutex<Tables>>,
    failure: Arc<Mutex<Option<FailurePoint>>>,
}

impl InMemoryTarget {
    pub fn fail_once(&self, point: FailurePoint) {
        *self
            .failure
            .lock()
            .expect("fixture failure lock must not be poisoned") = Some(point);
    }

    pub fn rows(&self, table: &QualifiedTable) -> Vec<Vec<DbValue>> {
        self.committed
            .lock()
            .expect("fixture target lock must not be poisoned")
            .get(table)
            .map(|value| value.rows.clone())
            .unwrap_or_default()
    }
}

impl TargetConnectionFactory for InMemoryTarget {
    fn capabilities(&self) -> CapabilitySet {
        fixture_capabilities()
    }

    fn open_writer(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn WriteSession>> {
        Ok(Box::new(FixtureWriter {
            committed: Arc::clone(&self.committed),
            pending: None,
            cancellation,
            failure: Arc::clone(&self.failure),
        }))
    }

    fn open_verifier(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn VerificationSession>> {
        Ok(Box::new(FixtureVerifier {
            committed: Arc::clone(&self.committed),
            cancellation,
        }))
    }
}

struct FixtureWriter {
    committed: Arc<Mutex<Tables>>,
    pending: Option<Tables>,
    cancellation: CancellationToken,
    failure: Arc<Mutex<Option<FailurePoint>>>,
}

impl WriteSession for FixtureWriter {
    fn begin(&mut self) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if self.pending.is_some() {
            return Err(ConnectionError::TransactionAlreadyOpen);
        }
        self.pending = Some(HashMap::new());
        Ok(())
    }

    fn insert(&mut self, table: &QualifiedTable, batch: &RowBatch) -> ConnectionResult<()> {
        self.cancellation.check()?;
        take_failure(&self.failure, FailurePoint::Insert)?;
        let pending = self
            .pending
            .as_mut()
            .ok_or(ConnectionError::TransactionRequired)?;
        let entry = pending
            .entry(table.clone())
            .or_insert_with(|| FixtureTable {
                columns: batch.columns().to_vec(),
                rows: Vec::new(),
            });
        entry.rows.extend(batch.rows().iter().cloned());
        Ok(())
    }

    fn commit(&mut self) -> ConnectionResult<()> {
        self.cancellation.check()?;
        take_failure(&self.failure, FailurePoint::Commit)?;
        let pending = self
            .pending
            .take()
            .ok_or(ConnectionError::TransactionRequired)?;
        let mut committed = self
            .committed
            .lock()
            .expect("fixture target lock must not be poisoned");
        for (table, mut addition) in pending {
            let entry = committed.entry(table).or_insert_with(|| FixtureTable {
                columns: addition.columns.clone(),
                rows: Vec::new(),
            });
            entry.rows.append(&mut addition.rows);
        }
        Ok(())
    }

    fn rollback(&mut self) -> ConnectionResult<()> {
        self.pending
            .take()
            .ok_or(ConnectionError::TransactionRequired)?;
        Ok(())
    }
}

struct FixtureVerifier {
    committed: Arc<Mutex<Tables>>,
    cancellation: CancellationToken,
}

impl VerificationSession for FixtureVerifier {
    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.cancellation.check()?;
        select_page(
            &self
                .committed
                .lock()
                .expect("fixture target lock must not be poisoned"),
            request,
        )
    }
}

fn fixture_capabilities() -> CapabilitySet {
    CapabilitySet::from_entries([
        ("consistent_snapshot", Capability::Supported),
        ("server_read_only", Capability::Supported),
        ("transactions", Capability::Supported),
        ("cancellation", Capability::Supported),
        ("typed_identifiers", Capability::Supported),
        ("bound_parameters", Capability::Supported),
    ])
}

fn take_failure(
    failure: &Mutex<Option<FailurePoint>>,
    expected: FailurePoint,
) -> ConnectionResult<()> {
    let mut failure = failure
        .lock()
        .expect("fixture failure lock must not be poisoned");
    if *failure == Some(expected) {
        *failure = None;
        return Err(ConnectionError::InjectedFailure(format!("{expected:?}")));
    }
    Ok(())
}

fn select_page(tables: &Tables, request: &KeysetPage) -> ConnectionResult<RowBatch> {
    if request.limit == 0 || request.key.is_empty() {
        return Err(ConnectionError::InvalidRequest(
            "limit and key must be non-zero".to_owned(),
        ));
    }
    let table = tables
        .get(&request.table)
        .ok_or_else(|| ConnectionError::TableNotFound(request.table.clone()))?;
    let projection_indexes = indexes(&table.columns, &request.projection)?;
    let key_indexes = indexes(&table.columns, &request.key)?;
    if request
        .after
        .as_ref()
        .is_some_and(|after| after.0.len() != key_indexes.len())
    {
        return Err(ConnectionError::InvalidRequest(
            "key tuple width differs from key".to_owned(),
        ));
    }
    let mut rows: Vec<&Vec<DbValue>> = table.rows.iter().collect();
    rows.sort_by(|left, right| compare_key(left, right, &key_indexes).unwrap_or(Ordering::Equal));
    for pair in rows.windows(2) {
        match compare_key(pair[0], pair[1], &key_indexes)? {
            Ordering::Equal => {
                return Err(ConnectionError::InvalidRequest(
                    "resumable key is not unique".to_owned(),
                ));
            }
            Ordering::Less => {}
            Ordering::Greater => unreachable!("rows were sorted by the selected key"),
        }
    }
    let selected_rows: Vec<Vec<DbValue>> = rows
        .into_iter()
        .filter(|row| {
            request.after.as_ref().is_none_or(|after| {
                compare_row_tuple(row, &after.0, &key_indexes) == Ok(Ordering::Greater)
            })
        })
        .take(request.limit as usize)
        .map(|row| {
            projection_indexes
                .iter()
                .map(|index| row[*index].clone())
                .collect()
        })
        .collect();
    let columns = projection_indexes
        .iter()
        .map(|index| table.columns[*index].clone())
        .collect();
    let mut batch = RowBatch::new(columns, selected_rows.len(), usize::MAX);
    for row in selected_rows {
        batch
            .try_push(row, 0)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    }
    Ok(batch)
}

fn indexes(columns: &[ColumnMeta], requested: &[Identifier]) -> ConnectionResult<Vec<usize>> {
    requested
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|column| &column.name == name)
                .ok_or_else(|| ConnectionError::InvalidRequest(format!("unknown column {name:?}")))
        })
        .collect()
}

fn compare_key(
    left: &[DbValue],
    right: &[DbValue],
    indexes: &[usize],
) -> ConnectionResult<Ordering> {
    for index in indexes {
        let ordering = compare_value(&left[*index], &right[*index])?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_row_tuple(
    row: &[DbValue],
    tuple: &[DbValue],
    indexes: &[usize],
) -> ConnectionResult<Ordering> {
    for (index, value) in indexes.iter().zip(tuple) {
        let ordering = compare_value(&row[*index], value)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_value(left: &DbValue, right: &DbValue) -> ConnectionResult<Ordering> {
    match (left, right) {
        (DbValue::Signed(left), DbValue::Signed(right)) => Ok(left.cmp(right)),
        (DbValue::Unsigned(left), DbValue::Unsigned(right)) => Ok(left.cmp(right)),
        (DbValue::Text(left), DbValue::Text(right)) => Ok(left.cmp(right)),
        (DbValue::Bytes(left), DbValue::Bytes(right)) => Ok(left.cmp(right)),
        _ => Err(ConnectionError::UnsupportedKeyValue),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::model::KeyTuple;

    fn identifier(value: &str) -> Identifier {
        Identifier::new(value).unwrap()
    }

    fn table() -> QualifiedTable {
        QualifiedTable {
            namespace: identifier("public"),
            name: identifier("items"),
        }
    }

    fn column(name: &str, ordinal: u32) -> ColumnMeta {
        ColumnMeta {
            name: identifier(name),
            ordinal,
            vendor_type: "integer".to_owned(),
            nullable: false,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        }
    }

    fn request(after: Option<Vec<DbValue>>) -> KeysetPage {
        KeysetPage {
            table: table(),
            projection: vec![identifier("a"), identifier("b")],
            key: vec![identifier("a"), identifier("b")],
            after: after.map(KeyTuple::new),
            limit: 2,
        }
    }

    #[test]
    fn snapshot_is_immutable_and_composite_paging_is_lexicographic() {
        let source = InMemorySource::new("source", "database");
        source.add_table(
            table(),
            vec![column("a", 0), column("b", 1)],
            vec![
                vec![DbValue::Signed(2), DbValue::Signed(0)],
                vec![DbValue::Signed(1), DbValue::Signed(2)],
                vec![DbValue::Signed(1), DbValue::Signed(1)],
            ],
        );
        let snapshot = source.capture_snapshot().unwrap();
        let mut reader = source
            .open_reader(&snapshot, CancellationToken::default())
            .unwrap();
        source.add_table(table(), vec![column("a", 0), column("b", 1)], Vec::new());

        let first = reader.select_page(&request(None)).unwrap();
        assert_eq!(
            first.rows()[0],
            vec![DbValue::Signed(1), DbValue::Signed(1)]
        );
        assert_eq!(
            first.rows()[1],
            vec![DbValue::Signed(1), DbValue::Signed(2)]
        );
        let second = reader
            .select_page(&request(Some(vec![DbValue::Signed(1), DbValue::Signed(2)])))
            .unwrap();
        assert_eq!(
            second.rows(),
            &[vec![DbValue::Signed(2), DbValue::Signed(0)]]
        );
        assert!(reader.read_only_evidence().server_enforced);
    }

    #[test]
    fn target_insert_is_transactional_and_readable() {
        let target = InMemoryTarget::default();
        let mut batch = RowBatch::new(vec![column("a", 0), column("b", 1)], 1, 0);
        batch
            .try_push(vec![DbValue::Signed(1), DbValue::Signed(2)], 0)
            .unwrap();
        let mut writer = target.open_writer(CancellationToken::default()).unwrap();
        writer.begin().unwrap();
        writer.insert(&table(), &batch).unwrap();
        writer.rollback().unwrap();
        assert!(target.rows(&table()).is_empty());

        writer.begin().unwrap();
        writer.insert(&table(), &batch).unwrap();
        writer.commit().unwrap();
        let mut verifier = target.open_verifier(CancellationToken::default()).unwrap();
        assert_eq!(
            verifier.select_page(&request(None)).unwrap().rows(),
            batch.rows()
        );
    }

    #[test]
    fn cancellation_prevents_a_partial_commit() {
        let target = InMemoryTarget::default();
        let cancellation = CancellationToken::default();
        let mut writer = target.open_writer(cancellation.clone()).unwrap();
        let mut batch = RowBatch::new(vec![column("a", 0), column("b", 1)], 1, 0);
        batch
            .try_push(vec![DbValue::Signed(1), DbValue::Signed(2)], 0)
            .unwrap();
        writer.begin().unwrap();
        writer.insert(&table(), &batch).unwrap();
        cancellation.cancel();
        assert_eq!(writer.commit(), Err(ConnectionError::Cancelled));
        assert!(target.rows(&table()).is_empty());
    }

    #[test]
    fn duplicate_page_key_is_rejected_before_copy() {
        let source = InMemorySource::new("source", "database");
        source.add_table(
            table(),
            vec![column("a", 0), column("b", 1)],
            vec![
                vec![DbValue::Signed(1), DbValue::Signed(1)],
                vec![DbValue::Signed(1), DbValue::Signed(1)],
            ],
        );
        let snapshot = source.capture_snapshot().unwrap();
        let mut reader = source
            .open_reader(&snapshot, CancellationToken::default())
            .unwrap();
        assert!(matches!(
            reader.select_page(&request(None)),
            Err(ConnectionError::InvalidRequest(message)) if message.contains("not unique")
        ));
    }
}
