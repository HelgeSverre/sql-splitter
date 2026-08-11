//! Strict keyed-row comparison with complete target coverage.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedRow<K, V> {
    pub key: K,
    pub value: V,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedRow<K, V> {
    pub key: K,
    pub expected: V,
    pub actual: V,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationReport<K, V> {
    pub missing: Vec<KeyedRow<K, V>>,
    pub extra: Vec<KeyedRow<K, V>>,
    pub changed: Vec<ChangedRow<K, V>>,
    pub duplicate_expected_keys: Vec<K>,
    pub duplicate_target_keys: Vec<K>,
    pub expected_count: usize,
    pub target_count: usize,
}

impl<K, V> Default for VerificationReport<K, V> {
    fn default() -> Self {
        Self {
            missing: Vec::new(),
            extra: Vec::new(),
            changed: Vec::new(),
            duplicate_expected_keys: Vec::new(),
            duplicate_target_keys: Vec::new(),
            expected_count: 0,
            target_count: 0,
        }
    }
}

impl<K, V> VerificationReport<K, V> {
    pub fn is_exact(&self) -> bool {
        self.missing.is_empty()
            && self.extra.is_empty()
            && self.changed.is_empty()
            && self.duplicate_expected_keys.is_empty()
            && self.duplicate_target_keys.is_empty()
            && self.expected_count == self.target_count
    }
}

pub fn verify_keyed_rows<K, V>(
    expected: impl IntoIterator<Item = KeyedRow<K, V>>,
    target: impl IntoIterator<Item = KeyedRow<K, V>>,
) -> VerificationReport<K, V>
where
    K: Ord + Clone,
    V: Eq + Clone,
{
    let mut expected_map = BTreeMap::new();
    let mut target_map = BTreeMap::new();
    let mut duplicate_expected_keys = Vec::new();
    let mut duplicate_target_keys = Vec::new();
    let mut expected_count = 0;
    let mut target_count = 0;
    for row in expected {
        expected_count += 1;
        if expected_map.insert(row.key.clone(), row.value).is_some() {
            duplicate_expected_keys.push(row.key);
        }
    }
    for row in target {
        target_count += 1;
        if target_map.insert(row.key.clone(), row.value).is_some() {
            duplicate_target_keys.push(row.key);
        }
    }
    let expected = expected_map;
    let target = target_map;
    let mut report = VerificationReport {
        duplicate_expected_keys,
        duplicate_target_keys,
        expected_count,
        target_count,
        ..VerificationReport::default()
    };
    for (key, value) in &expected {
        match target.get(key) {
            None => report.missing.push(KeyedRow {
                key: key.clone(),
                value: value.clone(),
            }),
            Some(actual) if actual != value => report.changed.push(ChangedRow {
                key: key.clone(),
                expected: value.clone(),
                actual: actual.clone(),
            }),
            Some(_) => {}
        }
    }
    for (key, value) in &target {
        if !expected.contains_key(key) {
            report.extra.push(KeyedRow {
                key: key.clone(),
                value: value.clone(),
            });
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reports_all_mismatch_classes_and_boundaries() {
        let report = verify_keyed_rows(
            [
                KeyedRow { key: 2, value: "b" },
                KeyedRow { key: 3, value: "c" },
                KeyedRow { key: 4, value: "d" },
            ],
            [
                KeyedRow { key: 1, value: "a" },
                KeyedRow {
                    key: 2,
                    value: "changed",
                },
                KeyedRow { key: 5, value: "e" },
            ],
        );
        assert_eq!(report.missing.len(), 2);
        assert_eq!(report.extra.len(), 2);
        assert_eq!(report.changed.len(), 1);
        assert!(!report.is_exact());
    }
    #[test]
    fn empty_expected_rejects_target_rows() {
        let report = verify_keyed_rows::<u8, _>(
            [],
            [KeyedRow {
                key: 1,
                value: "extra",
            }],
        );
        assert!(!report.is_exact());
        assert_eq!(report.extra.len(), 1);
    }
}
