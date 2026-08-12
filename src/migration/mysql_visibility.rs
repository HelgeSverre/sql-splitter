//! Typed MySQL metadata-visibility and authorization evidence.
//!
//! The source data reader is intentionally not trusted to enumerate all
//! catalog or grant state. A distinct metadata administrator captures the
//! authoritative catalog and the grant-table records represented here.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION: u16 = 2;

const REQUIRED_METADATA_PRIVILEGES: [&str; 5] =
    ["EVENT", "SELECT", "SHOW VIEW", "SHOW_ROUTINE", "TRIGGER"];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlAccountIdentity {
    pub user: String,
    pub host: String,
}

impl MySqlAccountIdentity {
    pub fn validate(&self) -> Result<(), MySqlVisibilityError> {
        if self.user.contains('\0') || self.host.is_empty() || self.host.contains('\0') {
            return Err(MySqlVisibilityError::InvalidAccount);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlOperationalAccountPurpose {
    CatalogReader,
    MetadataAdministrator,
    FreezeAdministrator,
    ServerAdministrator,
    OperationalRole,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlOperationalAccountExclusion {
    pub purpose: MySqlOperationalAccountPurpose,
    pub account: MySqlAccountIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlGrantTableColumn {
    pub table: String,
    pub column: String,
    pub data_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MySqlProxyTarget {
    Account { account: MySqlAccountIdentity },
    AnyAccount,
}

impl MySqlProxyTarget {
    fn validate(&self) -> Result<(), MySqlVisibilityError> {
        match self {
            Self::Account { account } => account.validate(),
            Self::AnyAccount => Ok(()),
        }
    }

    fn account(&self) -> Option<&MySqlAccountIdentity> {
        match self {
            Self::Account { account } => Some(account),
            Self::AnyAccount => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MySqlGrantRecord {
    StaticGlobal {
        account: MySqlAccountIdentity,
        privilege: String,
    },
    DynamicGlobal {
        account: MySqlAccountIdentity,
        privilege: String,
        grantable: bool,
    },
    Database {
        account: MySqlAccountIdentity,
        database: String,
        privilege: String,
    },
    Table {
        account: MySqlAccountIdentity,
        database: String,
        table: String,
        privilege: String,
        grantor: String,
    },
    Column {
        account: MySqlAccountIdentity,
        database: String,
        table: String,
        column: String,
        privilege: String,
    },
    Routine {
        account: MySqlAccountIdentity,
        database: String,
        routine: String,
        routine_type: String,
        privilege: String,
        grantor: String,
    },
    Proxy {
        account: MySqlAccountIdentity,
        target: MySqlProxyTarget,
        grantable: bool,
    },
    RoleEdge {
        role: MySqlAccountIdentity,
        grantee: MySqlAccountIdentity,
        admin_option: bool,
    },
    DefaultRole {
        account: MySqlAccountIdentity,
        role: MySqlAccountIdentity,
    },
    PartialRevoke {
        account: MySqlAccountIdentity,
        database: String,
        privileges: Vec<String>,
    },
}

impl MySqlGrantRecord {
    pub fn canonical_id(&self) -> Result<String, MySqlVisibilityError> {
        self.validate()?;
        Ok(format!(
            "mysql-grant:{}",
            hex::encode(Sha256::digest(serde_json::to_vec(self)?))
        ))
    }

    pub fn validate(&self) -> Result<(), MySqlVisibilityError> {
        let require_text = |value: &str| {
            if value.is_empty() || value.contains('\0') {
                Err(MySqlVisibilityError::InvalidGrantRecord)
            } else {
                Ok(())
            }
        };
        match self {
            Self::StaticGlobal { account, privilege } => {
                account.validate()?;
                require_text(privilege)
            }
            Self::DynamicGlobal {
                account, privilege, ..
            } => {
                account.validate()?;
                require_text(privilege)
            }
            Self::Database {
                account,
                database,
                privilege,
            } => {
                account.validate()?;
                require_text(database)?;
                require_text(privilege)
            }
            Self::Table {
                account,
                database,
                table,
                privilege,
                grantor,
            } => {
                account.validate()?;
                require_text(database)?;
                require_text(table)?;
                require_text(privilege)?;
                require_text(grantor)
            }
            Self::Column {
                account,
                database,
                table,
                column,
                privilege,
            } => {
                account.validate()?;
                require_text(database)?;
                require_text(table)?;
                require_text(column)?;
                require_text(privilege)
            }
            Self::Routine {
                account,
                database,
                routine,
                routine_type,
                privilege,
                grantor,
            } => {
                account.validate()?;
                require_text(database)?;
                require_text(routine)?;
                require_text(routine_type)?;
                require_text(privilege)?;
                require_text(grantor)
            }
            Self::Proxy {
                account, target, ..
            } => {
                account.validate()?;
                target.validate()
            }
            Self::RoleEdge { role, grantee, .. } => {
                role.validate()?;
                grantee.validate()
            }
            Self::DefaultRole { account, role } => {
                account.validate()?;
                role.validate()
            }
            Self::PartialRevoke {
                account,
                database,
                privileges,
            } => {
                account.validate()?;
                require_text(database)?;
                if privileges.is_empty()
                    || privileges
                        .iter()
                        .any(|privilege| require_text(privilege).is_err())
                    || !is_strictly_sorted_unique(privileges)
                {
                    return Err(MySqlVisibilityError::InvalidGrantRecord);
                }
                Ok(())
            }
        }
    }

    pub fn involved_accounts(&self) -> Vec<&MySqlAccountIdentity> {
        match self {
            Self::StaticGlobal { account, .. }
            | Self::DynamicGlobal { account, .. }
            | Self::Database { account, .. }
            | Self::Table { account, .. }
            | Self::Column { account, .. }
            | Self::Routine { account, .. }
            | Self::PartialRevoke { account, .. } => vec![account],
            Self::Proxy {
                account, target, ..
            } => target
                .account()
                .map_or_else(|| vec![account], |target| vec![account, target]),
            Self::RoleEdge { role, grantee, .. } => vec![role, grantee],
            Self::DefaultRole { account, role } => vec![account, role],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlGrantInventory {
    pub partial_revokes_enabled: bool,
    pub grant_table_columns: Vec<MySqlGrantTableColumn>,
    pub accounts: Vec<MySqlAccountIdentity>,
    pub records: Vec<MySqlGrantRecord>,
    pub unknown_privilege_classes: Vec<String>,
}

impl MySqlGrantInventory {
    pub fn validate(&self) -> Result<(), MySqlVisibilityError> {
        if !is_strictly_sorted_unique(&self.grant_table_columns)
            || !is_strictly_sorted_unique(&self.accounts)
            || !is_strictly_sorted_unique(&self.records)
            || !is_strictly_sorted_unique(&self.unknown_privilege_classes)
        {
            return Err(MySqlVisibilityError::UnstableInventoryOrder);
        }
        for column in &self.grant_table_columns {
            if column.table.is_empty()
                || column.column.is_empty()
                || column.data_type.is_empty()
                || column.table.contains('\0')
                || column.column.contains('\0')
                || column.data_type.contains('\0')
            {
                return Err(MySqlVisibilityError::InvalidGrantTableSchema);
            }
        }
        for account in &self.accounts {
            account.validate()?;
        }
        for record in &self.records {
            record.validate()?;
            if record
                .involved_accounts()
                .iter()
                .any(|account| self.accounts.binary_search(account).is_err())
            {
                return Err(MySqlVisibilityError::UnknownGrantAccount);
            }
        }
        if !self.partial_revokes_enabled
            && self
                .records
                .iter()
                .any(|record| matches!(record, MySqlGrantRecord::PartialRevoke { .. }))
        {
            return Err(MySqlVisibilityError::InvalidGrantRecord);
        }
        if self
            .unknown_privilege_classes
            .iter()
            .any(|value| value.is_empty() || value.contains('\0'))
        {
            return Err(MySqlVisibilityError::InvalidGrantTableSchema);
        }
        Ok(())
    }

    pub fn canonical_hash(&self) -> Result<String, MySqlVisibilityError> {
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlMetadataVisibilityEvidence {
    pub schema_version: u16,
    pub endpoint_identity: String,
    pub database_identity: String,
    pub server_uuid: String,
    pub catalog_reader_tls_binding: String,
    pub metadata_administrator_tls_binding: String,
    pub catalog_reader_account: MySqlAccountIdentity,
    pub metadata_administrator_account: MySqlAccountIdentity,
    pub active_administrator_roles: Vec<MySqlAccountIdentity>,
    pub effective_administrator_privileges: Vec<String>,
    pub operational_exclusions: Vec<MySqlOperationalAccountExclusion>,
    pub catalog_reader_fingerprint: String,
    pub authoritative_catalog_fingerprint: String,
    pub grant_inventory_digest: String,
    pub grant_inventory: MySqlGrantInventory,
}

impl MySqlMetadataVisibilityEvidence {
    pub fn validate(&self) -> Result<(), MySqlVisibilityError> {
        if self.schema_version != MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION {
            return Err(MySqlVisibilityError::UnsupportedVersion);
        }
        for value in [
            &self.endpoint_identity,
            &self.database_identity,
            &self.server_uuid,
            &self.catalog_reader_tls_binding,
            &self.metadata_administrator_tls_binding,
        ] {
            if value.is_empty() || value.contains('\0') {
                return Err(MySqlVisibilityError::EmptyBinding);
            }
        }
        require_sha256(&self.catalog_reader_fingerprint)?;
        require_sha256(&self.authoritative_catalog_fingerprint)?;
        require_sha256(&self.grant_inventory_digest)?;
        self.catalog_reader_account.validate()?;
        self.metadata_administrator_account.validate()?;
        if self.catalog_reader_account == self.metadata_administrator_account {
            return Err(MySqlVisibilityError::AdministratorNotSeparate);
        }
        if !is_strictly_sorted_unique(&self.active_administrator_roles)
            || !is_strictly_sorted_unique(&self.effective_administrator_privileges)
            || !is_strictly_sorted_unique(&self.operational_exclusions)
        {
            return Err(MySqlVisibilityError::UnstableInventoryOrder);
        }
        self.grant_inventory.validate()?;
        if std::iter::once(&self.catalog_reader_account)
            .chain(std::iter::once(&self.metadata_administrator_account))
            .chain(self.active_administrator_roles.iter())
            .chain(
                self.operational_exclusions
                    .iter()
                    .map(|exclusion| &exclusion.account),
            )
            .any(|account| {
                self.grant_inventory
                    .accounts
                    .binary_search(account)
                    .is_err()
            })
        {
            return Err(MySqlVisibilityError::UnknownGrantAccount);
        }
        if self.grant_inventory.canonical_hash()? != self.grant_inventory_digest {
            return Err(MySqlVisibilityError::InventoryDigestMismatch);
        }
        let effective = self
            .effective_administrator_privileges
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if REQUIRED_METADATA_PRIVILEGES
            .iter()
            .any(|required| !effective.contains(required))
        {
            return Err(MySqlVisibilityError::InsufficientAdministratorPrivileges);
        }
        if !self.grant_inventory.unknown_privilege_classes.is_empty() {
            return Err(MySqlVisibilityError::UnknownPrivilegeClass);
        }
        let excluded = self
            .operational_exclusions
            .iter()
            .map(|exclusion| &exclusion.account)
            .collect::<BTreeSet<_>>();
        if excluded.len() != self.operational_exclusions.len()
            || !self.operational_exclusions.iter().any(|exclusion| {
                exclusion.purpose == MySqlOperationalAccountPurpose::CatalogReader
                    && exclusion.account == self.catalog_reader_account
            })
            || !self.operational_exclusions.iter().any(|exclusion| {
                exclusion.purpose == MySqlOperationalAccountPurpose::MetadataAdministrator
                    && exclusion.account == self.metadata_administrator_account
            })
            || self
                .active_administrator_roles
                .iter()
                .any(|role| !excluded.contains(role))
        {
            return Err(MySqlVisibilityError::InvalidOperationalExclusions);
        }
        Ok(())
    }

    pub fn canonical_hash(&self) -> Result<String, MySqlVisibilityError> {
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }

    pub fn non_operational_records(&self) -> Vec<&MySqlGrantRecord> {
        let excluded = self
            .operational_exclusions
            .iter()
            .map(|exclusion| &exclusion.account)
            .collect::<BTreeSet<_>>();
        self.grant_inventory
            .records
            .iter()
            .filter(|record| {
                record
                    .involved_accounts()
                    .iter()
                    .any(|account| !excluded.contains(account))
            })
            .collect()
    }

    pub fn same_authorization_as(&self, accepted: &Self) -> bool {
        self.endpoint_identity == accepted.endpoint_identity
            && self.database_identity == accepted.database_identity
            && self.server_uuid == accepted.server_uuid
            && self.catalog_reader_tls_binding == accepted.catalog_reader_tls_binding
            && self.metadata_administrator_tls_binding
                == accepted.metadata_administrator_tls_binding
            && self.catalog_reader_account == accepted.catalog_reader_account
            && self.metadata_administrator_account == accepted.metadata_administrator_account
            && self.active_administrator_roles == accepted.active_administrator_roles
            && self.effective_administrator_privileges
                == accepted.effective_administrator_privileges
            && self.operational_exclusions == accepted.operational_exclusions
            && self.grant_inventory_digest == accepted.grant_inventory_digest
            && self.grant_inventory == accepted.grant_inventory
    }
}

#[derive(Debug, Error)]
pub enum MySqlVisibilityError {
    #[error("unsupported MySQL metadata-visibility schema version")]
    UnsupportedVersion,
    #[error("invalid MySQL account identity")]
    InvalidAccount,
    #[error("invalid MySQL grant record")]
    InvalidGrantRecord,
    #[error("invalid MySQL grant-table schema")]
    InvalidGrantTableSchema,
    #[error("MySQL grant evidence has an unstable or duplicate order")]
    UnstableInventoryOrder,
    #[error("MySQL grant record refers to an unknown account")]
    UnknownGrantAccount,
    #[error("MySQL metadata-visibility binding is empty")]
    EmptyBinding,
    #[error("MySQL metadata administrator is not separate from the source reader")]
    AdministratorNotSeparate,
    #[error("MySQL metadata administrator lacks a required effective privilege")]
    InsufficientAdministratorPrivileges,
    #[error("MySQL grant inventory contains an unknown privilege class")]
    UnknownPrivilegeClass,
    #[error("MySQL operational-account exclusions are incomplete or ambiguous")]
    InvalidOperationalExclusions,
    #[error("MySQL grant inventory digest differs from its typed inventory")]
    InventoryDigestMismatch,
    #[error("invalid SHA-256 digest in MySQL metadata-visibility evidence")]
    InvalidDigest,
    #[error("cannot serialize MySQL metadata-visibility evidence")]
    Serialization(#[from] serde_json::Error),
}

fn require_sha256(value: &str) -> Result<(), MySqlVisibilityError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(MySqlVisibilityError::InvalidDigest)
    }
}

fn is_strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(user: &str) -> MySqlAccountIdentity {
        MySqlAccountIdentity {
            user: user.into(),
            host: "%".into(),
        }
    }

    fn evidence() -> MySqlMetadataVisibilityEvidence {
        let source = account("source");
        let admin = account("admin");
        let mut records = REQUIRED_METADATA_PRIVILEGES
            .iter()
            .map(|privilege| {
                if *privilege == "SHOW_ROUTINE" {
                    MySqlGrantRecord::DynamicGlobal {
                        account: admin.clone(),
                        privilege: (*privilege).into(),
                        grantable: false,
                    }
                } else {
                    MySqlGrantRecord::StaticGlobal {
                        account: admin.clone(),
                        privilege: (*privilege).into(),
                    }
                }
            })
            .collect::<Vec<_>>();
        records.sort();
        let inventory = MySqlGrantInventory {
            partial_revokes_enabled: true,
            grant_table_columns: vec![MySqlGrantTableColumn {
                table: "user".into(),
                column: "Host".into(),
                data_type: "char".into(),
            }],
            accounts: vec![admin.clone(), source.clone()],
            records,
            unknown_privilege_classes: Vec::new(),
        };
        MySqlMetadataVisibilityEvidence {
            schema_version: MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
            endpoint_identity: "mysql://source/app".into(),
            database_identity: "app".into(),
            server_uuid: "uuid".into(),
            catalog_reader_tls_binding: "source-tls".into(),
            metadata_administrator_tls_binding: "admin-tls".into(),
            catalog_reader_account: source.clone(),
            metadata_administrator_account: admin.clone(),
            active_administrator_roles: Vec::new(),
            effective_administrator_privileges: REQUIRED_METADATA_PRIVILEGES
                .iter()
                .map(ToString::to_string)
                .collect(),
            operational_exclusions: vec![
                MySqlOperationalAccountExclusion {
                    purpose: MySqlOperationalAccountPurpose::CatalogReader,
                    account: source,
                },
                MySqlOperationalAccountExclusion {
                    purpose: MySqlOperationalAccountPurpose::MetadataAdministrator,
                    account: admin,
                },
            ],
            catalog_reader_fingerprint: "a".repeat(64),
            authoritative_catalog_fingerprint: "b".repeat(64),
            grant_inventory_digest: inventory.canonical_hash().unwrap(),
            grant_inventory: inventory,
        }
    }

    #[test]
    fn visibility_evidence_binds_inventory_and_required_privileges() {
        let value = evidence();
        value.validate().unwrap();
        let original_hash = value.canonical_hash().unwrap();

        let mut changed = value.clone();
        changed.grant_inventory.records.pop();
        assert!(matches!(
            changed.validate(),
            Err(MySqlVisibilityError::InventoryDigestMismatch)
        ));

        let mut changed = value;
        changed.effective_administrator_privileges.pop();
        assert!(matches!(
            changed.validate(),
            Err(MySqlVisibilityError::InsufficientAdministratorPrivileges)
        ));
        assert_ne!(changed.canonical_hash().ok(), Some(original_hash));
    }

    #[test]
    fn business_grants_are_not_hidden_by_operational_exclusions() {
        let mut value = evidence();
        let business = account("business");
        value.grant_inventory.accounts.push(business.clone());
        value.grant_inventory.accounts.sort();
        value
            .grant_inventory
            .records
            .push(MySqlGrantRecord::Database {
                account: business,
                database: "app".into(),
                privilege: "SELECT".into(),
            });
        value.grant_inventory.records.sort();
        value.grant_inventory_digest = value.grant_inventory.canonical_hash().unwrap();
        value.validate().unwrap();
        assert_eq!(value.non_operational_records().len(), 1);
    }

    #[test]
    fn evidence_rejects_roles_or_exclusions_absent_from_authoritative_accounts() {
        let mut value = evidence();
        let missing_role = account("missing_role");
        value.active_administrator_roles.push(missing_role.clone());
        value
            .operational_exclusions
            .push(MySqlOperationalAccountExclusion {
                purpose: MySqlOperationalAccountPurpose::OperationalRole,
                account: missing_role,
            });
        value.operational_exclusions.sort();
        assert!(matches!(
            value.validate(),
            Err(MySqlVisibilityError::UnknownGrantAccount)
        ));
    }

    #[test]
    fn proxy_wildcard_is_not_misclassified_as_a_missing_account() {
        let mut value = evidence();
        let admin = value.metadata_administrator_account.clone();
        value.grant_inventory.records.push(MySqlGrantRecord::Proxy {
            account: admin,
            target: MySqlProxyTarget::AnyAccount,
            grantable: true,
        });
        value.grant_inventory.records.sort();
        value.grant_inventory_digest = value.grant_inventory.canonical_hash().unwrap();
        value.validate().unwrap();
        assert!(value.non_operational_records().is_empty());
    }
}
