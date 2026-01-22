# Morphable/Polymorphic Ownership Support

**Status**: Investigation Complete, Deferred to v2.x  
**Last Updated**: 2025-12-20

## Overview

This document describes the requirements for supporting SQL dumps that use Laravel-style **polymorphic relationships** (morphable ownership) for tenant sharding.

## What is Morphable Ownership?

In Laravel and similar ORMs, polymorphic relationships allow a single table to belong to multiple different parent tables using a type/id column pair:

```sql
CREATE TABLE `accommodation_places` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(191) NOT NULL,
  `owner_type` varchar(191) NOT NULL,   -- e.g., 'App\\Festival', 'App\\Venue', 'App\\Group'
  `owner_id` bigint unsigned NOT NULL,  -- ID in the owner_type table
  ...
);
```

Example data:

```sql
INSERT INTO `accommodation_places` VALUES
  (1, 'Augustin Hotel', 'App\\Festival', 3, ...),
  (2, 'Thon Hotel', 'App\\Venue', 5, ...);
```

The `owner_type` column contains the fully-qualified class name (e.g., `App\Festival`), and `owner_id` references the PK in that model's table.

## Dump Analysis: dump_2025_10_28.sql

### Statistics

- **Tables**: 281
- **Primary polymorphic patterns**: 43+ tables with `owner_type`/`owner_id`
- **Total polymorphic column pairs**: 30+ different patterns

### Polymorphic Patterns Found

| Pattern                                 | Count | Example Tables                          |
| --------------------------------------- | ----- | --------------------------------------- |
| `owner_type` + `owner_id`               | 43    | accommodation_places, events, documents |
| `commentable_type` + `commentable_id`   | 1     | comments                                |
| `auditable_type` + `auditable_id`       | 1     | audits                                  |
| `notepaddable_type` + `notepaddable_id` | 1     | notepads                                |
| `boardable_type` + `boardable_id`       | 1     | boards                                  |
| `trackable_type` + `trackable_id`       | 2     | completion_items, completion_trackers   |
| `formable_type` + `formable_id`         | 1     | forms                                   |
| `assignable_type` + `assignable_id`     | 1     | assignables                             |
| `invitable_type` + `invitable_id`       | 1     | invites                                 |
| `person_type` + `person_id`             | 3     | contacts, accredees                     |
| `listable_type` + `listable_id`         | 4     | lists                                   |
| `templateable_type` + `templateable_id` | 3     | templates                               |
| `categorable_type` + `categorable_id`   | 1     | categories                              |
| `place_type` + `place_id`               | 3     | locations                               |
| `recurrable_type`                       | 147   | recurring patterns (audit log)          |

### Model Class Names Found in Data

From the `audits` table and other sources:

- `App\Festival`
- `App\Venue`
- `App\Group`
- `App\Event`
- `App\User`
- `App\Room`
- `App\Invite`
- `App\PartnerCompany`
- `App\PartnerContact`
- `App\Performance`
- `App\Document`
- `App\Assignment`
- And many more...

## Why Current Shard Command Can't Handle This

### 1. No Standard FK Declarations

Polymorphic relationships are **not declared as SQL foreign keys**. They exist only at the application/ORM level:

```sql
-- This does NOT exist:
FOREIGN KEY (`owner_id`) REFERENCES `festivals` (`id`)

-- Instead, we have:
KEY `accommodation_places_owner_type_owner_id_index` (`owner_type`, `owner_id`)
```

### 2. Dynamic Target Tables

The target table is determined by the `owner_type` value:

- `App\Festival` → `festivals` table
- `App\Venue` → `venues` table
- `App\Group` → `groups` table

This requires:

1. Parsing the class name from each row
2. Mapping class names to table names
3. Looking up the referenced PK in the appropriate table

### 3. Class-to-Table Mapping

Laravel uses conventions but they're configurable:

- `App\Festival` → `festivals` (pluralized, snake_case)
- `App\PartnerCompany` → `partner_companies`
- `App\User` → `users`

Custom mappings are also possible in Laravel.

## Requirements for v2.x Support

### Config File Extensions

```yaml
# shard.yaml - Extended for polymorphic support
tenant:
  column: festival_id # Direct tenant column

  # Polymorphic tenant identification
  morphable:
    # Tables where ownership is via owner_type/owner_id
    owner_type: owner_type
    owner_id: owner_id

    # Root tenant class
    root_class: App\Festival

    # Class-to-table mapping (auto-detected if follows Laravel conventions)
    class_map:
      App\Festival: festivals
      App\Venue: venues
      App\Group: groups
      App\Event: events
      App\User: users
      App\PartnerCompany: partner_companies

tables:
  accommodation_places:
    role: tenant-dependent
    morph_column: owner_type
    morph_id: owner_id

  events:
    role: tenant-root # Events belong to Festival directly
    morph_column: owner_type # But ALSO can be owned by Group

  audits:
    role: lookup # Skip - too big and not tenant-specific
```

### Processing Algorithm

1. **Pass 1: Schema Analysis**
   - Detect polymorphic column pairs (`*_type` + `*_id`)
   - Auto-detect class-to-table mappings
2. **Pass 2: Tenant Root Identification**
   - Find rows where `owner_type = 'App\Festival'` AND `owner_id = <tenant_value>`
3. **Pass 3: Transitive Closure**
   - For each selected row, check if OTHER tables reference it morphably
   - Build complete tenant subgraph

### Challenges

1. **Performance**: Must parse `owner_type` string from every row
2. **Multiple Owners**: A table might be owned by Festival directly AND by Festival's children
3. **Audit Tables**: `audits` table references ALL models - special handling needed
4. **Nullable Morphs**: Some morphable relationships are optional

## Recommended Approach

### Phase 1: Config-Driven (v2.0.0)

- Explicit config declaring morphable relationships
- Class-to-table mapping in config
- Manual specification of tenant root class

### Phase 2: Auto-Detection (v2.1.0)

- Scan for `*_type`/`*_id` column pairs
- Parse model class names from data
- Apply Laravel naming conventions automatically

### Phase 3: Smart Classification (v2.2.0)

- Detect which classes are tenant roots vs dependents
- Build complete morphable dependency graph
- Handle circular morphable references

## Workaround for Current Version

For users with polymorphic schemas, recommend:

1. **Pre-processing**: Add explicit `festival_id` column to key tables via migration
2. **Denormalization**: Copy tenant ID into child tables before sharding
3. **Post-processing**: Use application logic to filter after split

Example migration approach:

```sql
-- Add direct tenant column to polymorphic tables
ALTER TABLE accommodation_places ADD COLUMN festival_id BIGINT;
UPDATE accommodation_places
SET festival_id = owner_id
WHERE owner_type = 'App\\Festival';

-- Now shard normally
sql-splitter shard dump.sql --tenant-column festival_id --tenant-value 5
```

## Effort Estimate

| Component                     | Effort   | Priority |
| ----------------------------- | -------- | -------- |
| Config schema for polymorphic | 4h       | High     |
| Class-to-table mapping        | 4h       | High     |
| Morph column parsing          | 6h       | High     |
| Transitive closure for morphs | 8h       | Medium   |
| Auto-detection                | 12h      | Low      |
| Testing                       | 8h       | High     |
| **Total**                     | **~42h** | -        |

## Related Documents

- [SHARD_FEATURE.md](SHARD_FEATURE.md) - Base shard implementation
- [../ROADMAP.md](../ROADMAP.md) - Overall project roadmap

## Appendix: Tables Using owner_type in dump_2025_10_28.sql

```
accommodation_guests
accommodation_places
accommodation_rooms
advances
boards
check_lists
completion_trackers
contacts
documents
events
economies
forms
...
(43+ tables total)
```
