//! Canonical metadata for every built-in `GEN-*` diagnostic.

use super::{DiagnosticCategory, DiagnosticDefinition, TypicalSeverity};

macro_rules! define_diagnostics {
    (
        $(
            $name:ident,
            $code:literal,
            $title:literal,
            $category:ident,
            $severity:ident,
            $summary:literal;
        )+
    ) => {
        $(
            pub const $name: DiagnosticDefinition = DiagnosticDefinition {
                code: $code,
                title: $title,
                category: DiagnosticCategory::$category,
                typical_severity: TypicalSeverity::$severity,
                summary: $summary,
            };
        )+

        /// Every built-in diagnostic definition, in code order.
        pub static ALL: &[&DiagnosticDefinition] = &[$(&$name),+];
    };
}

define_diagnostics! {
    AFFIX_MISSING_VALUE, "GEN-AFFIX-MISSING-VALUE", "Affix value is missing", Modifier, Error, "A prefix or suffix modifier does not declare the value it should add.";
    BOOLEAN_PROBABILITY, "GEN-BOOLEAN-PROBABILITY", "Boolean probability is invalid", Generator, Error, "A boolean generator probability falls outside the supported zero-to-one range.";
    BYTES_LENGTH_RANGE, "GEN-BYTES-LENGTH-RANGE", "Byte length range is invalid", Generator, Error, "A bytes generator has incompatible minimum and maximum lengths.";
    CANCELLED, "GEN-CANCELLED", "Generation was cancelled", Runtime, Error, "The generation loop observed a cancellation request before it finished.";
    CASE_INVALID_MODE, "GEN-CASE-INVALID-MODE", "Case mode is invalid", Modifier, Error, "A case modifier names a mode other than the supported lower, upper, or title transformations.";
    CHILD_COUNT_IMPOSSIBLE, "GEN-CHILD-COUNT-IMPOSSIBLE", "Child row count is impossible", Selection, Error, "The resolved child count cannot satisfy the configured minimum fan-out for every parent.";
    CHOICE_EMPTY, "GEN-CHOICE-EMPTY", "Choice list is empty", Generator, Error, "A choice generator has no values from which to draw.";
    CHOICE_INVALID_VALUE, "GEN-CHOICE-INVALID-VALUE", "Choice value is incompatible", Generator, Error, "A choice value cannot be represented by the target column type.";
    CHOICE_MISSING_VALUES, "GEN-CHOICE-MISSING-VALUES", "Choice values are missing", Generator, Error, "A choice generator omits its required values list.";
    CLAMP_MISSING_BOUNDS, "GEN-CLAMP-MISSING-BOUNDS", "Clamp bounds are missing", Modifier, Error, "A clamp modifier supplies neither a minimum nor a maximum bound.";
    CLAMP_RANGE, "GEN-CLAMP-RANGE", "Clamp range is invalid", Modifier, Error, "A clamp modifier's maximum is below its minimum.";
    COLUMN_CYCLE, "GEN-COLUMN-CYCLE", "Column dependency cycle", Selection, Error, "Column and planner read/write dependencies contain a cycle that no single planner owns.";
    COLUMN_OWNER_CONFLICT, "GEN-COLUMN-OWNER-CONFLICT", "Column has multiple owners", Selection, Error, "More than one generator or planner claims responsibility for the same output column.";
    COLUMN_OWNER_MISSING, "GEN-COLUMN-OWNER-MISSING", "Column has no owner", Selection, Error, "A generated column has no generator, planner, or structural source that can produce its value.";
    COMMERCE_MONEY_RANGE, "GEN-COMMERCE-MONEY-RANGE", "Money range is invalid", Generator, Error, "A commerce money generator has incompatible amount bounds.";
    COMMERCE_MONEY_SCALE, "GEN-COMMERCE-MONEY-SCALE", "Money scale is invalid", Generator, Error, "A commerce money generator requests an unsupported decimal scale.";
    CONFIG_COMPLETE_MODEL, "GEN-CONFIG-COMPLETE-MODEL", "Complete model overrides the profiled base", Config, Warning, "A complete model was supplied with a source dump, so the complete model is authoritative.";
    CONFIG_IO, "GEN-CONFIG-IO", "Configuration file could not be read", Config, Error, "The root configuration or an imported configuration file could not be read.";
    CONFIG_PARSE, "GEN-CONFIG-PARSE", "Configuration YAML is invalid", Config, Error, "A configuration document is not valid YAML or contains a duplicate mapping key.";
    CONFIG_ROLE, "GEN-CONFIG-ROLE", "Configuration role is invalid", Config, Error, "A merged document cannot be parsed as its declared model or overrides role.";
    COORDINATE_BOUNDS, "GEN-COORDINATE-BOUNDS", "Coordinate bounds are invalid", Planner, Error, "A coordinate planner bounding box is inverted or outside valid latitude or longitude limits.";
    COORDINATE_COLUMN_MISSING, "GEN-COORDINATE-COLUMN-MISSING", "Coordinate column is missing", Planner, Error, "A coordinate planner role names a column that does not exist.";
    COORDINATE_PRECISION, "GEN-COORDINATE-PRECISION", "Coordinate precision is invalid", Planner, Error, "A coordinate planner precision falls outside the supported range.";
    COPY_MISSING_SOURCE, "GEN-COPY-MISSING-SOURCE", "Copy source is missing", Generator, Error, "A copy generator omits the sibling column it should read.";
    COPY_TYPE_MISMATCH, "GEN-COPY-TYPE-MISMATCH", "Copy source type is incompatible", Generator, Error, "A copy generator's source value cannot populate the target column type.";
    COPY_UNKNOWN_FIELD, "GEN-COPY-UNKNOWN-FIELD", "Copy source column is unknown", Generator, Error, "A copy generator names a sibling column that does not exist.";
    COUNT_CONTROL_CONFLICT, "GEN-COUNT-CONTROL-CONFLICT", "Global count controls conflict", Selection, Error, "Mutually exclusive global row-count controls were supplied together.";
    DECIMAL_RANGE, "GEN-DECIMAL-RANGE", "Decimal range is invalid", Generator, Error, "A decimal generator's maximum is below its minimum.";
    DECIMAL_SCALE, "GEN-DECIMAL-SCALE", "Decimal scale is invalid", Generator, Error, "A decimal generator's scale cannot represent its configured range or target type.";
    DETACHED_DEPENDENCY, "GEN-DETACHED-DEPENDENCY", "Optional dependency was detached", Selection, Warning, "A nullable foreign key points to an excluded table and its rendered constraint was omitted.";
    EMIT_IO, "GEN-EMIT-IO", "Resolved model could not be written", Rendering, Error, "Writing the resolved model configuration failed.";
    EMIT_SERIALIZE, "GEN-EMIT-SERIALIZE", "Resolved model could not be serialized", Rendering, Error, "The resolved model could not be encoded as YAML.";
    EXCLUDED_DEPENDENCY, "GEN-EXCLUDED-DEPENDENCY", "Required dependency was excluded", Selection, Error, "A selected table depends on another table that filtering excluded.";
    FAMILY_SPOOL, "GEN-FAMILY-SPOOL", "Family spool failed", Runtime, Error, "A buffered cross-table planner could not write or replay its protected temporary data.";
    FILE_COLUMN_MISSING, "GEN-FILE-COLUMN-MISSING", "File metadata column is missing", Planner, Error, "A file metadata role names a column that does not exist.";
    FILE_EXTENSIONS, "GEN-FILE-EXTENSIONS", "File extension list is invalid", Planner, Error, "A file metadata planner has no usable recognized extension.";
    FILE_HASH_KIND, "GEN-FILE-HASH-KIND", "File hash kind is invalid", Planner, Error, "A file metadata planner requests an unsupported digest shape.";
    FILE_SIZE_RANGE, "GEN-FILE-SIZE-RANGE", "File size range is invalid", Planner, Error, "A file metadata size range is negative or inverted.";
    FOREIGN_KEY_CYCLE, "GEN-FK-CYCLE", "Nullable foreign-key cycle", Selection, Warning, "Nullable foreign keys form a cycle; the cyclic tables cannot all be ordered parent-before-child in the output.";
    FOREIGN_KEY_UNRESOLVED, "GEN-FOREIGN-KEY-UNRESOLVED", "Foreign key value cannot be resolved", Selection, Error, "A relationship-owned foreign-key column has no usable value source.";
    FORMAT_MISSING_TEMPLATE, "GEN-FORMAT-MISSING-TEMPLATE", "Format template is missing", Modifier, Error, "A format modifier omits its required template.";
    GAUSSIAN_MISSING_PARAMS, "GEN-GAUSSIAN-MISSING-PARAMS", "Gaussian parameters are missing", Generator, Error, "A gaussian generator omits required distribution parameters.";
    GAUSSIAN_NON_FINITE, "GEN-GAUSSIAN-NON-FINITE", "Gaussian parameter is not finite", Generator, Error, "A gaussian generator contains a NaN or infinite parameter.";
    GAUSSIAN_RANGE, "GEN-GAUSSIAN-RANGE", "Gaussian range is invalid", Generator, Error, "A gaussian generator's configured bounds are incompatible.";
    GENERATOR_TYPE, "GEN-GENERATOR-TYPE", "Generator does not support the column type", Generator, Error, "The selected generator cannot produce values for the target SQL type family.";
    GENERATOR_UNKNOWN, "GEN-GENERATOR-UNKNOWN", "Generator kind is unknown", Generator, Error, "The model names a generator kind that is not registered.";
    HISTOGRAM_ALL_ZERO, "GEN-HISTOGRAM-ALL-ZERO", "Histogram weights are all zero", Generator, Error, "A histogram has no bin with positive probability.";
    HISTOGRAM_EMPTY, "GEN-HISTOGRAM-EMPTY", "Histogram is empty", Generator, Error, "A histogram generator has no bins from which to draw.";
    HISTOGRAM_INVALID_BIN, "GEN-HISTOGRAM-INVALID-BIN", "Histogram bin is invalid", Generator, Error, "A histogram bin is malformed or cannot be represented by the target column.";
    HISTOGRAM_MISSING_BINS, "GEN-HISTOGRAM-MISSING-BINS", "Histogram bins are missing", Generator, Error, "A histogram generator omits its required bins.";
    HISTOGRAM_NON_FINITE, "GEN-HISTOGRAM-NON-FINITE", "Histogram value is not finite", Generator, Error, "A histogram bin contains a NaN or infinite bound or weight.";
    HISTOGRAM_RANGE, "GEN-HISTOGRAM-RANGE", "Histogram range is invalid", Generator, Error, "A histogram bin or overall range has incompatible bounds.";
    HISTOGRAM_UNSORTED, "GEN-HISTOGRAM-UNSORTED", "Histogram bins are unsorted", Generator, Error, "Histogram bin boundaries are not in ascending order.";
    IMPORT_COLLISION, "GEN-IMPORT-COLLISION", "Imported settings collide", Config, Error, "Two imports define the same configuration path without an unambiguous owner.";
    IMPORT_KIND, "GEN-IMPORT-KIND", "Imported document has the wrong kind", Config, Error, "An imported document is not an overrides document.";
    IMPORT_NESTED, "GEN-IMPORT-NESTED", "Nested imports are not allowed", Config, Error, "An imported overrides document declares its own imports.";
    IMPORT_REMOTE, "GEN-IMPORT-REMOTE", "Remote import is not allowed", Config, Error, "An import path is absolute or looks like a remote URL.";
    INCOMPLETE_ROWS, "GEN-INCOMPLETE-ROWS", "Row-count override is incomplete", Config, Error, "An override changes the row-count rule kind without supplying the fields that kind needs.";
    INFER_FAILED, "GEN-INFER-FAILED", "Model inference failed", Inference, Error, "Profiling evidence could not be converted into a complete synthetic model.";
    INFER_PLANNER_NOMINATE, "GEN-INFER-PLANNER-NOMINATE", "Schema matches an optional planner", Inference, Info, "The schema has the columns required by a planner that inference does not enable automatically.";
    INFER_SOURCE_DERIVED, "GEN-INFER-SOURCE-DERIVED", "Inference retained source-derived literals", Inference, Warning, "An inferred rule embeds bounded literal values observed in the source dump.";
    INFER_TABLE_UNKNOWN, "GEN-INFER-TABLE-UNKNOWN", "Profiled table is absent from the schema", Inference, Warning, "Profiling produced evidence for a table that the inferred schema does not contain.";
    INTEGER_RANGE, "GEN-INTEGER-RANGE", "Integer range is invalid", Generator, Error, "An integer generator's maximum is below its minimum.";
    INTERVAL_COLUMN_MISSING, "GEN-INTERVAL-COLUMN-MISSING", "Interval column is missing", Planner, Error, "A temporal interval role names a column that does not exist.";
    INTERVAL_DURATION, "GEN-INTERVAL-DURATION", "Interval duration is invalid", Planner, Error, "A temporal interval duration is negative, overflowing, or incompatible with inclusive ends.";
    INTERVAL_OPEN_END, "GEN-INTERVAL-OPEN-END", "Open interval cannot represent a missing end", Planner, Error, "Open rows are possible but the configured end column is not nullable.";
    INTERVAL_START, "GEN-INTERVAL-START", "Interval start configuration is invalid", Planner, Error, "A temporal interval start range is missing, unparsable, or inverted.";
    INTERVAL_TIMEZONE, "GEN-INTERVAL-TIMEZONE", "Interval timezone is invalid", Planner, Error, "A temporal interval names an unknown timezone.";
    INVALID_GLOB, "GEN-INVALID-GLOB", "Table glob is invalid", Selection, Error, "A table include or exclude pattern cannot be compiled as a glob.";
    JSON_VALUE_INVALID, "GEN-JSON-VALUE-INVALID", "JSON value is invalid", Generator, Error, "A JSON value generator contains text that is not valid JSON.";
    JUNCTION_COLUMN_MISSING, "GEN-JUNCTION-COLUMN-MISSING", "Junction column is missing", Planner, Error, "A junction planner role names a column that does not exist.";
    JUNCTION_EXHAUSTED, "GEN-JUNCTION-EXHAUSTED", "Junction pair space is exhausted", Planner, Error, "The requested junction rows exceed the number of distinct left/right pairs.";
    JUNCTION_KEY_UNSUPPORTED, "GEN-JUNCTION-KEY-UNSUPPORTED", "Junction key domain is unsupported", Planner, Error, "A junction relationship does not expose a dense key domain.";
    JUNCTION_RELATIONSHIP, "GEN-JUNCTION-RELATIONSHIP", "Junction relationship is invalid", Planner, Error, "A junction planner omits or names an unknown left or right relationship.";
    KEY_DOMAIN_UNSUPPORTED, "GEN-KEY-DOMAIN-UNSUPPORTED", "Relationship key domain is unsupported", Runtime, Error, "A relationship targets a parent key that cannot be enumerated as a supported dense domain.";
    LIFECYCLE_COLUMN_MISSING, "GEN-LIFECYCLE-COLUMN-MISSING", "Lifecycle column is missing", Planner, Error, "A lifecycle planner role names a column that does not exist.";
    LIFECYCLE_NULLABILITY, "GEN-LIFECYCLE-NULLABILITY", "Lifecycle column nullability is impossible", Planner, Error, "A timestamp can be absent in a reachable state but its target column is not nullable.";
    LIFECYCLE_RANGE, "GEN-LIFECYCLE-RANGE", "Lifecycle start range is invalid", Planner, Error, "A lifecycle start range is missing, unparsable, or inverted.";
    LIFECYCLE_STATES, "GEN-LIFECYCLE-STATES", "Lifecycle states are invalid", Planner, Error, "A lifecycle planner has an empty or duplicate state chain.";
    LIFECYCLE_STATUS_VOCABULARY, "GEN-LIFECYCLE-STATUS-VOCABULARY", "Lifecycle status vocabulary is inconsistent", Planner, Error, "A lifecycle column mapping names a state outside the declared state chain.";
    LIFECYCLE_STEP, "GEN-LIFECYCLE-STEP", "Lifecycle step is invalid", Planner, Error, "A lifecycle timestamp step is negative, overflowing, or uses an unknown unit.";
    LIFECYCLE_WEIGHTS, "GEN-LIFECYCLE-WEIGHTS", "Lifecycle weights are invalid", Planner, Error, "Lifecycle terminal-state weights are malformed, negative, or sum to zero.";
    LOSSY_TYPE, "GEN-LOSSY-TYPE", "Type conversion is lossy", Rendering, Warning, "Cross-dialect rendering cannot preserve a source column type exactly.";
    MAX_ROWS_CAPPED, "GEN-MAX-ROWS-CAPPED", "Maximum row cap reduced a table", Selection, Warning, "The final maximum-row limit reduced a table's resolved row count.";
    MISSING_COLUMN, "GEN-MISSING-COLUMN", "Override column does not exist", Config, Error, "An overrides document names a column absent from the base model.";
    MISSING_TABLE, "GEN-MISSING-TABLE", "Override table does not exist", Config, Error, "An overrides document names a table absent from the base model.";
    MODIFIER_TYPE, "GEN-MODIFIER-TYPE", "Modifier does not support the column type", Modifier, Error, "The selected modifier cannot transform the target SQL type family.";
    MODIFIER_UNKNOWN, "GEN-MODIFIER-UNKNOWN", "Modifier kind is unknown", Modifier, Error, "The model names a modifier kind that is not registered.";
    MONOTONIC_STEP, "GEN-MONOTONIC-STEP", "Monotonic step is invalid", Generator, Error, "A monotonic generator uses a zero or otherwise invalid step.";
    NULL_ON_NON_NULLABLE, "GEN-NULL-ON-NON-NULLABLE", "Null generator targets a non-nullable column", Generator, Error, "A rule can emit null for a column whose schema forbids null values.";
    NULL_RATE_MISSING_RATE, "GEN-NULL-RATE-MISSING-RATE", "Null-rate probability is missing", Modifier, Error, "A null-rate modifier omits its required rate.";
    NULL_RATE_ON_NON_NULLABLE, "GEN-NULL-RATE-ON-NON-NULLABLE", "Null-rate modifier targets a non-nullable column", Modifier, Error, "A null-rate modifier can emit null for a column whose schema forbids it.";
    NULL_RATE_RANGE, "GEN-NULL-RATE-RANGE", "Null-rate probability is invalid", Modifier, Error, "A null-rate probability falls outside the supported zero-to-one range.";
    OBSERVED_SAMPLE_ALL_ZERO, "GEN-OBSERVED-SAMPLE-ALL-ZERO", "Observed-sample weights are all zero", Generator, Error, "An observed sample has no value with positive probability.";
    OBSERVED_SAMPLE_EMPTY, "GEN-OBSERVED-SAMPLE-EMPTY", "Observed sample is empty", Generator, Error, "An observed-sample generator has no values from which to draw.";
    OBSERVED_SAMPLE_INVALID_VALUE, "GEN-OBSERVED-SAMPLE-INVALID-VALUE", "Observed-sample value is incompatible", Generator, Error, "An observed sample value cannot be represented by the target column type.";
    OBSERVED_SAMPLE_INVALID_WEIGHT, "GEN-OBSERVED-SAMPLE-INVALID-WEIGHT", "Observed-sample weight is invalid", Generator, Error, "An observed sample contains a negative, NaN, or infinite weight.";
    OBSERVED_SAMPLE_MISSING_VALUES, "GEN-OBSERVED-SAMPLE-MISSING-VALUES", "Observed-sample values are missing", Generator, Error, "An observed-sample generator omits its required values.";
    ORDER_FAMILY_CHILD_UNKNOWN, "GEN-ORDER-FAMILY-CHILD-UNKNOWN", "Order-family child table is unknown", Planner, Error, "An order-family planner names a child table absent from the model.";
    ORDER_FAMILY_COLUMN_MISSING, "GEN-ORDER-FAMILY-COLUMN-MISSING", "Order-family column is missing", Planner, Error, "A required parent or child money role is absent or names an unknown column.";
    ORDER_FAMILY_CONFIG, "GEN-ORDER-FAMILY-CONFIG", "Order-family configuration is invalid", Planner, Error, "An order-family planner omits or misconfigures a required top-level option.";
    ORDER_FAMILY_OVERFLOW, "GEN-ORDER-FAMILY-OVERFLOW", "Order-family arithmetic overflow", Planner, Error, "Order-family minor-unit arithmetic exceeds the representable range.";
    ORDER_FAMILY_RELATIONSHIP, "GEN-ORDER-FAMILY-RELATIONSHIP", "Order-family relationship is invalid", Planner, Error, "The named child relationship does not reference the planner's parent table.";
    ORDER_FAMILY_SCALE, "GEN-ORDER-FAMILY-SCALE", "Order-family currency scale is inconsistent", Planner, Error, "A configured money column does not match the planner's currency scale.";
    ORDER_FAMILY_UNKNOWN_FIELD, "GEN-ORDER-FAMILY-UNKNOWN-FIELD", "Order-family field is unknown", Planner, Error, "An order-family planner uses a removed flat field instead of the parent or child column maps.";
    ORDER_FAMILY_ZERO_LINES, "GEN-ORDER-FAMILY-ZERO-LINES", "Order-family line count is impossible", Planner, Error, "A configured line distribution can yield zero lines where the planner requires at least one.";
    OUTPUT_IO, "GEN-OUTPUT-IO", "Generated output could not be written", Runtime, Error, "Opening, writing, flushing, or publishing the generated SQL output failed.";
    OPERATOR_UNKNOWN_ARGUMENT, "GEN-OPERATOR-UNKNOWN-ARGUMENT", "Operator argument is unknown", Config, Error, "A generator, modifier, or planner configuration contains a key absent from that operator's descriptor.";
    OVERRIDES_NO_BASE, "GEN-OVERRIDES-NO-BASE", "Overrides document has no base model", Config, Error, "An overrides document was supplied without a source dump or other base model.";
    PATTERN_MISSING_MASK, "GEN-PATTERN-MISSING-MASK", "Pattern mask is missing", Generator, Error, "A pattern generator omits the mask that describes its output.";
    PLANNER_UNKNOWN, "GEN-PLANNER-UNKNOWN", "Planner kind is unknown", Planner, Error, "The model names a planner kind that is not registered.";
    POLYMORPHIC_COLUMN_MISSING, "GEN-POLYMORPHIC-COLUMN-MISSING", "Polymorphic column is missing", Planner, Error, "A polymorphic planner role names a column that does not exist.";
    POLYMORPHIC_KEY_UNSUPPORTED, "GEN-POLYMORPHIC-KEY-UNSUPPORTED", "Polymorphic key domain is unsupported", Planner, Error, "A polymorphic target does not expose a supported dense key domain.";
    POLYMORPHIC_TARGET_UNKNOWN, "GEN-POLYMORPHIC-TARGET-UNKNOWN", "Polymorphic target is unknown", Planner, Error, "A polymorphic target table or identifier column does not exist.";
    POLYMORPHIC_TARGETS, "GEN-POLYMORPHIC-TARGETS", "Polymorphic targets are invalid", Planner, Error, "A polymorphic planner has no usable target with rows and a valid weight.";
    PROFILE_DECODE_SKIPPED, "GEN-PROFILE-DECODE-SKIPPED", "Some source values could not be decoded", Profiling, Warning, "The profiler counted rows whose values could not all be decoded into column evidence.";
    PROFILE_SCHEMA_LATE, "GEN-PROFILE-SCHEMA-LATE", "Source schema arrived after data", Profiling, Warning, "The dump declared a table after more data rows than the bounded late-schema buffer could retain.";
    PROGRESS_COLUMN_MISSING, "GEN-PROGRESS-COLUMN-MISSING", "Progress column is missing", Planner, Error, "A progress-counter role names a column that does not exist.";
    PROGRESS_COMPLETION, "GEN-PROGRESS-COMPLETION", "Progress completion state is impossible", Planner, Error, "Configured completion timestamps or nullability cannot represent every reachable progress state.";
    PROGRESS_OBSERVED, "GEN-PROGRESS-OBSERVED", "Observed progress lacks evidence", Planner, Error, "Observed progress was requested where no planner evidence can form the required exact partition.";
    PROGRESS_OVERFLOW, "GEN-PROGRESS-OVERFLOW", "Progress counter can overflow", Planner, Error, "A possible total exceeds the capacity of one or more counter columns.";
    PROGRESS_PARTITION, "GEN-PROGRESS-PARTITION", "Progress partition mode is invalid", Planner, Error, "A progress planner names an unsupported partition mode.";
    PROGRESS_STATUS_VOCABULARY, "GEN-PROGRESS-STATUS-VOCABULARY", "Progress status vocabulary is incomplete", Planner, Error, "A reachable progress state has no status label available.";
    PROGRESS_TOTAL, "GEN-PROGRESS-TOTAL", "Progress total range is invalid", Planner, Error, "A progress total is negative or has an inverted range.";
    PROGRESS_WEIGHTS, "GEN-PROGRESS-WEIGHTS", "Progress weights are invalid", Planner, Error, "Progress-state weights are non-finite, negative, or sum to zero.";
    RANDOM_STRING_INVALID_ALPHABET, "GEN-RANDOM-STRING-INVALID-ALPHABET", "Random-string alphabet is invalid", Generator, Error, "A random-string generator has an empty or unsupported alphabet.";
    RANGED_INTEGER_RANGE, "GEN-RANGED-INTEGER-RANGE", "Ranged integer bounds are invalid", Generator, Error, "A ranged-integer generator has incompatible bounds.";
    REGISTRY_ALIAS_DUPLICATE, "GEN-REGISTRY-ALIAS-DUPLICATE", "Registry alias is duplicated", Registry, Error, "Two registered operators claim the same alias.";
    REGISTRY_ALIAS_SHADOWS_KIND, "GEN-REGISTRY-ALIAS-SHADOWS-KIND", "Registry alias shadows a kind", Registry, Error, "An operator alias collides with a registered canonical kind.";
    REGISTRY_DUPLICATE, "GEN-REGISTRY-DUPLICATE", "Registry kind is duplicated", Registry, Error, "The same canonical operator kind was registered more than once.";
    RELATIONSHIP_UNKNOWN, "GEN-RELATIONSHIP-UNKNOWN", "Relationship is unknown", Planner, Error, "A rule or planner names a relationship absent from the table schema.";
    RELATIVE_MISSING_SOURCE, "GEN-RELATIVE-MISSING-SOURCE", "Relative source is missing", Generator, Error, "A relative generator omits the sibling column it should offset.";
    RELATIVE_RANGE, "GEN-RELATIVE-RANGE", "Relative offset range is invalid", Generator, Error, "A relative generator has negative, inverted, or overflowing offset bounds.";
    RELATIVE_UNKNOWN_SOURCE, "GEN-RELATIVE-UNKNOWN-SOURCE", "Relative source column is unknown", Generator, Error, "A relative generator names a sibling column that does not exist.";
    RENDER_COPY_DEFAULT, "GEN-RENDER-COPY-DEFAULT", "PostgreSQL COPY cannot render DEFAULT", Rendering, Error, "A COPY row requests DEFAULT for a column without a database default, identity, or generated expression.";
    RENDER_IO, "GEN-RENDER-IO", "SQL rendering failed", Rendering, Error, "The renderer could not write or format generated SQL.";
    RENDER_WARNING, "GEN-RENDER-WARNING", "SQL renderer reported a warning", Rendering, Warning, "The renderer encountered a non-fatal condition without a more specific diagnostic code.";
    REQUEST_OUTPUT, "GEN-REQUEST-OUTPUT", "Generation output request is invalid", Runtime, Error, "The requested output or resolved-model destination is unavailable or conflicting.";
    REQUEST_SOURCE, "GEN-REQUEST-SOURCE", "Generation source is missing", Runtime, Error, "Neither a source dump nor a complete model configuration was supplied.";
    ROUND_MISSING_SCALE, "GEN-ROUND-MISSING-SCALE", "Round scale is missing", Modifier, Error, "A round modifier omits the number of decimal places to retain.";
    ROUND_SCALE_RANGE, "GEN-ROUND-SCALE-RANGE", "Round scale is invalid", Modifier, Error, "A round modifier's requested scale falls outside the supported range.";
    ROWS_CYCLE, "GEN-ROWS-CYCLE", "Row-count dependency cycle", Selection, Error, "Parent-derived table row-count rules contain a dependency cycle.";
    ROWS_OBSERVED_MISSING, "GEN-ROWS-OBSERVED-MISSING", "Observed row count is unavailable", Selection, Error, "An observed row-count rule has no attached profile count to resolve.";
    SCHEMA_MISMATCH, "GEN-SCHEMA-MISMATCH", "Override schema assertion does not match", Config, Error, "An override's schema name or create statement disagrees with the base model.";
    SEQUENCE_ZERO_STEP, "GEN-SEQUENCE-ZERO-STEP", "Sequence step is zero", Generator, Error, "A sequence generator cannot advance because its configured step is zero.";
    SOFT_DELETE_COLUMN_MISSING, "GEN-SOFT-DELETE-COLUMN-MISSING", "Soft-delete column is missing", Planner, Error, "A soft-delete role names a column that does not exist.";
    SOFT_DELETE_NULLABILITY, "GEN-SOFT-DELETE-NULLABILITY", "Soft-delete nullability is impossible", Planner, Error, "Non-deleted rows are possible but the deleted timestamp column is not nullable.";
    SOFT_DELETE_RANGE, "GEN-SOFT-DELETE-RANGE", "Soft-delete timestamp range is invalid", Planner, Error, "A deletion timestamp range is missing, unparsable, or inverted.";
    SOURCE_FINGERPRINT, "GEN-SOURCE-FINGERPRINT", "Source fingerprint does not match", Config, Variable, "An overrides document's source fingerprint differs from the base model under the configured policy.";
    SOURCE_IO, "GEN-SOURCE-IO", "Source dump could not be profiled", Runtime, Error, "Reading or profiling the source SQL dump failed.";
    SOURCE_VALUES, "GEN-SOURCE-VALUES", "Generated output replays literal values", Privacy, Advisory, "One or more rules replay literal values and should be reviewed before sharing generated output.";
    STRING_LENGTH_RANGE, "GEN-STRING-LENGTH-RANGE", "String length range is invalid", Generator, Error, "A string generator has incompatible minimum and maximum lengths.";
    TABLE_COUNT_CONFLICT, "GEN-TABLE-COUNT-CONFLICT", "Per-table count controls conflict", Selection, Error, "The same table matches both an absolute and a scaled row-count override.";
    TABLE_SCALE_INVALID, "GEN-TABLE-SCALE-INVALID", "Per-table scale is invalid", Selection, Error, "A table scale is negative, non-finite, or otherwise unsupported.";
    TEMPLATE_INVALID_PART, "GEN-TEMPLATE-INVALID-PART", "Template part is invalid", Generator, Error, "A template part is neither a supported literal nor a valid sibling-column reference.";
    TEMPLATE_MISSING_PARTS, "GEN-TEMPLATE-MISSING-PARTS", "Template parts are missing", Generator, Error, "A template generator omits its required parts list.";
    TEMPLATE_UNKNOWN_FIELD, "GEN-TEMPLATE-UNKNOWN-FIELD", "Template field is unknown", Generator, Error, "A template generator references a sibling column that does not exist.";
    TENANT_COLUMN_MISSING, "GEN-TENANT-COLUMN-MISSING", "Tenant-family column is missing", Planner, Error, "A tenant-family role names a column that does not exist.";
    TENANT_KEY_UNSUPPORTED, "GEN-TENANT-KEY-UNSUPPORTED", "Tenant-family key domain is unsupported", Planner, Error, "The parent relationship does not expose a supported dense key domain.";
    TENANT_PARTITION, "GEN-TENANT-PARTITION", "Tenant partition is invalid", Planner, Error, "The requested tenant partition count cannot be formed from the available parent rows.";
    TENANT_RELATIONSHIP, "GEN-TENANT-RELATIONSHIP", "Tenant-family relationship is invalid", Planner, Error, "A tenant-family planner omits or names an unknown parent relationship.";
    TIMESTAMPS_COLUMN_MISSING, "GEN-TIMESTAMPS-COLUMN-MISSING", "Timestamp column is missing", Planner, Error, "A temporal timestamps role names a column that does not exist.";
    TIMESTAMPS_DELAY, "GEN-TIMESTAMPS-DELAY", "Timestamp delay is invalid", Planner, Error, "A temporal delay is negative, overflowing, inverted, or uses an unknown unit.";
    TIMESTAMPS_RANGE, "GEN-TIMESTAMPS-RANGE", "Timestamp range is invalid", Planner, Error, "A created timestamp range is missing, unparsable, or inverted.";
    TREE_BRANCHING, "GEN-TREE-BRANCHING", "Tree branching limit is invalid", Planner, Error, "A hierarchy tree branching limit is below one.";
    TREE_COLUMN_MISSING, "GEN-TREE-COLUMN-MISSING", "Tree parent column is missing", Planner, Error, "A hierarchy tree parent role names a column that does not exist.";
    TREE_DEPTH, "GEN-TREE-DEPTH", "Tree depth is invalid", Planner, Error, "A hierarchy tree maximum depth is below one.";
    TREE_REQUIRED_CYCLE, "GEN-TREE-REQUIRED-CYCLE", "Tree root cannot be represented", Planner, Error, "The self-reference column is non-nullable, so the hierarchy cannot create a root row.";
    TREE_ROOT_RATIO, "GEN-TREE-ROOT-RATIO", "Tree root ratio is invalid", Planner, Error, "A hierarchy tree root ratio falls outside the supported zero-to-one range.";
    TRUNCATE_MISSING_MAX_LENGTH, "GEN-TRUNCATE-MISSING-MAX-LENGTH", "Truncate length is missing", Modifier, Error, "A truncate modifier omits its required maximum length.";
    UNIQUE_INVALID_ON_EXHAUSTION, "GEN-UNIQUE-INVALID-ON-EXHAUSTION", "Unique exhaustion policy is invalid", Modifier, Error, "A unique modifier names an unsupported behavior for exhausted retries.";
    UNIQUE_WIDEN_UNSUPPORTED, "GEN-UNIQUE-WIDEN-UNSUPPORTED", "Unique widening is unsupported", Modifier, Error, "The target type has no safe widening strategy for unique-value exhaustion.";
    VERIFY_FAILED, "GEN-VERIFY-FAILED", "Generated output failed verification", Verification, Error, "One or more exact verification checks failed and the destination was not published.";
    VERIFY_IO, "GEN-VERIFY-IO", "Verification input could not be read", Verification, Error, "The verifier could not read the staged generated output.";
    VERIFY_MODE, "GEN-VERIFY-MODE", "Verification mode is invalid", Verification, Error, "Verification was requested with an incompatible generation mode.";
    VERIFY_NO_FILE, "GEN-VERIFY-NO-FILE", "Verification requires a file", Verification, Error, "Verification cannot audit output that has no real file destination.";
    VERIFY_NOTCHECKED, "GEN-VERIFY-NOTCHECKED", "Some capabilities were not checked", Verification, Warning, "Verification passed, but one or more capabilities had no exact audit predicate.";
    VERIFY_PARSE, "GEN-VERIFY-PARSE", "Generated SQL could not be parsed for verification", Verification, Error, "The verifier could not parse the staged generated SQL.";
    VERIFY_PARTIAL_PUBLISH, "GEN-VERIFY-PARTIAL-PUBLISH", "Verified outputs were only partially published", Verification, Error, "Publishing a verified output set failed after one destination had already changed.";
    VERIFY_STAGE, "GEN-VERIFY-STAGE", "Verification staging failed", Verification, Error, "Creating or writing protected temporary output for verification failed.";
    WEIGHTED_CHOICE_ALL_ZERO, "GEN-WEIGHTED-CHOICE-ALL-ZERO", "Weighted-choice weights are all zero", Generator, Error, "A weighted choice has no entry with positive probability.";
    WEIGHTED_CHOICE_EMPTY, "GEN-WEIGHTED-CHOICE-EMPTY", "Weighted choice is empty", Generator, Error, "A weighted-choice generator has no choices from which to draw.";
    WEIGHTED_CHOICE_INVALID_ENTRY, "GEN-WEIGHTED-CHOICE-INVALID-ENTRY", "Weighted-choice entry is invalid", Generator, Error, "A weighted choice contains a malformed entry.";
    WEIGHTED_CHOICE_INVALID_VALUE, "GEN-WEIGHTED-CHOICE-INVALID-VALUE", "Weighted-choice value is incompatible", Generator, Error, "A weighted choice value cannot be represented by the target column type.";
    WEIGHTED_CHOICE_INVALID_WEIGHT, "GEN-WEIGHTED-CHOICE-INVALID-WEIGHT", "Weighted-choice weight is invalid", Generator, Error, "A weighted choice contains a negative, NaN, or infinite weight.";
    WEIGHTED_CHOICE_MISSING_CHOICES, "GEN-WEIGHTED-CHOICE-MISSING-CHOICES", "Weighted choices are missing", Generator, Error, "A weighted-choice generator omits its required choices.";
}

/// Looks up canonical metadata for a built-in code.
pub fn find(code: &str) -> Option<&'static DiagnosticDefinition> {
    ALL.iter()
        .copied()
        .find(|definition| definition.code == code)
}
