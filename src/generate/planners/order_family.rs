//! The `commerce.order_family` planner: a cross-table parent/child money family.
//!
//! Unlike the same-table [`super::interval`] / [`super::progress`] planners, this
//! planner coordinates TWO tables — a parent `orders` table and a named child
//! `order_items` table — as one correlated family. For each parent row it draws a
//! line count (from the child table's `rows.distribution`, injected by the
//! compiler so the distribution is never duplicated), generates every child line
//! (quantity, unit price, per-line discount, per-line tax, line total), and
//! derives the parent aggregates (subtotal, discount, tax, shipping, grand
//! total). The child rows are handed back to the engine, which spools them
//! through a protected [`crate::generate::output::FamilyBuffer`] and renders them
//! at the child table's dependency position.
//!
//! # Exact minor-unit money
//!
//! Every monetary quantity is an integer count of currency minor units (an
//! `i128`); the equations are pure checked-integer arithmetic and never touch a
//! float. A tax or discount *rate* is converted once, at compile time, to an
//! exact rational `num / 1_000_000` so even `rate * amount` stays integral. An
//! order-level tax or discount total is rounded once, then apportioned across the
//! lines by the selected [`Rounding`] method so the child line values sum to the
//! parent total EXACTLY — no float drift can make a child sum disagree with its
//! parent. The three invariants hold for every order by construction:
//!
//! * `sum(line_subtotal) == subtotal`, `sum(line_discount) == discount_total`,
//!   `sum(line_tax) == tax_total`, `sum(line_total) == subtotal - discount + tax`;
//! * `grand_total == subtotal - discount + tax + shipping`.
//!
//! # Determinism across spill thresholds
//!
//! Each order family is seeded by its PARENT ROW INDEX, so the family's values
//! depend only on that index — never on how many families preceded it or whether
//! the child buffer has spilled to disk. A run therefore produces byte-identical
//! output at a 1 KiB family budget and a 1 GiB one; the spill only changes where
//! rows live, not what they are.

use rand::RngExt;
use serde_yaml_ng::Value;

use crate::diagnostic::DiagnosticBag;
use crate::generate::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledPlanner, Determinism,
    PlannerDescriptor, PlannerFactory, Verification,
};
use crate::generate::seed::{SeedRoot, StreamId};
use crate::generate::value::{GenerateError, GeneratedValue};
use crate::synthetic::model::PlannerConfig;
use crate::synthetic::schema::{PortableTable, SqlTypeFamily};

/// The rational denominator every tax/discount rate is expressed over, giving
/// six decimal digits of rate precision (enough for `0.0`, `0.08`, `0.25`, …)
/// while keeping `rate * amount` an exact integer operation.
const RATE_DEN: i128 = 1_000_000;

/// The private config key under which the compiler injects the child-table facts
/// the parent-scoped planner cannot see on its own (the child column types, the
/// line-count distribution, and the resolved relationship). Documented as an
/// internal contract between [`super::super::compiler`] and this planner.
pub const FAMILY_FACTS_KEY: &str = "__family_facts";

/// Static description of the `commerce.order_family` planner.
pub static COMMERCE_ORDER_FAMILY_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "commerce.order_family",
    aliases: &[],
    summary: "Coordinates an orders/order_items parent-child family with exact minor-unit money.",
    arguments: &[
        ArgumentSpec {
            name: "children",
            required: true,
            summary: "Name of the line-item table coordinated by this parent planner.",
        },
        ArgumentSpec {
            name: "relationship",
            required: true,
            summary: "Relationship on the child table that references the parent table.",
        },
        ArgumentSpec {
            name: "columns",
            required: true,
            summary: "Maps parent subtotal, total, and optional discount, tax, and shipping roles.",
        },
        ArgumentSpec {
            name: "child_columns",
            required: true,
            summary:
                "Maps child quantity, unit price, line total, and optional discount and tax roles.",
        },
        ArgumentSpec {
            name: "currency_scale",
            required: true,
            summary: "Decimal scale shared by every parent and child money column.",
        },
        ArgumentSpec {
            name: "rounding",
            required: true,
            summary: "Exact residual allocation mode: largest_remainder, last_line, or bankers.",
        },
        ArgumentSpec {
            name: "quantity",
            required: false,
            summary: "Per-line integer quantity range.",
        },
        ArgumentSpec {
            name: "unit_price",
            required: false,
            summary: "Per-line unit-price range in minor or major currency units.",
        },
        ArgumentSpec {
            name: "tax",
            required: false,
            summary: "Fixed or weighted tax-rate configuration.",
        },
        ArgumentSpec {
            name: "discount",
            required: false,
            summary: "Fixed or weighted discount-rate configuration.",
        },
        ArgumentSpec {
            name: "shipping",
            required: false,
            summary: "Fixed shipping amount in minor or major currency units.",
        },
    ],
    writes: ColumnScope::Configured,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Buffered,
    verification: Verification::Unsupported,
    cross_table: true,
};

/// Factory for the `commerce.order_family` planner.
pub struct OrderFamilyFactory;

impl PlannerFactory for OrderFamilyFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &COMMERCE_ORDER_FAMILY_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        compile_order_family(config, context)
            .map(|planner| Box::new(planner) as Box<dyn CompiledPlanner>)
    }
}

// --- Rounding / allocation --------------------------------------------------

/// How an order-level total is apportioned across its lines so the parts sum to
/// the total exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rounding {
    /// Distribute the leftover minor units to the lines with the largest
    /// fractional remainders (the Hamilton / largest-remainder method).
    LargestRemainder,
    /// Floor every line, then dump the entire rounding residual on the last line.
    LastLine,
    /// Banker's rounding: give the leftover units to the largest remainders,
    /// breaking exact ties toward the line whose floor is odd (rounding to even).
    Bankers,
}

impl Rounding {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "largest_remainder" => Some(Rounding::LargestRemainder),
            "last_line" => Some(Rounding::LastLine),
            "bankers" => Some(Rounding::Bankers),
            _ => None,
        }
    }
}

/// Apportion `total` minor units across `weights` (each `>= 0`) so the returned
/// parts are non-negative and sum to `total` EXACTLY, using `mode`.
///
/// The proportional share of line `i` is the rational `total * weights[i] /
/// sum(weights)`; the floors are assigned first and the leftover units (there are
/// fewer than `weights.len()`) are handed out per `mode`. All arithmetic is
/// `i128`; no float rounding can perturb the exact sum.
#[cfg(test)]
fn apportion(total: i128, weights: &[i128], mode: Rounding) -> Vec<i128> {
    let count = weights.len();
    let weight_total: i128 = weights.iter().sum();
    if count == 0 || total == 0 {
        return vec![0; count];
    }
    if weight_total <= 0 {
        // Degenerate: no weight to apportion by. Put everything on the last line
        // so the sum still equals `total`.
        let mut parts = vec![0i128; count];
        parts[count - 1] = total;
        return parts;
    }

    let mut parts = Vec::with_capacity(count);
    let mut remainders = Vec::with_capacity(count);
    let mut allocated: i128 = 0;
    for &weight in weights {
        let numerator = total * weight;
        let floor = numerator.div_euclid(weight_total);
        parts.push(floor);
        remainders.push(numerator.rem_euclid(weight_total));
        allocated += floor;
    }

    let mut leftover = total - allocated;
    match mode {
        Rounding::LastLine => {
            // The whole residual lands on the final line.
            parts[count - 1] += leftover;
        }
        Rounding::LargestRemainder | Rounding::Bankers => {
            let mut order: Vec<usize> = (0..count).collect();
            order.sort_by(|&a, &b| {
                remainders[b]
                    .cmp(&remainders[a])
                    .then_with(|| tie_break(mode, &parts, a, b))
            });
            for &index in order.iter().take(leftover.max(0) as usize) {
                parts[index] += 1;
            }
            leftover = 0;
        }
    }
    let _ = leftover;
    parts
}

/// Order two lines whose fractional remainders tie. `LargestRemainder` keeps the
/// lower index first (stable); `Bankers` prefers the line whose current floor is
/// odd, so the extra unit rounds it toward even.
#[cfg(test)]
fn tie_break(mode: Rounding, parts: &[i128], a: usize, b: usize) -> std::cmp::Ordering {
    match mode {
        Rounding::Bankers => {
            let a_odd = parts[a].rem_euclid(2) == 1;
            let b_odd = parts[b].rem_euclid(2) == 1;
            b_odd.cmp(&a_odd).then(a.cmp(&b))
        }
        _ => a.cmp(&b),
    }
}

// --- Rate selection ---------------------------------------------------------

/// A discrete distribution over exact rational rates (`num / RATE_DEN`), sampled
/// once per order.
#[derive(Debug, Clone)]
struct RateChoice {
    /// `(rate numerator over RATE_DEN, cumulative weight)` pairs.
    cumulative: Vec<(i128, f64)>,
    total_weight: f64,
}

impl RateChoice {
    /// A fixed zero rate (the default when no tax/discount is configured).
    fn zero() -> Self {
        Self {
            cumulative: vec![(0, 1.0)],
            total_weight: 1.0,
        }
    }

    /// A single fixed rate.
    fn fixed(rate_num: i128) -> Self {
        Self {
            cumulative: vec![(rate_num, 1.0)],
            total_weight: 1.0,
        }
    }

    /// A weighted choice over `rates`, each paired with a positive weight.
    fn weighted(rates: &[i128], weights: &[f64]) -> Self {
        let mut cumulative = Vec::with_capacity(rates.len());
        let mut acc = 0.0;
        for (rate, weight) in rates.iter().zip(weights) {
            acc += weight.max(0.0);
            cumulative.push((*rate, acc));
        }
        Self {
            total_weight: acc,
            cumulative,
        }
    }

    /// Sample a rate numerator from a uniform draw `r` in `[0, 1)`.
    fn sample(&self, r: f64) -> i128 {
        if self.total_weight <= 0.0 {
            return self.cumulative.first().map_or(0, |(rate, _)| *rate);
        }
        let target = r * self.total_weight;
        for (rate, cumulative) in &self.cumulative {
            if target < *cumulative {
                return *rate;
            }
        }
        self.cumulative.last().map_or(0, |(rate, _)| *rate)
    }
}

// --- Compiled planner -------------------------------------------------------

/// A parent aggregate column the planner writes, with its schema name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentRole {
    Subtotal,
    Discount,
    Tax,
    Shipping,
    Total,
}

/// A child line column the planner writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildRole {
    Quantity,
    UnitPrice,
    Discount,
    Tax,
    LineTotal,
}

/// An integer draw bounded by an inclusive `[min, max]` range.
#[derive(Debug, Clone, Copy)]
struct IntRange {
    min: i128,
    max: i128,
}

impl IntRange {
    fn draw(&self, rng: &mut rand_chacha::ChaCha8Rng) -> i128 {
        if self.max <= self.min {
            self.min
        } else {
            rng.random_range(self.min..=self.max)
        }
    }
}

/// How a per-order line count is drawn from the child table's `rows.distribution`
/// — the SOLE source of line counts. The distribution's *shape* (its kind and
/// mean), not just its `[min, max]` bounds, drives the draw so an order carries
/// about `mean` lines, matching the fan-out the child table would have had.
#[derive(Debug, Clone, Copy)]
struct LineDist {
    min: i128,
    max: i128,
    mean: f64,
    kind: LineKind,
}

/// The line-count draw shape, mapped from the child fan-out distribution kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineKind {
    /// A deterministic count of `round(mean)` (the `fixed` fan-out).
    Fixed,
    /// A uniform draw over `[min, max]` (the `uniform` fan-out).
    Uniform,
    /// A mean-respecting count around `mean` (`observed`/`poisson`/`histogram`).
    MeanRespecting,
}

impl LineDist {
    /// Draw one order's line count, honoring the distribution shape and clamped
    /// to the inclusive `[min, max]` bounds.
    fn draw(&self, rng: &mut rand_chacha::ChaCha8Rng) -> i128 {
        let raw = match self.kind {
            LineKind::Fixed => self.mean.round() as i128,
            LineKind::Uniform => {
                if self.max <= self.min {
                    self.min
                } else {
                    rng.random_range(self.min..=self.max)
                }
            }
            LineKind::MeanRespecting => poisson(rng, self.mean),
        };
        raw.clamp(self.min, self.max.max(self.min))
    }
}

/// A Poisson draw with the given `mean`, so a family's per-order line count
/// averages `mean` rather than the midpoint of its bounds. Knuth's method for a
/// small mean (line counts are small); a normal approximation past 30 keeps a
/// large mean from looping. Non-negative.
fn poisson(rng: &mut rand_chacha::ChaCha8Rng, mean: f64) -> i128 {
    if mean <= 0.0 {
        return 0;
    }
    if mean < 30.0 {
        let limit = (-mean).exp();
        let mut k = 0i128;
        let mut product = 1.0f64;
        loop {
            k += 1;
            product *= rng.random::<f64>();
            if product <= limit {
                break;
            }
        }
        k - 1
    } else {
        // Box–Muller standard normal, scaled to the Poisson's own variance.
        let u1 = rng.random::<f64>().max(f64::MIN_POSITIVE);
        let u2 = rng.random::<f64>();
        let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
        (mean + z * mean.sqrt()).round().max(0.0) as i128
    }
}

/// The compiled `commerce.order_family` planner.
pub struct OrderFamilyPlanner {
    /// The parent table name (stream identity + family seeding).
    parent_table: String,
    /// The named child table this family feeds.
    child_table: String,
    /// The relationship (declared on the child) that carries the FK to the parent.
    relationship: String,
    /// Parent aggregate columns in `generate_row` write order.
    parent_writes: Vec<String>,
    /// Parent roles parallel to `parent_writes`.
    parent_roles: Vec<ParentRole>,
    /// Child line columns in child-row value order.
    child_writes: Vec<String>,
    /// Child roles parallel to `child_writes`.
    child_roles: Vec<ChildRole>,
    /// Currency minor-unit scale for every money column.
    scale: u32,
    /// SQL type family of each parent money column (parallel to `parent_writes`).
    parent_families: Vec<SqlTypeFamily>,
    /// SQL type family of each child column (parallel to `child_writes`).
    child_families: Vec<SqlTypeFamily>,
    rounding: Rounding,
    quantity: IntRange,
    unit_price_minor: IntRange,
    /// The child fan-out distribution the per-order line count is drawn from.
    lines: LineDist,
    tax: RateChoice,
    discount: RateChoice,
    /// Fixed shipping charge in minor units (0 when unconfigured).
    shipping_minor: i128,
    seed: SeedRoot,
    /// Bounded summary for the most recent parent row. Child rows are replayed
    /// lazily from this summary when the engine asks for them.
    pending_family: Option<OrderSummary>,
}

/// The bounded money summary of one order family.
///
/// It retains only scalar aggregates, allocation thresholds, and the RNG state
/// needed to replay raw lines. Memory is independent of the parent's fan-out.
struct OrderSummary {
    subtotal: i128,
    discount: i128,
    tax: i128,
    shipping: i128,
    grand_total: i128,
    line_count: usize,
    line_rng: rand_chacha::ChaCha8Rng,
    discount_plan: AllocationPlan,
    tax_plan: AllocationPlan,
}

/// One child line's computed minor-unit values.
struct LineTotals {
    quantity: i128,
    unit_price: i128,
    discount: i128,
    tax: i128,
    line_total: i128,
}

/// One replayable raw line before order-level discount/tax apportionment.
struct RawLine {
    quantity: i128,
    unit_price: i128,
    subtotal: i128,
}

/// Draw one raw line using the same RNG order as the original family
/// implementation: quantity first, then unit price.
fn draw_raw_line(
    rng: &mut rand_chacha::ChaCha8Rng,
    quantity: IntRange,
    unit_price: IntRange,
) -> Result<RawLine, GenerateError> {
    let quantity = quantity.draw(rng).max(0);
    let unit_price = unit_price.draw(rng).max(0);
    let subtotal = quantity
        .checked_mul(unit_price)
        .ok_or_else(|| overflow("line subtotal (quantity * unit_price)"))?;
    Ok(RawLine {
        quantity,
        unit_price,
        subtotal,
    })
}

/// Replays a fixed number of raw lines from a cloned per-parent RNG state.
struct RawLines {
    rng: rand_chacha::ChaCha8Rng,
    remaining: usize,
    quantity: IntRange,
    unit_price: IntRange,
}

impl RawLines {
    fn new(
        rng: rand_chacha::ChaCha8Rng,
        count: usize,
        quantity: IntRange,
        unit_price: IntRange,
    ) -> Self {
        Self {
            rng,
            remaining: count,
            quantity,
            unit_price,
        }
    }
}

impl Iterator for RawLines {
    type Item = Result<RawLine, GenerateError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(draw_raw_line(&mut self.rng, self.quantity, self.unit_price))
    }
}

/// Bounded representation of an exact proportional allocation.
///
/// Ranked rounding stores the cutoff remainder and tie counts instead of a
/// row-sized sorted vector. The cutoff is found by repeated streaming scans of
/// the replayable weights; this trades bounded CPU for memory independent of
/// fan-out while preserving Hamilton/banker's ordering exactly.
#[derive(Clone)]
struct AllocationPlan {
    total: i128,
    weight_total: i128,
    count: usize,
    mode: Rounding,
    leftover: i128,
    threshold: Option<i128>,
    tie_slots: usize,
    odd_ties: usize,
}

impl AllocationPlan {
    fn build<F, I>(total: i128, mode: Rounding, weights: F) -> Result<Self, GenerateError>
    where
        F: Fn() -> I,
        I: Iterator<Item = Result<i128, GenerateError>>,
    {
        let mut count = 0usize;
        let mut weight_total = 0i128;
        for weight in weights() {
            let weight = weight?.max(0);
            weight_total = weight_total
                .checked_add(weight)
                .ok_or_else(|| overflow("allocation weight total"))?;
            count += 1;
        }

        let mut plan = Self {
            total,
            weight_total,
            count,
            mode,
            leftover: 0,
            threshold: None,
            tie_slots: 0,
            odd_ties: 0,
        };
        if count == 0 || total == 0 || weight_total <= 0 {
            return Ok(plan);
        }

        let mut allocated = 0i128;
        for weight in weights() {
            let (floor, _) = proportional_share(total, weight?.max(0), weight_total)?;
            allocated = allocated
                .checked_add(floor)
                .ok_or_else(|| overflow("allocated family amount"))?;
        }
        plan.leftover = total
            .checked_sub(allocated)
            .ok_or_else(|| overflow("family allocation residual"))?;

        if mode == Rounding::LastLine || plan.leftover <= 0 {
            return Ok(plan);
        }

        let wanted = usize::try_from(plan.leftover)
            .map_err(|_| overflow("family allocation residual count"))?;
        // Remainders lie in [0, weight_total). Find the greatest cutoff with at
        // least `wanted` values at or above it: the wanted-th largest value.
        let mut low = 0i128;
        let mut high = weight_total;
        while low + 1 < high {
            let mid = low + (high - low) / 2;
            let mut at_or_above = 0usize;
            for weight in weights() {
                let (_, remainder) = proportional_share(total, weight?.max(0), weight_total)?;
                if remainder >= mid {
                    at_or_above += 1;
                }
            }
            if at_or_above >= wanted {
                low = mid;
            } else {
                high = mid;
            }
        }
        plan.threshold = Some(low);

        let mut greater = 0usize;
        let mut odd_ties = 0usize;
        for weight in weights() {
            let (floor, remainder) = proportional_share(total, weight?.max(0), weight_total)?;
            if remainder > low {
                greater += 1;
            } else if remainder == low && floor.rem_euclid(2) == 1 {
                odd_ties += 1;
            }
        }
        plan.tie_slots = wanted.saturating_sub(greater);
        plan.odd_ties = odd_ties;
        Ok(plan)
    }

    fn cursor(&self) -> AllocationCursor {
        AllocationCursor {
            plan: self.clone(),
            tied_seen: 0,
            odd_seen: 0,
            even_seen: 0,
        }
    }
}

/// Sequential cursor over one bounded allocation plan.
struct AllocationCursor {
    plan: AllocationPlan,
    tied_seen: usize,
    odd_seen: usize,
    even_seen: usize,
}

impl AllocationCursor {
    fn part(&mut self, index: usize, weight: i128) -> Result<i128, GenerateError> {
        if self.plan.count == 0 || self.plan.total == 0 {
            return Ok(0);
        }
        if self.plan.weight_total <= 0 {
            return Ok(if index + 1 == self.plan.count {
                self.plan.total
            } else {
                0
            });
        }

        let (floor, remainder) =
            proportional_share(self.plan.total, weight.max(0), self.plan.weight_total)?;
        if self.plan.mode == Rounding::LastLine {
            return if index + 1 == self.plan.count {
                floor
                    .checked_add(self.plan.leftover)
                    .ok_or_else(|| overflow("last-line allocation"))
            } else {
                Ok(floor)
            };
        }

        let Some(threshold) = self.plan.threshold else {
            return Ok(floor);
        };
        let receives_extra = if remainder > threshold {
            true
        } else if remainder < threshold {
            false
        } else {
            match self.plan.mode {
                Rounding::LargestRemainder => {
                    let selected = self.tied_seen < self.plan.tie_slots;
                    self.tied_seen += 1;
                    selected
                }
                Rounding::Bankers => {
                    let odd_slots = self.plan.tie_slots.min(self.plan.odd_ties);
                    if floor.rem_euclid(2) == 1 {
                        let selected = self.odd_seen < odd_slots;
                        self.odd_seen += 1;
                        selected
                    } else {
                        let selected = self.even_seen < self.plan.tie_slots - odd_slots;
                        self.even_seen += 1;
                        selected
                    }
                }
                Rounding::LastLine => false,
            }
        };
        if receives_extra {
            floor
                .checked_add(1)
                .ok_or_else(|| overflow("ranked allocation"))
        } else {
            Ok(floor)
        }
    }
}

fn proportional_share(
    total: i128,
    weight: i128,
    weight_total: i128,
) -> Result<(i128, i128), GenerateError> {
    let numerator = total
        .checked_mul(weight)
        .ok_or_else(|| overflow("proportional allocation"))?;
    Ok((
        numerator.div_euclid(weight_total),
        numerator.rem_euclid(weight_total),
    ))
}

/// Replays taxable line weights while building the tax allocation plan.
struct TaxWeights {
    raw: RawLines,
    discount: AllocationCursor,
    index: usize,
}

impl Iterator for TaxWeights {
    type Item = Result<i128, GenerateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.raw.next()?;
        Some(raw.and_then(|raw| {
            let discount = self.discount.part(self.index, raw.subtotal)?;
            self.index += 1;
            raw.subtotal
                .checked_sub(discount)
                .ok_or_else(|| overflow("taxable line amount"))
        }))
    }
}

impl OrderFamilyPlanner {
    /// Compute a bounded money summary for the order at `order_index`, seeded by
    /// that index so the result is independent of any spill threshold.
    fn compute(&self, order_index: u64) -> Result<OrderSummary, GenerateError> {
        let mut rng = self.seed.stream(StreamId::operator(
            self.parent_table.clone(),
            "commerce.order_family",
            format!("family.{order_index}"),
        ));

        let line_count = usize::try_from(self.lines.draw(&mut rng).max(0))
            .map_err(|_| overflow("order-family line count"))?;
        let line_rng = rng.clone();

        // First pass: retain only the aggregate while advancing the original RNG
        // to the exact point where the order-level rate draws have always lived.
        let mut subtotal: i128 = 0;
        for _ in 0..line_count {
            let raw = draw_raw_line(&mut rng, self.quantity, self.unit_price_minor)?;
            subtotal = subtotal
                .checked_add(raw.subtotal)
                .ok_or_else(|| overflow("order subtotal"))?;
        }

        let discount_rate = self.discount.sample(rng.random::<f64>());
        let tax_rate = self.tax.sample(rng.random::<f64>());

        let discount_total = rounded_rate(subtotal, discount_rate)?;
        let discount_plan = AllocationPlan::build(discount_total, self.rounding, || {
            RawLines::new(
                line_rng.clone(),
                line_count,
                self.quantity,
                self.unit_price_minor,
            )
            .map(|raw| raw.map(|line| line.subtotal))
        })?;

        let order_taxable = subtotal
            .checked_sub(discount_total)
            .ok_or_else(|| overflow("order taxable amount"))?;
        let tax_total = rounded_rate(order_taxable, tax_rate)?;
        let tax_plan = AllocationPlan::build(tax_total, self.rounding, || TaxWeights {
            raw: RawLines::new(
                line_rng.clone(),
                line_count,
                self.quantity,
                self.unit_price_minor,
            ),
            discount: discount_plan.cursor(),
            index: 0,
        })?;

        let shipping = self.shipping_minor;
        let grand_total = subtotal
            .checked_sub(discount_total)
            .and_then(|value| value.checked_add(tax_total))
            .and_then(|value| value.checked_add(shipping))
            .ok_or_else(|| overflow("order grand total"))?;

        Ok(OrderSummary {
            subtotal,
            discount: discount_total,
            tax: tax_total,
            shipping,
            grand_total,
            line_count,
            line_rng,
            discount_plan,
            tax_plan,
        })
    }

    /// Render a money minor-unit amount for a column of `family`.
    fn render_money(&self, minor: i128, family: &SqlTypeFamily) -> GeneratedValue {
        match family {
            SqlTypeFamily::Decimal => GeneratedValue::Decimal {
                minor,
                scale: self.scale,
            },
            SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => GeneratedValue::Integer(minor),
            _ => GeneratedValue::Text(format_minor(minor, self.scale)),
        }
    }

    /// The value for one parent role.
    fn parent_value(
        &self,
        role: ParentRole,
        totals: &OrderSummary,
        family: &SqlTypeFamily,
    ) -> GeneratedValue {
        let minor = match role {
            ParentRole::Subtotal => totals.subtotal,
            ParentRole::Discount => totals.discount,
            ParentRole::Tax => totals.tax,
            ParentRole::Shipping => totals.shipping,
            ParentRole::Total => totals.grand_total,
        };
        self.render_money(minor, family)
    }

    /// The value for one child role of a given line.
    fn child_value(
        &self,
        role: ChildRole,
        line: &LineTotals,
        family: &SqlTypeFamily,
    ) -> GeneratedValue {
        match role {
            ChildRole::Quantity => match family {
                SqlTypeFamily::Integer | SqlTypeFamily::BigInteger => {
                    GeneratedValue::Integer(line.quantity)
                }
                _ => GeneratedValue::Text(line.quantity.to_string()),
            },
            ChildRole::UnitPrice => self.render_money(line.unit_price, family),
            ChildRole::Discount => self.render_money(line.discount, family),
            ChildRole::Tax => self.render_money(line.tax, family),
            ChildRole::LineTotal => self.render_money(line.line_total, family),
        }
    }
}

/// Incremental child-row replay for one parent order.
struct OrderFamilyChildRows<'a> {
    planner: &'a OrderFamilyPlanner,
    raw: RawLines,
    discount: AllocationCursor,
    tax: AllocationCursor,
    index: usize,
}

impl<'a> OrderFamilyChildRows<'a> {
    fn new(planner: &'a OrderFamilyPlanner, summary: OrderSummary) -> Self {
        Self {
            raw: RawLines::new(
                summary.line_rng,
                summary.line_count,
                planner.quantity,
                planner.unit_price_minor,
            ),
            discount: summary.discount_plan.cursor(),
            tax: summary.tax_plan.cursor(),
            planner,
            index: 0,
        }
    }
}

impl Iterator for OrderFamilyChildRows<'_> {
    type Item = Result<Vec<GeneratedValue>, GenerateError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = match self.raw.next()? {
            Ok(raw) => raw,
            Err(error) => return Some(Err(error)),
        };
        let result = (|| {
            let discount = self.discount.part(self.index, raw.subtotal)?;
            let taxable = raw
                .subtotal
                .checked_sub(discount)
                .ok_or_else(|| overflow("taxable line amount"))?;
            let tax = self.tax.part(self.index, taxable)?;
            let line_total = taxable
                .checked_add(tax)
                .ok_or_else(|| overflow("child line total"))?;
            self.index += 1;

            let line = LineTotals {
                quantity: raw.quantity,
                unit_price: raw.unit_price,
                discount,
                tax,
                line_total,
            };
            let mut values = Vec::with_capacity(self.planner.child_writes.len());
            for (role, family) in self
                .planner
                .child_roles
                .iter()
                .zip(&self.planner.child_families)
            {
                values.push(self.planner.child_value(*role, &line, family));
            }
            Ok(values)
        })();
        Some(result)
    }
}

impl CompiledPlanner for OrderFamilyPlanner {
    fn writes(&self) -> &[String] {
        &self.parent_writes
    }

    fn generate_row(
        &mut self,
        row_index: u64,
        output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        let totals = self.compute(row_index)?;

        for (slot, role) in output.iter_mut().zip(&self.parent_roles) {
            let family = self.parent_family_for(*role);
            *slot = self.parent_value(*role, &totals, family);
        }

        self.pending_family = Some(totals);
        Ok(())
    }

    fn family_child_table(&self) -> Option<&str> {
        Some(&self.child_table)
    }

    fn family_relationship(&self) -> Option<&str> {
        Some(&self.relationship)
    }

    fn child_writes(&self) -> &[String] {
        &self.child_writes
    }

    fn take_family_children(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<Vec<GeneratedValue>, GenerateError>> + '_> {
        match self.pending_family.take() {
            Some(summary) => Box::new(OrderFamilyChildRows::new(self, summary)),
            None => Box::new(std::iter::empty()),
        }
    }

    fn family_sum_checks(&self) -> Vec<crate::generate::registry::FamilySumCheck> {
        use crate::generate::registry::FamilySumCheck;

        // Only the roles that are a pure sum of the matching child column are
        // stated as sum checks: a per-line `discount`/`tax` sums exactly to its
        // parent aggregate by construction (largest-remainder apportionment).
        // `subtotal`/`total`/`shipping` involve a product or the shipping offset,
        // so they are not a single-column sum and are intentionally omitted here
        // (the structural + apportionment invariants still hold internally).
        let pure_sum_roles = [ParentRole::Discount, ParentRole::Tax];
        let child_role_of = |parent: ParentRole| match parent {
            ParentRole::Discount => Some(ChildRole::Discount),
            ParentRole::Tax => Some(ChildRole::Tax),
            _ => None,
        };

        pure_sum_roles
            .into_iter()
            .filter_map(|parent_role| {
                let child_role = child_role_of(parent_role)?;
                let parent_column = self.column_for_parent_role(parent_role)?;
                let child_column = self.column_for_child_role(child_role)?;
                Some(FamilySumCheck {
                    relationship: self.relationship.clone(),
                    parent_column,
                    child_column,
                })
            })
            .collect()
    }
}

impl OrderFamilyPlanner {
    /// The schema column name the planner writes for a parent role, if that role
    /// is configured for this family.
    fn column_for_parent_role(&self, role: ParentRole) -> Option<String> {
        self.parent_roles
            .iter()
            .position(|candidate| *candidate == role)
            .map(|index| self.parent_writes[index].clone())
    }

    /// The schema column name the planner writes for a child role, if that role
    /// is configured for this family.
    fn column_for_child_role(&self, role: ChildRole) -> Option<String> {
        self.child_roles
            .iter()
            .position(|candidate| *candidate == role)
            .map(|index| self.child_writes[index].clone())
    }

    fn parent_family_for(&self, role: ParentRole) -> &SqlTypeFamily {
        let index = self
            .parent_roles
            .iter()
            .position(|candidate| *candidate == role)
            .expect("role is in parent_roles");
        &self.parent_families[index]
    }
}

/// `round_half_up(amount * rate_num / RATE_DEN)` in exact `i128` arithmetic.
fn rounded_rate(amount: i128, rate_num: i128) -> Result<i128, GenerateError> {
    if rate_num == 0 || amount == 0 {
        return Ok(0);
    }
    let numerator = amount
        .checked_mul(rate_num)
        .ok_or_else(|| overflow("rate * amount"))?;
    // Round half up on the (non-negative) amounts money families use here.
    Ok((numerator + RATE_DEN / 2).div_euclid(RATE_DEN))
}

/// Format minor units as a fixed-point decimal string for a non-decimal money
/// column (a text fallback).
fn format_minor(minor: i128, scale: u32) -> String {
    if scale == 0 {
        return minor.to_string();
    }
    let divisor = 10i128.pow(scale);
    let sign = if minor < 0 { "-" } else { "" };
    let magnitude = minor.abs();
    let whole = magnitude / divisor;
    let frac = magnitude % divisor;
    format!("{sign}{whole}.{frac:0width$}", width = scale as usize)
}

/// The `GEN-ORDER-FAMILY-OVERFLOW` runtime overflow error.
fn overflow(what: &str) -> GenerateError {
    GenerateError::Overflow(format!(
        "commerce.order_family: {what} overflows the representable minor-unit range"
    ))
}

// --- Compilation ------------------------------------------------------------

/// Validate `config` (plus the compiler-injected child facts) and build the
/// compiled planner, gathering every independent error first.
fn compile_order_family(
    config: &PlannerConfig,
    context: &CompileContext<'_>,
) -> Result<OrderFamilyPlanner, DiagnosticBag> {
    let mut bag = DiagnosticBag::default();
    let parent = context.table();
    let path = context.path();

    reject_flat_form(config, path, &mut bag);

    let facts = config.args.get(FAMILY_FACTS_KEY);
    let child_found = facts
        .and_then(|f| f.get("child_found"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let child_name = string_arg(config, "children")
        .unwrap_or_default()
        .to_string();

    if !child_found {
        bag.error(
            crate::diagnostic::codes::ORDER_FAMILY_CHILD_UNKNOWN.code,
            format!("{path}.children"),
            format!(
                "commerce.order_family `children` names table `{child_name}`, which is not a table in the model"
            ),
        );
    }

    let rel_name = string_arg(config, "relationship")
        .unwrap_or_default()
        .to_string();
    let rel_on_child = facts
        .and_then(|f| f.get("rel_on_child"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if child_found && !rel_on_child {
        bag.error(
            crate::diagnostic::codes::ORDER_FAMILY_RELATIONSHIP.code,
            format!("{path}.relationship"),
            format!(
                "commerce.order_family `relationship` `{rel_name}` is not a relationship declared on child table `{child_name}` that references parent `{}`",
                parent.name
            ),
        );
    }

    let scale = match config.args.get("currency_scale").and_then(as_u32) {
        // Bound to 0..=18: money math and formatting use 10i128.pow(scale),
        // which overflows i128 past 18.
        Some(scale) if scale <= 18 => scale,
        Some(scale) => {
            bag.error(
                crate::diagnostic::codes::ORDER_FAMILY_CONFIG.code,
                format!("{path}.currency_scale"),
                format!(
                    "commerce.order_family `currency_scale` must be between 0 and 18 (got {scale})"
                ),
            );
            0
        }
        None => {
            bag.error(
                crate::diagnostic::codes::ORDER_FAMILY_CONFIG.code,
                format!("{path}.currency_scale"),
                "commerce.order_family requires a `currency_scale`".to_string(),
            );
            0
        }
    };

    let rounding = match config.args.get("rounding").and_then(Value::as_str) {
        Some(name) => match Rounding::parse(name) {
            Some(rounding) => rounding,
            None => {
                bag.error(
                    crate::diagnostic::codes::ORDER_FAMILY_CONFIG.code,
                    format!("{path}.rounding"),
                    format!("commerce.order_family `rounding` `{name}` is not one of `largest_remainder`, `last_line`, `bankers`"),
                );
                Rounding::LargestRemainder
            }
        },
        None => {
            bag.error(
                crate::diagnostic::codes::ORDER_FAMILY_CONFIG.code,
                format!("{path}.rounding"),
                "commerce.order_family requires a `rounding` mode".to_string(),
            );
            Rounding::LargestRemainder
        }
    };

    // Resolve the parent aggregate columns (subtotal + total required).
    let (parent_writes, parent_roles, parent_families) =
        resolve_parent_roles(config, parent, scale, path, &mut bag);

    // Resolve the child line columns against the injected child column types.
    let (child_writes, child_roles, child_families) =
        resolve_child_roles(config, facts, &child_name, scale, path, &mut bag);

    let quantity = int_range(config.args.get("quantity"), 1, 1);
    let unit_price_minor = unit_price_range(config.args.get("unit_price"), scale);
    let lines = compile_line_range(facts, path, &mut bag);
    let tax = compile_rate(config.args.get("tax"), "tax", path, &mut bag);
    let discount = compile_rate(config.args.get("discount"), "discount", path, &mut bag);
    let shipping_minor = compile_shipping(config.args.get("shipping"), scale);

    // Decimal overflow: the largest possible grand total must fit every money
    // column's declared precision.
    check_overflow(
        &lines,
        &quantity,
        &unit_price_minor,
        &tax,
        shipping_minor,
        &parent_writes,
        parent,
        facts,
        &child_writes,
        &child_name,
        path,
        &mut bag,
    );

    if bag.has_errors() {
        return Err(bag);
    }

    Ok(OrderFamilyPlanner {
        parent_table: parent.name.clone(),
        child_table: child_name,
        relationship: rel_name,
        parent_writes,
        parent_roles,
        child_writes,
        child_roles,
        scale,
        parent_families,
        child_families,
        rounding,
        quantity,
        unit_price_minor,
        lines,
        tax,
        discount,
        shipping_minor,
        seed: context.seed(),
        pending_family: None,
    })
}

/// Reject the old flat planner form (columns declared directly rather than under
/// `columns` / `child_columns`) as an unknown-field error.
fn reject_flat_form(config: &PlannerConfig, path: &str, bag: &mut DiagnosticBag) {
    for key in [
        "subtotal",
        "line_total",
        "grand_total",
        "line_items",
        "items",
    ] {
        if config.args.contains_key(key) {
            bag.error(
                crate::diagnostic::codes::ORDER_FAMILY_UNKNOWN_FIELD.code,
                format!("{path}.{key}"),
                format!(
                    "commerce.order_family does not accept a top-level `{key}`; map parent columns under `columns` and child columns under `child_columns` (the old flat form is not supported)"
                ),
            );
        }
    }
}

/// Parent role keys in write order. `subtotal` and `total` are required.
const PARENT_ROLE_ORDER: [(&str, ParentRole, bool); 5] = [
    ("subtotal", ParentRole::Subtotal, true),
    ("discount", ParentRole::Discount, false),
    ("tax", ParentRole::Tax, false),
    ("shipping", ParentRole::Shipping, false),
    ("total", ParentRole::Total, true),
];

/// Child role keys in value order. `quantity`, `unit_price`, `line_total` are
/// required.
const CHILD_ROLE_ORDER: [(&str, ChildRole, bool); 5] = [
    ("quantity", ChildRole::Quantity, true),
    ("unit_price", ChildRole::UnitPrice, true),
    ("discount", ChildRole::Discount, false),
    ("tax", ChildRole::Tax, false),
    ("line_total", ChildRole::LineTotal, true),
];

/// Resolve the parent `columns:` mapping into ordered writes/roles/families,
/// reporting missing required roles, columns absent from the parent schema, and
/// money columns whose declared scale disagrees with `currency_scale`.
fn resolve_parent_roles(
    config: &PlannerConfig,
    parent: &PortableTable,
    scale: u32,
    path: &str,
    bag: &mut DiagnosticBag,
) -> (Vec<String>, Vec<ParentRole>, Vec<SqlTypeFamily>) {
    let columns = config.args.get("columns");
    let mut writes = Vec::new();
    let mut roles = Vec::new();
    let mut families = Vec::new();
    for (key, role, required) in PARENT_ROLE_ORDER {
        let Some(name) = role_name(columns, key) else {
            if required {
                bag.error(
                    crate::diagnostic::codes::ORDER_FAMILY_COLUMN_MISSING.code,
                    format!("{path}.columns.{key}"),
                    format!("commerce.order_family requires a `{key}` column under `columns`"),
                );
            }
            continue;
        };
        match parent.columns.iter().find(|column| column.name == name) {
            Some(column) => {
                check_money_scale(
                    &column.source_type,
                    &column.family,
                    scale,
                    &format!("{path}.columns.{key}"),
                    &column.name,
                    bag,
                );
                writes.push(column.name.clone());
                roles.push(role);
                families.push(column.family.clone());
            }
            None => {
                bag.error(
                    crate::diagnostic::codes::ORDER_FAMILY_COLUMN_MISSING.code,
                    format!("{path}.columns.{key}"),
                    format!(
                        "commerce.order_family `{key}` column `{name}` does not exist on parent table `{}`",
                        parent.name
                    ),
                );
            }
        }
    }
    (writes, roles, families)
}

/// Resolve the `child_columns:` mapping against the injected child column types.
fn resolve_child_roles(
    config: &PlannerConfig,
    facts: Option<&Value>,
    child_name: &str,
    scale: u32,
    path: &str,
    bag: &mut DiagnosticBag,
) -> (Vec<String>, Vec<ChildRole>, Vec<SqlTypeFamily>) {
    let columns = config.args.get("child_columns");
    let child_types = facts.and_then(|f| f.get("child_columns"));
    let mut writes = Vec::new();
    let mut roles = Vec::new();
    let mut families = Vec::new();
    for (key, role, required) in CHILD_ROLE_ORDER {
        let Some(name) = role_name(columns, key) else {
            if required {
                bag.error(
                    crate::diagnostic::codes::ORDER_FAMILY_COLUMN_MISSING.code,
                    format!("{path}.child_columns.{key}"),
                    format!(
                        "commerce.order_family requires a `{key}` column under `child_columns`"
                    ),
                );
            }
            continue;
        };
        // The compiler injects the child's column source types; a name absent
        // there does not exist on the child table.
        let source_type = child_types
            .and_then(|types| types.get(name))
            .and_then(Value::as_str);
        match source_type {
            Some(source_type) => {
                let family = family_of_source_type(source_type);
                if role != ChildRole::Quantity {
                    check_money_scale(
                        source_type,
                        &family,
                        scale,
                        &format!("{path}.child_columns.{key}"),
                        name,
                        bag,
                    );
                }
                writes.push(name.to_string());
                roles.push(role);
                families.push(family);
            }
            None => {
                bag.error(
                    crate::diagnostic::codes::ORDER_FAMILY_COLUMN_MISSING.code,
                    format!("{path}.child_columns.{key}"),
                    format!(
                        "commerce.order_family `{key}` column `{name}` does not exist on child table `{child_name}`"
                    ),
                );
            }
        }
    }
    (writes, roles, families)
}

/// Report a money column whose declared decimal scale disagrees with the
/// planner's `currency_scale` (an ambiguous scale that would silently mis-scale
/// the minor units).
fn check_money_scale(
    source_type: &str,
    family: &SqlTypeFamily,
    scale: u32,
    path: &str,
    column: &str,
    bag: &mut DiagnosticBag,
) {
    if *family != SqlTypeFamily::Decimal {
        return;
    }
    if let Some((_, declared_scale)) = decimal_precision_scale(source_type) {
        if declared_scale != scale {
            bag.error(
                crate::diagnostic::codes::ORDER_FAMILY_SCALE.code,
                path.to_string(),
                format!(
                    "commerce.order_family `currency_scale` is {scale}, but money column `{column}` is declared `{source_type}` with scale {declared_scale}; the currency scale is ambiguous"
                ),
            );
        }
    }
}

/// Compile the injected child line-count distribution into a shape-aware
/// [`LineDist`], honoring its kind and mean (not just its bounds), and rejecting
/// a zero maximum with a non-zero minimum.
fn compile_line_range(facts: Option<&Value>, path: &str, bag: &mut DiagnosticBag) -> LineDist {
    let min = facts
        .and_then(|f| f.get("dist_min"))
        .and_then(as_f64)
        .unwrap_or(1.0);
    let max = facts
        .and_then(|f| f.get("dist_max"))
        .and_then(as_f64)
        .unwrap_or(min);
    let mean = facts.and_then(|f| f.get("dist_mean")).and_then(as_f64);
    let kind = match facts
        .and_then(|f| f.get("dist_kind"))
        .and_then(Value::as_str)
    {
        Some("fixed") => LineKind::Fixed,
        Some("uniform") => LineKind::Uniform,
        // `observed` / `poisson` / `histogram` all respect the declared mean; an
        // absent distribution (a non-`relation.children` child) falls back to a
        // fixed single line via the `mean` default below.
        _ => LineKind::MeanRespecting,
    };
    let min_lines = min.max(0.0).ceil() as i128;
    let max_lines = max.max(0.0).floor() as i128;
    if max_lines < min_lines || (max_lines == 0 && min_lines > 0) {
        bag.error(
            crate::diagnostic::codes::ORDER_FAMILY_ZERO_LINES.code,
            path.to_string(),
            format!(
                "commerce.order_family child distribution allows at most {max_lines} line(s) but requires at least {min_lines}; no order could satisfy the minimum"
            ),
        );
    }
    let min_lines = min_lines.max(0);
    let max_lines = max_lines.max(min_lines);
    // A mean clamped into the achievable band; default to the low bound when the
    // child has no distribution to speak of.
    let mean = mean
        .unwrap_or(min_lines.max(1) as f64)
        .clamp(min_lines as f64, max_lines as f64);
    LineDist {
        min: min_lines,
        max: max_lines,
        mean,
        kind,
    }
}

/// Compile a `tax:`/`discount:` rate block. Absent → a fixed zero rate.
fn compile_rate(
    value: Option<&Value>,
    what: &str,
    path: &str,
    bag: &mut DiagnosticBag,
) -> RateChoice {
    let Some(value) = value else {
        return RateChoice::zero();
    };
    match value.get("kind").and_then(Value::as_str) {
        Some("weighted_choice") | None if value.get("rates").is_some() => {
            let rates = number_list(value.get("rates"));
            let weights = number_list(value.get("weights"));
            if rates.is_empty() || weights.len() != rates.len() {
                bag.error(
                    crate::diagnostic::codes::ORDER_FAMILY_CONFIG.code,
                    format!("{path}.{what}"),
                    format!("commerce.order_family `{what}` weighted_choice needs matching `rates` and `weights` lists"),
                );
                return RateChoice::zero();
            }
            let nums: Vec<i128> = rates.iter().map(|rate| rate_to_num(*rate)).collect();
            RateChoice::weighted(&nums, &weights)
        }
        Some("fixed") | Some("fixed_rate") => {
            let rate = value.get("rate").and_then(as_f64).unwrap_or(0.0);
            RateChoice::fixed(rate_to_num(rate))
        }
        _ => {
            // A bare `rate:` scalar is accepted as a fixed rate.
            if let Some(rate) = value.get("rate").and_then(as_f64) {
                RateChoice::fixed(rate_to_num(rate))
            } else {
                RateChoice::zero()
            }
        }
    }
}

/// Compile the fixed `shipping:` charge into minor units (0 when unconfigured).
fn compile_shipping(value: Option<&Value>, scale: u32) -> i128 {
    let Some(value) = value else {
        return 0;
    };
    if let Some(minor) = value.get("amount_minor").and_then(as_i128) {
        return minor.max(0);
    }
    if let Some(amount) = value.get("amount").and_then(as_f64) {
        return to_minor(amount, scale).max(0);
    }
    0
}

/// The worst-case grand total (max lines * max quantity * max unit price, plus
/// the largest tax) must fit every money column's declared precision.
#[allow(clippy::too_many_arguments)]
fn check_overflow(
    lines: &LineDist,
    quantity: &IntRange,
    unit_price: &IntRange,
    tax: &RateChoice,
    shipping_minor: i128,
    parent_writes: &[String],
    parent: &PortableTable,
    facts: Option<&Value>,
    child_writes: &[String],
    child_name: &str,
    path: &str,
    bag: &mut DiagnosticBag,
) {
    let max_rate = tax
        .cumulative
        .iter()
        .map(|(rate, _)| *rate)
        .max()
        .unwrap_or(0);
    let per_line = match quantity.max.checked_mul(unit_price.max) {
        Some(value) => value,
        None => {
            bag.error(
                crate::diagnostic::codes::ORDER_FAMILY_OVERFLOW.code,
                path.to_string(),
                "commerce.order_family maximum line subtotal (quantity * unit_price) overflows i128".to_string(),
            );
            return;
        }
    };
    let subtotal_max = per_line.saturating_mul(lines.max.max(0));
    let tax_max = subtotal_max.saturating_mul(max_rate) / RATE_DEN;
    // The grand total is subtotal - discount + tax + shipping; discount only
    // lowers it, so the maximum adds subtotal, tax, and the full shipping fee.
    let grand_max = subtotal_max
        .saturating_add(tax_max)
        .saturating_add(shipping_minor.max(0));
    // A single child line can carry at most its OWN tax, never the whole order's,
    // so bound child columns by the per-line value (+ per-line tax) — bounding by
    // the order-wide `tax_max` would spuriously reject valid configs.
    let per_line_tax_max = per_line.saturating_mul(max_rate) / RATE_DEN;
    let child_max = per_line.saturating_add(per_line_tax_max);

    // Parent money columns.
    for name in parent_writes {
        if let Some(column) = parent.columns.iter().find(|column| &column.name == name) {
            report_capacity(
                &column.source_type,
                grand_max,
                name,
                &parent.name,
                path,
                bag,
            );
        }
    }
    // Child money columns (line_total is the largest child value).
    let child_types = facts.and_then(|f| f.get("child_columns"));
    for name in child_writes {
        if let Some(source_type) = child_types
            .and_then(|types| types.get(name))
            .and_then(Value::as_str)
        {
            report_capacity(source_type, child_max, name, child_name, path, bag);
        }
    }
}

/// Emit `GEN-ORDER-FAMILY-OVERFLOW` when `max_minor` exceeds the declared
/// decimal precision's minor-unit capacity.
fn report_capacity(
    source_type: &str,
    max_minor: i128,
    column: &str,
    table: &str,
    path: &str,
    bag: &mut DiagnosticBag,
) {
    if let Some((precision, _)) = decimal_precision_scale(source_type) {
        let capacity = 10i128
            .checked_pow(precision)
            .map(|v| v - 1)
            .unwrap_or(i128::MAX);
        if max_minor > capacity {
            bag.error(
                crate::diagnostic::codes::ORDER_FAMILY_OVERFLOW.code,
                path.to_string(),
                format!(
                    "commerce.order_family can reach {max_minor} minor units, exceeding the capacity {capacity} of money column `{column}` on table `{table}` (declared `{source_type}`)"
                ),
            );
        }
    }
}

// --- Value / config parsing helpers -----------------------------------------

/// Convert an `f64` rate into an exact numerator over [`RATE_DEN`].
fn rate_to_num(rate: f64) -> i128 {
    (rate * RATE_DEN as f64).round() as i128
}

/// Convert a decimal major-unit amount into minor units at `scale`.
fn to_minor(amount: f64, scale: u32) -> i128 {
    (amount * 10f64.powi(scale as i32)).round() as i128
}

/// The `(precision, scale)` declared by a `decimal(p,s)` / `numeric(p,s)` type.
fn decimal_precision_scale(source_type: &str) -> Option<(u32, u32)> {
    let open = source_type.find('(')?;
    let close = source_type.find(')')?;
    let inner = source_type.get(open + 1..close)?;
    let mut parts = inner.split(',');
    let precision = parts.next()?.trim().parse::<u32>().ok()?;
    let scale = parts
        .next()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(0);
    Some((precision, scale))
}

/// Classify a raw SQL type name via the same coarse mapping the schema uses.
fn family_of_source_type(source_type: &str) -> SqlTypeFamily {
    let lower = source_type.to_lowercase();
    if lower.starts_with("decimal") || lower.starts_with("numeric") {
        SqlTypeFamily::Decimal
    } else if lower.starts_with("bigint") {
        SqlTypeFamily::BigInteger
    } else if lower.starts_with("int")
        || lower.starts_with("smallint")
        || lower.starts_with("tinyint")
    {
        SqlTypeFamily::Integer
    } else {
        SqlTypeFamily::Other
    }
}

/// Read an integer `{ min, max }` range, defaulting each bound.
fn int_range(value: Option<&Value>, default_min: i128, default_max: i128) -> IntRange {
    let min = value
        .and_then(|v| v.get("min"))
        .and_then(as_i128)
        .unwrap_or(default_min);
    let max = value
        .and_then(|v| v.get("max"))
        .and_then(as_i128)
        .unwrap_or(default_max.max(min));
    IntRange {
        min: min.max(0),
        max: max.max(min).max(0),
    }
}

/// Read a `unit_price` range in minor units (`min_minor`/`max_minor`) or decimal
/// major units (`min`/`max`) converted at `scale`.
fn unit_price_range(value: Option<&Value>, scale: u32) -> IntRange {
    let Some(value) = value else {
        return IntRange {
            min: 100,
            max: 10_000,
        };
    };
    if let (Some(min), Some(max)) = (
        value.get("min_minor").and_then(as_i128),
        value.get("max_minor").and_then(as_i128),
    ) {
        return IntRange {
            min: min.max(0),
            max: max.max(min).max(0),
        };
    }
    let min = value
        .get("min")
        .and_then(as_f64)
        .map(|v| to_minor(v, scale))
        .unwrap_or(100);
    let max = value
        .get("max")
        .and_then(as_f64)
        .map(|v| to_minor(v, scale))
        .unwrap_or(min.max(10_000));
    IntRange {
        min: min.max(0),
        max: max.max(min).max(0),
    }
}

fn role_name<'a>(columns: Option<&'a Value>, role: &str) -> Option<&'a str> {
    columns?.get(role).and_then(Value::as_str)
}

fn string_arg<'a>(config: &'a PlannerConfig, key: &str) -> Option<&'a str> {
    config.args.get(key).and_then(Value::as_str)
}

fn number_list(value: Option<&Value>) -> Vec<f64> {
    match value {
        Some(Value::Sequence(items)) => items.iter().filter_map(as_f64).collect(),
        _ => Vec::new(),
    }
}

fn as_u32(value: &Value) -> Option<u32> {
    as_i128(value).and_then(|v| u32::try_from(v).ok())
}

fn as_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .map(i128::from)
            .or_else(|| number.as_u64().map(i128::from))
            .or_else(|| number.as_f64().map(|float| float as i128)),
        Value::String(text) => text.trim().parse::<i128>().ok(),
        _ => None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apportion_sums_exactly_for_every_mode() {
        let weights = [300i128, 500, 200];
        for mode in [
            Rounding::LargestRemainder,
            Rounding::LastLine,
            Rounding::Bankers,
        ] {
            for total in [0i128, 1, 7, 100, 999, 1_000_000] {
                let parts = apportion(total, &weights, mode);
                assert_eq!(
                    parts.iter().sum::<i128>(),
                    total,
                    "mode {mode:?} total {total}"
                );
                assert!(parts.iter().all(|&p| p >= 0));
            }
        }
    }

    #[test]
    fn apportion_last_line_dumps_residual_on_the_last_line() {
        // 10 split by equal weights: 3,3,3 floors leave 1 -> last line gets it.
        let parts = apportion(10, &[1, 1, 1], Rounding::LastLine);
        assert_eq!(parts, vec![3, 3, 4]);
    }

    #[test]
    fn bounded_allocation_matches_the_reference_algorithms() {
        let cases: &[&[i128]] = &[
            &[],
            &[0],
            &[0, 0, 0],
            &[1, 1, 1],
            &[300, 500, 200],
            &[7, 2, 13, 13, 1],
        ];
        for mode in [
            Rounding::LargestRemainder,
            Rounding::LastLine,
            Rounding::Bankers,
        ] {
            for &weights in cases {
                for total in [0, 1, 7, 10, 100, 999] {
                    let plan =
                        AllocationPlan::build(total, mode, || weights.iter().copied().map(Ok))
                            .unwrap();
                    let mut cursor = plan.cursor();
                    let actual: Vec<i128> = weights
                        .iter()
                        .copied()
                        .enumerate()
                        .map(|(index, weight)| cursor.part(index, weight).unwrap())
                        .collect();
                    assert_eq!(actual, apportion(total, weights, mode));
                }
            }
        }
    }

    #[test]
    fn rate_conversion_is_exact_for_common_rates() {
        assert_eq!(rate_to_num(0.0), 0);
        assert_eq!(rate_to_num(0.08), 80_000);
        assert_eq!(rate_to_num(0.25), 250_000);
        // 8% of $100.00 (10000 minor) == $8.00 exactly.
        assert_eq!(rounded_rate(10_000, 80_000).unwrap(), 800);
        // 25% of 10000 == 2500.
        assert_eq!(rounded_rate(10_000, 250_000).unwrap(), 2_500);
    }

    #[test]
    fn format_minor_renders_fixed_point() {
        assert_eq!(format_minor(1050, 2), "10.50");
        assert_eq!(format_minor(5, 2), "0.05");
        assert_eq!(format_minor(-1234, 2), "-12.34");
        assert_eq!(format_minor(42, 0), "42");
    }

    #[test]
    fn decimal_precision_scale_parses_declarations() {
        assert_eq!(decimal_precision_scale("decimal(12,2)"), Some((12, 2)));
        assert_eq!(decimal_precision_scale("numeric(10, 3)"), Some((10, 3)));
        assert_eq!(decimal_precision_scale("bigint"), None);
    }
}
