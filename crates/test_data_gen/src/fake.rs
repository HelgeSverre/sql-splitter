//! Fake data generation helpers.
//!
//! Provides deterministic fake data for names, emails, dates, etc.

use rand::Rng;

/// First names for fake data
const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry", "Iris", "Jack", "Kate",
    "Leo", "Maya", "Noah", "Olivia", "Peter", "Quinn", "Rose", "Sam", "Tara", "Uma", "Victor",
    "Wendy", "Xavier", "Yara", "Zack", "Anna", "Brian", "Clara", "Derek",
];

/// Last names for fake data
const LAST_NAMES: &[&str] = &[
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez",
    "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White",
    "Harris", "Clark", "Lewis", "Robinson", "Walker", "Hall", "Young", "King", "Wright", "Hill",
];

/// Company name parts
const COMPANY_PREFIXES: &[&str] = &[
    "Acme", "Global", "Tech", "Prime", "Nova", "Alpha", "Beta", "Delta", "Omega", "Apex", "Peak",
    "Summit", "Core", "Edge", "Wave", "Flow", "Spark", "Swift", "Bright", "Clear",
];

const COMPANY_SUFFIXES: &[&str] = &[
    "Corp",
    "Inc",
    "LLC",
    "Systems",
    "Solutions",
    "Tech",
    "Labs",
    "Group",
    "Industries",
    "Dynamics",
    "Works",
    "Hub",
    "Net",
    "Cloud",
    "Digital",
    "Services",
    "Partners",
    "Co",
];

/// Product adjectives
const PRODUCT_ADJECTIVES: &[&str] = &[
    "Premium", "Pro", "Ultra", "Super", "Mega", "Mini", "Lite", "Plus", "Max", "Elite", "Advanced",
    "Basic", "Standard", "Classic", "Modern", "Smart", "Quick", "Easy", "Fast",
];

/// Product nouns
const PRODUCT_NOUNS: &[&str] = &[
    "Widget",
    "Gadget",
    "Device",
    "Tool",
    "Kit",
    "Pack",
    "Set",
    "Bundle",
    "System",
    "Module",
    "Component",
    "Unit",
    "Item",
    "Product",
    "Solution",
    "Platform",
    "Service",
    "Package",
];

/// Category names
const CATEGORIES: &[&str] = &[
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Toys",
    "Food & Beverage",
    "Health",
    "Beauty",
    "Automotive",
    "Office",
    "Pet Supplies",
    "Music",
    "Movies",
    "Software",
];

/// Lorem ipsum words for text generation
const LOREM_WORDS: &[&str] = &[
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
    "et",
    "dolore",
    "magna",
    "aliqua",
    "enim",
    "ad",
    "minim",
    "veniam",
    "quis",
    "nostrud",
    "exercitation",
    "ullamco",
    "laboris",
    "nisi",
    "aliquip",
    "ex",
    "ea",
    "commodo",
    "consequat",
    "duis",
    "aute",
    "irure",
    "in",
    "reprehenderit",
    "voluptate",
    "velit",
    "esse",
    "cillum",
    "fugiat",
    "nulla",
    "pariatur",
    "excepteur",
    "sint",
    "occaecat",
    "cupidatat",
    "non",
    "proident",
    "sunt",
    "culpa",
    "qui",
    "officia",
    "deserunt",
    "mollit",
    "anim",
    "id",
    "est",
    "laborum",
];

/// Role names for RBAC
const ROLES: &[&str] = &[
    "admin",
    "manager",
    "editor",
    "viewer",
    "member",
    "guest",
    "moderator",
    "analyst",
    "developer",
    "support",
];

/// Permission names
const PERMISSIONS: &[&str] = &[
    "users.create",
    "users.read",
    "users.update",
    "users.delete",
    "posts.create",
    "posts.read",
    "posts.update",
    "posts.delete",
    "settings.read",
    "settings.update",
    "reports.view",
    "reports.export",
    "admin.access",
    "billing.manage",
    "api.access",
];

/// Order statuses
const ORDER_STATUSES: &[&str] = &[
    "pending",
    "confirmed",
    "processing",
    "shipped",
    "delivered",
    "cancelled",
    "refunded",
];

/// Project statuses
const PROJECT_STATUSES: &[&str] = &["active", "on_hold", "completed", "archived", "cancelled"];

/// Task priorities
const TASK_PRIORITIES: &[i32] = &[1, 2, 3, 4, 5];

/// Fake data generator with deterministic RNG
pub struct FakeData<R: Rng> {
    rng: R,
}

impl<R: Rng> FakeData<R> {
    pub fn new(rng: R) -> Self {
        Self { rng }
    }

    /// Generate a random first name
    pub fn first_name(&mut self) -> &'static str {
        FIRST_NAMES[self.rng.gen_range(0..FIRST_NAMES.len())]
    }

    /// Generate a random last name
    pub fn last_name(&mut self) -> &'static str {
        LAST_NAMES[self.rng.gen_range(0..LAST_NAMES.len())]
    }

    /// Generate a full name
    pub fn full_name(&mut self) -> String {
        format!("{} {}", self.first_name(), self.last_name())
    }

    /// Generate an email address
    pub fn email(&mut self, first: &str, last: &str, domain: &str) -> String {
        let num: u32 = self.rng.gen_range(1..1000);
        format!(
            "{}.{}{}@{}",
            first.to_lowercase(),
            last.to_lowercase(),
            num,
            domain
        )
    }

    /// Generate a company name
    pub fn company_name(&mut self) -> String {
        let prefix = COMPANY_PREFIXES[self.rng.gen_range(0..COMPANY_PREFIXES.len())];
        let suffix = COMPANY_SUFFIXES[self.rng.gen_range(0..COMPANY_SUFFIXES.len())];
        format!("{} {}", prefix, suffix)
    }

    /// Generate a slug from a name
    pub fn slug(&mut self, name: &str) -> String {
        let base = name
            .to_lowercase()
            .replace(' ', "-")
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-')
            .collect::<String>();
        let num: u32 = self.rng.gen_range(1..10000);
        format!("{}-{}", base, num)
    }

    /// Generate a product name
    pub fn product_name(&mut self) -> String {
        let adj = PRODUCT_ADJECTIVES[self.rng.gen_range(0..PRODUCT_ADJECTIVES.len())];
        let noun = PRODUCT_NOUNS[self.rng.gen_range(0..PRODUCT_NOUNS.len())];
        format!("{} {}", adj, noun)
    }

    /// Generate a SKU
    pub fn sku(&mut self) -> String {
        let prefix: String = (0..3)
            .map(|_| self.rng.gen_range(b'A'..=b'Z') as char)
            .collect();
        let num: u32 = self.rng.gen_range(10000..99999);
        format!("{}-{}", prefix, num)
    }

    /// Generate a category name
    pub fn category(&mut self) -> &'static str {
        CATEGORIES[self.rng.gen_range(0..CATEGORIES.len())]
    }

    /// Generate a price
    pub fn price(&mut self, min: f64, max: f64) -> f64 {
        let value = self.rng.gen_range(min..max);
        (value * 100.0).round() / 100.0
    }

    /// Generate a random integer in range
    pub fn int_range(&mut self, min: i64, max: i64) -> i64 {
        self.rng.gen_range(min..=max)
    }

    /// Generate a boolean with given probability of true
    pub fn bool_with_probability(&mut self, probability: f64) -> bool {
        self.rng.gen::<f64>() < probability
    }

    /// Generate lorem ipsum text
    pub fn lorem(&mut self, word_count: usize) -> String {
        (0..word_count)
            .map(|_| LOREM_WORDS[self.rng.gen_range(0..LOREM_WORDS.len())])
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Generate a sentence (capitalized, with period)
    pub fn sentence(&mut self, word_count: usize) -> String {
        let mut text = self.lorem(word_count);
        if let Some(first) = text.get_mut(0..1) {
            first.make_ascii_uppercase();
        }
        text.push('.');
        text
    }

    /// Generate a paragraph
    pub fn paragraph(&mut self, sentence_count: usize) -> String {
        let mut sentences = Vec::with_capacity(sentence_count);
        for _ in 0..sentence_count {
            let word_count = self.rng.gen_range(5..15);
            sentences.push(self.sentence(word_count));
        }
        sentences.join(" ")
    }

    /// Generate a role name
    pub fn role(&mut self) -> &'static str {
        ROLES[self.rng.gen_range(0..ROLES.len())]
    }

    /// Get all permission names
    pub fn all_permissions() -> &'static [&'static str] {
        PERMISSIONS
    }

    /// Get all role names
    pub fn all_roles() -> &'static [&'static str] {
        ROLES
    }

    /// Generate an order status
    pub fn order_status(&mut self) -> &'static str {
        ORDER_STATUSES[self.rng.gen_range(0..ORDER_STATUSES.len())]
    }

    /// Generate a project status
    pub fn project_status(&mut self) -> &'static str {
        PROJECT_STATUSES[self.rng.gen_range(0..PROJECT_STATUSES.len())]
    }

    /// Generate a task priority
    pub fn task_priority(&mut self) -> i32 {
        TASK_PRIORITIES[self.rng.gen_range(0..TASK_PRIORITIES.len())]
    }

    /// Generate an order number
    pub fn order_number(&mut self) -> String {
        let year = 2024;
        let num: u32 = self.rng.gen_range(100000..999999);
        format!("ORD-{}-{}", year, num)
    }

    /// Generate a phone number
    pub fn phone(&mut self) -> String {
        let area: u32 = self.rng.gen_range(200..999);
        let prefix: u32 = self.rng.gen_range(200..999);
        let line: u32 = self.rng.gen_range(1000..9999);
        format!("+1-{}-{}-{}", area, prefix, line)
    }

    /// Generate a datetime string (ISO 8601)
    pub fn datetime(&mut self, year_start: i32, year_end: i32) -> String {
        let year = self.rng.gen_range(year_start..=year_end);
        let month = self.rng.gen_range(1..=12);
        let day = self.rng.gen_range(1..=28); // Safe for all months
        let hour = self.rng.gen_range(0..24);
        let minute = self.rng.gen_range(0..60);
        let second = self.rng.gen_range(0..60);
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        )
    }

    /// Generate a date string (ISO 8601)
    pub fn date(&mut self, year_start: i32, year_end: i32) -> String {
        let year = self.rng.gen_range(year_start..=year_end);
        let month = self.rng.gen_range(1..=12);
        let day = self.rng.gen_range(1..=28);
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    /// Pick a random element from a slice
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.rng.gen_range(0..items.len())]
    }

    /// Pick a random element from a non-empty Vec, returning the value
    pub fn pick_id(&mut self, ids: &[i64]) -> i64 {
        ids[self.rng.gen_range(0..ids.len())]
    }

    /// Generate a folder/file path segment
    pub fn path_segment(&mut self) -> String {
        let words = [
            "docs", "files", "images", "reports", "archive", "data", "tmp", "exports",
        ];
        let word = words[self.rng.gen_range(0..words.len())];
        let num: u32 = self.rng.gen_range(1..100);
        format!("{}_{}", word, num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn test_deterministic_generation() {
        let mut fake1 = FakeData::new(ChaCha8Rng::seed_from_u64(42));
        let mut fake2 = FakeData::new(ChaCha8Rng::seed_from_u64(42));

        // Same seed should produce same results
        assert_eq!(fake1.first_name(), fake2.first_name());
        assert_eq!(fake1.company_name(), fake2.company_name());
        assert_eq!(fake1.price(10.0, 100.0), fake2.price(10.0, 100.0));
    }

    #[test]
    fn test_email_generation() {
        let mut fake = FakeData::new(ChaCha8Rng::seed_from_u64(42));
        let email = fake.email("John", "Doe", "example.com");
        assert!(email.contains("@example.com"));
        assert!(email.starts_with("john.doe"));
    }

    #[test]
    fn test_price_precision() {
        let mut fake = FakeData::new(ChaCha8Rng::seed_from_u64(42));
        let price = fake.price(10.0, 100.0);
        // Should have at most 2 decimal places
        assert_eq!(price, (price * 100.0).round() / 100.0);
    }
}
