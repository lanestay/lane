//! PII detection and scrubbing — SSN, credit card, email, phone, custom rules.
//!
//! Detected values are replaced with `<entity_type>` placeholders.
//! Column hints force scrubbing on named columns even when regex misses.

use std::cmp::Ordering;
use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use serde_json::Value;

/// Whether PII scrubbing is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PiiMode {
    /// Pass values through unchanged.
    None,
    /// Replace detected PII with `<type>` placeholders.
    Scrub,
}

// ── Entity types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityKind {
    Ssn,
    CreditCard,
    Email,
    Phone,
    Custom { replacement: String },
}

impl EntityKind {
    fn tag(&self) -> &str {
        match self {
            Self::Ssn => "ssn",
            Self::CreditCard => "credit_card",
            Self::Email => "email_address",
            Self::Phone => "phone_number",
            Self::Custom { replacement } => replacement.as_str(),
        }
    }
}

// ── Custom rules ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CustomRule {
    #[allow(dead_code)]
    pub name: String,
    pub regex: Regex,
    pub replacement_text: String,
}

// ── Stats ───────────────────────────────────────────────────────────────────

#[derive(Default, Debug, Clone, Serialize)]
pub struct PiiStats {
    pub ssn: usize,
    pub cc: usize,
    pub email: usize,
    pub phone: usize,
    pub custom: usize,
}

impl PiiStats {
    pub fn increment(&mut self, kind: &EntityKind) {
        match kind {
            EntityKind::Ssn => self.ssn += 1,
            EntityKind::CreditCard => self.cc += 1,
            EntityKind::Email => self.email += 1,
            EntityKind::Phone => self.phone += 1,
            EntityKind::Custom { .. } => self.custom += 1,
        }
    }
}

// ── Processor ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PiiProcessor {
    mode: PiiMode,
    column_hints: Vec<String>,
    column_excludes: Vec<String>,
    custom_rules: Vec<CustomRule>,
}

impl PiiProcessor {
    pub fn new(
        mode: PiiMode,
        column_hints: Vec<String>,
        column_excludes: Vec<String>,
    ) -> Self {
        let lc = |v: Vec<String>| v.into_iter().map(|s| s.to_ascii_lowercase()).collect();
        Self {
            mode,
            column_hints: lc(column_hints),
            column_excludes: lc(column_excludes),
            custom_rules: Vec::new(),
        }
    }

    pub fn with_custom_rules(mut self, rules: Vec<CustomRule>) -> Self {
        self.custom_rules = rules;
        self
    }

    #[allow(dead_code)]
    pub fn mode(&self) -> PiiMode {
        self.mode
    }

    pub fn process_text(&self, value: &str) -> String {
        self.process_text_with_stats(value, &mut PiiStats::default())
    }

    pub fn process_text_with_stats(&self, value: &str, stats: &mut PiiStats) -> String {
        if self.mode == PiiMode::None {
            return value.to_string();
        }

        let mut hits = scan(value);

        for rule in &self.custom_rules {
            for m in rule.regex.find_iter(value) {
                hits.push(Span {
                    kind: EntityKind::Custom { replacement: rule.replacement_text.clone() },
                    start: m.start(),
                    end: m.end(),
                });
            }
        }

        if hits.is_empty() {
            return value.to_string();
        }

        // Sort by position, longest first on ties, then merge overlaps.
        hits.sort_by(|a, b| match a.start.cmp(&b.start) {
            Ordering::Equal => b.len().cmp(&a.len()),
            other => other,
        });

        let merged = merge_overlapping(hits);

        // Replace in reverse so earlier indices stay valid.
        let mut out = value.to_string();
        for span in merged.iter().rev() {
            stats.increment(&span.kind);
            let tag = format!("<{}>", span.kind.tag());
            out.replace_range(span.start..span.end, &tag);
        }
        out
    }

    fn should_force_column(&self, column: &str) -> bool {
        let col = column.to_ascii_lowercase();
        if self.column_excludes.iter().any(|ex| col.contains(ex)) {
            return false;
        }
        self.column_hints.iter().any(|h| col.contains(h))
    }

    fn force_redact(&self, column: &str, _value: &str) -> String {
        match self.mode {
            PiiMode::None => _value.to_string(),
            PiiMode::Scrub => {
                let col = column.to_ascii_lowercase().replace(' ', "_");
                format!("<{}>", col)
            }
        }
    }
}

// ── Detection spans ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Span {
    kind: EntityKind,
    start: usize,
    end: usize,
}

impl Span {
    fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }
}

fn merge_overlapping(spans: Vec<Span>) -> Vec<Span> {
    let mut out: Vec<Span> = Vec::with_capacity(spans.len());
    for s in spans {
        if let Some(prev) = out.last_mut() {
            if s.start <= prev.end {
                prev.end = prev.end.max(s.end);
                continue;
            }
        }
        out.push(s);
    }
    out
}

// ── Detectors ───────────────────────────────────────────────────────────────

fn scan(value: &str) -> Vec<Span> {
    let mut out = Vec::new();
    out.extend(find_ssns(value));
    out.extend(find_credit_cards(value));
    out.extend(find_emails(value));
    out.extend(find_phones(value));
    out
}

fn find_ssns(value: &str) -> Vec<Span> {
    // 9 digits, optionally hyphenated 3-2-4.
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?:^|[^0-9])(\d{3}-\d{2}-\d{4}|\d{9})(?:$|[^0-9])").unwrap()
    });
    RE.captures_iter(value)
        .filter_map(|c| {
            let m = c.get(1)?;
            let digits: String = m.as_str().chars().filter(|ch| ch.is_ascii_digit()).collect();
            if ssn_plausible(&digits) {
                Some(Span { kind: EntityKind::Ssn, start: m.start(), end: m.end() })
            } else {
                None
            }
        })
        .collect()
}

/// Reject obviously invalid SSN area/group/serial combos per SSA rules.
fn ssn_plausible(digits: &str) -> bool {
    if digits.len() != 9 { return false; }
    let area = &digits[..3];
    let group = &digits[3..5];
    let serial = &digits[5..];
    // Area cannot be 000, 666, or 9xx
    if area == "000" || area == "666" || digits.starts_with('9') { return false; }
    if group == "00" || serial == "0000" { return false; }
    true
}

fn find_credit_cards(value: &str) -> Vec<Span> {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?:^|[^0-9])((?:\d[ -]?){12,18}\d)(?:$|[^0-9])").unwrap()
    });
    RE.captures_iter(value)
        .filter_map(|c| {
            let m = c.get(1)?;
            let digits: String = m.as_str().chars().filter(|ch| ch.is_ascii_digit()).collect();
            if digits.len() < 13 || digits.len() > 19 { return None; }
            if card_network_prefix(&digits) && luhn_check(&digits) {
                Some(Span { kind: EntityKind::CreditCard, start: m.start(), end: m.end() })
            } else {
                None
            }
        })
        .collect()
}

/// Check if the leading digits match a known card network (Visa, MC, Amex, Discover).
fn card_network_prefix(digits: &str) -> bool {
    if digits.starts_with('4') { return true; } // Visa
    if let Some(p2) = digits.get(..2) {
        if p2 == "34" || p2 == "37" { return true; } // Amex
        if let Ok(n) = p2.parse::<u8>() {
            if (51..=55).contains(&n) { return true; } // Mastercard
        }
    }
    if digits.starts_with("6011") || digits.starts_with("65") { return true; } // Discover
    if let Some(p3) = digits.get(..3) {
        if let Ok(n) = p3.parse::<u16>() {
            if (644..=649).contains(&n) { return true; } // Discover
        }
    }
    if let Some(p4) = digits.get(..4) {
        if let Ok(n) = p4.parse::<u16>() {
            if (2221..=2720).contains(&n) { return true; } // Mastercard 2-series
        }
    }
    false
}

/// Luhn checksum validation.
fn luhn_check(digits: &str) -> bool {
    let mut sum = 0u32;
    let mut dbl = false;
    for ch in digits.chars().rev() {
        let Some(mut d) = ch.to_digit(10) else { return false };
        if dbl {
            d *= 2;
            if d > 9 { d -= 9; }
        }
        sum += d;
        dbl = !dbl;
    }
    sum % 10 == 0
}

fn find_emails(value: &str) -> Vec<Span> {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b").unwrap()
    });
    RE.find_iter(value)
        .map(|m| Span { kind: EntityKind::Email, start: m.start(), end: m.end() })
        .collect()
}

fn find_phones(value: &str) -> Vec<Span> {
    // US numbers: optional +1 prefix, area code with optional parens, 7 trailing digits.
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"(?x)(?:^|[^0-9])
              ((?:\+?1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4})
              (?:$|[^0-9])",
        )
        .unwrap()
    });
    RE.captures_iter(value)
        .filter_map(|c| {
            let m = c.get(1)?;
            Some(Span { kind: EntityKind::Phone, start: m.start(), end: m.end() })
        })
        .collect()
}

// ── JSON helpers ────────────────────────────────────────────────────────────

pub fn process_json_value(processor: &PiiProcessor, v: &mut Value) {
    if processor.mode == PiiMode::None { return; }
    match v {
        Value::String(s) => *s = processor.process_text(s),
        Value::Array(arr) => arr.iter_mut().for_each(|item| process_json_value(processor, item)),
        Value::Object(map) => map.values_mut().for_each(|vv| process_json_value(processor, vv)),
        _ => {}
    }
}

pub fn process_json_row(processor: &PiiProcessor, row: &mut HashMap<String, Value>) {
    if processor.mode == PiiMode::None { return; }
    for (col, value) in row.iter_mut() {
        if processor.should_force_column(col) {
            if let Value::String(s) = value {
                *s = processor.force_redact(col, s);
                continue;
            }
        }
        process_json_value(processor, value);
    }
}

#[allow(dead_code)]
pub fn process_string_fields(
    processor: &PiiProcessor,
    values: &mut [String],
    column_names: &[String],
) {
    if processor.mode == PiiMode::None { return; }
    for (idx, value) in values.iter_mut().enumerate() {
        if let Some(col) = column_names.get(idx) {
            if processor.should_force_column(col) {
                *value = processor.force_redact(col, value);
                continue;
            }
        }
        *value = processor.process_text(value);
    }
}
