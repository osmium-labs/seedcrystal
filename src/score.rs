//! Reliability scoring over exponentially decaying windows, after Pieter Wuille's bitcoin-seeder.

pub const WINDOWS: [(&str, f64); 5] = [
    ("2h", 7200.0),
    ("8h", 28800.0),
    ("1d", 86400.0),
    ("1w", 604800.0),
    ("1m", 2592000.0),
];

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Stat {
    pub weight: f64,
    pub reliability: f64,
    pub count: f64,
}

impl Stat {
    /// Reliability as a fraction of the weight observed so far.
    pub fn effective(&self) -> f64 {
        if self.weight > 0.0 {
            self.reliability / self.weight
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Stats {
    pub windows: [Stat; 5],
    pub last_update: i64,
}

/// (window index, minimum effective reliability, minimum count). A node meets the full bar when
/// any one window clears both of its thresholds.
const FULL_BAR: [(usize, f64, f64); 5] = [
    (0, 0.85, 2.0),
    (1, 0.70, 4.0),
    (2, 0.55, 8.0),
    (3, 0.45, 16.0),
    (4, 0.35, 32.0),
];

impl Stats {
    pub fn update(&mut self, ok: bool, now: i64) {
        let dt = (now - self.last_update).max(0) as f64;
        for (stat, (_, tau)) in self.windows.iter_mut().zip(WINDOWS) {
            if stat.count == 0.0 {
                *stat = Stat {
                    weight: 1.0,
                    reliability: f64::from(u8::from(ok)),
                    count: 1.0,
                };
                continue;
            }
            let f = (-dt / tau).exp();
            stat.weight = stat.weight * f + (1.0 - f);
            stat.reliability = stat.reliability * f + if ok { 1.0 - f } else { 0.0 };
            stat.count = stat.count * f + 1.0;
        }
        self.last_update = now;
    }

    pub fn meets_full_bar(&self) -> bool {
        FULL_BAR.iter().any(|&(i, rel, count)| {
            let s = &self.windows[i];
            s.effective() > rel && s.count > count
        })
    }

    pub(crate) fn encode(&self) -> String {
        let mut parts: Vec<String> = self
            .windows
            .iter()
            .flat_map(|s| [s.weight, s.reliability, s.count])
            .map(|v| v.to_string())
            .collect();
        parts.push(self.last_update.to_string());
        parts.join(",")
    }

    pub(crate) fn decode(s: &str) -> Option<Stats> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 16 {
            return None;
        }
        let mut out = Stats::default();
        for (i, stat) in out.windows.iter_mut().enumerate() {
            stat.weight = parts[i * 3].parse().ok()?;
            stat.reliability = parts[i * 3 + 1].parse().ok()?;
            stat.count = parts[i * 3 + 2].parse().ok()?;
        }
        out.last_update = parts[15].parse().ok()?;
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_text_format_is_pinned() {
        let literal = "1,0.5,2,0.75,0.25,3,0.5,0.125,4,0.25,0.0625,5,0.875,0.375,6,1234567";
        let w = |weight, reliability, count| Stat {
            weight,
            reliability,
            count,
        };
        let stats = Stats {
            windows: [
                w(1.0, 0.5, 2.0),
                w(0.75, 0.25, 3.0),
                w(0.5, 0.125, 4.0),
                w(0.25, 0.0625, 5.0),
                w(0.875, 0.375, 6.0),
            ],
            last_update: 1_234_567,
        };
        assert_eq!(stats.encode(), literal);
        assert_eq!(Stats::decode(literal), Some(stats));
    }

    #[test]
    fn first_success_sets_reliability_one() {
        let mut s = Stats::default();
        s.update(true, 1000);
        for w in &s.windows {
            assert_eq!((w.weight, w.reliability, w.count), (1.0, 1.0, 1.0));
        }
        let mut f = Stats::default();
        f.update(false, 1000);
        assert_eq!(f.windows[0].reliability, 0.0);
        assert_eq!(f.windows[0].weight, 1.0);
    }

    #[test]
    fn failure_one_window_later_leaves_e_inverse() {
        let mut s = Stats::default();
        s.update(true, 0);
        s.update(false, 7200);
        let w = s.windows[0];
        assert!((w.weight - 1.0).abs() < 1e-9);
        assert!((w.effective() - (-1.0f64).exp()).abs() < 1e-9);
    }

    #[test]
    fn full_bar_needs_count() {
        let mut s = Stats::default();
        s.update(true, 0);
        s.update(true, 600);
        assert!(!s.meets_full_bar());
        s.update(true, 1200);
        assert!(s.meets_full_bar());
    }

    #[test]
    fn mixed_sequence_matches_hand_computed_values_in_every_window() {
        // Taus written out literally so a typo in WINDOWS fails here.
        let taus = [7200.0, 28800.0, 86400.0, 604800.0, 2592000.0];
        let mut s = Stats::default();
        s.update(false, 0);
        s.update(true, 600);
        s.update(false, 1800);
        assert_eq!(s.last_update, 1800);
        for (i, tau) in taus.into_iter().enumerate() {
            assert_eq!(WINDOWS[i].1, tau);
            let f1 = (-600.0f64 / tau).exp();
            let f2 = (-1200.0f64 / tau).exp();
            let w = s.windows[i];
            assert!((w.weight - 1.0).abs() < 1e-12, "weight {i}");
            assert!((w.reliability - (1.0 - f1) * f2).abs() < 1e-12, "rel {i}");
            assert!(
                (w.count - ((f1 + 1.0) * f2 + 1.0)).abs() < 1e-12,
                "count {i}"
            );
        }
        // Spot-check one window against a literal number.
        assert!((s.windows[0].reliability - 0.0677).abs() < 1e-3);
    }

    #[test]
    fn window_names_are_in_order() {
        let names: Vec<_> = WINDOWS.iter().map(|w| w.0).collect();
        assert_eq!(names, ["2h", "8h", "1d", "1w", "1m"]);
    }

    fn only_window(i: usize, reliability: f64, count: f64) -> Stats {
        let mut s = Stats::default();
        s.windows[i] = Stat {
            weight: 1.0,
            reliability,
            count,
        };
        s
    }

    #[test]
    fn each_full_bar_rule_pins_both_thresholds() {
        // (window, reliability threshold, count threshold)
        let rules = [
            (0, 0.85, 2.0),
            (1, 0.70, 4.0),
            (2, 0.55, 8.0),
            (3, 0.45, 16.0),
            (4, 0.35, 32.0),
        ];
        for (i, rel, count) in rules {
            let eps = 1e-6;
            assert!(
                only_window(i, rel + eps, count + eps).meets_full_bar(),
                "above both {i}"
            );
            assert!(
                !only_window(i, rel - eps, count + eps).meets_full_bar(),
                "rel below {i}"
            );
            assert!(
                !only_window(i, rel, count + eps).meets_full_bar(),
                "rel equal {i}"
            );
            assert!(
                !only_window(i, rel + eps, count - eps).meets_full_bar(),
                "count below {i}"
            );
            assert!(
                !only_window(i, rel + eps, count).meets_full_bar(),
                "count equal {i}"
            );
            // Another window's data must not satisfy this rule.
            let other = (i + 1) % 5;
            assert!(!only_window(other, 1.0, 1.0).meets_full_bar());
        }
    }

    #[test]
    fn encode_roundtrips() {
        let mut s = Stats::default();
        s.update(true, 5);
        s.update(false, 900);
        assert_eq!(Stats::decode(&s.encode()), Some(s));
        assert_eq!(Stats::decode("garbage"), None);
    }
}
