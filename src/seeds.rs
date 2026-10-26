//! The `seeds` command's output: one `ip:port` per line, the input of a coin's
//! `contrib/seeds/generate-seeds.py`.

use std::collections::HashMap;

use crate::dns::answer::{MAX_PER_GROUP, netgroup};
use crate::store::Servable;

/// At most `max` lines, in the order of `nodes`, with no more than [`MAX_PER_GROUP`]
/// per network group. IPv6 is written `[addr]:port`.
pub fn format_seeds(nodes: &[Servable], max: usize) -> String {
    let mut per_group: HashMap<[u8; 4], usize> = HashMap::new();
    let mut out = String::new();
    let mut lines = 0;
    for node in nodes {
        if lines >= max {
            break;
        }
        let n = per_group.entry(netgroup(&node.addr.ip())).or_default();
        if *n >= MAX_PER_GROUP {
            continue;
        }
        *n += 1;
        lines += 1;
        out.push_str(&node.addr.to_string());
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(addr: &str) -> Servable {
        Servable {
            addr: addr.parse().unwrap(),
            services: 1,
        }
    }

    #[test]
    fn format_seeds_caps_groups_and_formats_v6() {
        let nodes = [
            node("203.0.113.1:10226"),
            node("203.0.113.2:10226"),
            node("203.0.113.3:10226"),
            node("[2001:db8::1]:10226"),
        ];
        assert_eq!(
            format_seeds(&nodes, 50),
            "203.0.113.1:10226\n203.0.113.2:10226\n[2001:db8::1]:10226\n"
        );
        assert_eq!(
            format_seeds(&nodes, 2),
            "203.0.113.1:10226\n203.0.113.2:10226\n"
        );
    }

    #[test]
    fn format_seeds_of_nothing_is_empty() {
        assert_eq!(format_seeds(&[], 50), "");
        assert_eq!(format_seeds(&[node("203.0.113.1:1")], 0), "");
    }
}
