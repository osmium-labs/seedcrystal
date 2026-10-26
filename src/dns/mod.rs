//! Authoritative DNS for the seeder: pure answer building and the UDP/TCP servers.

pub mod answer;
pub mod querylog;
pub mod server;

pub use answer::{Target, Zone, ZoneSet, build_answer, parse_target};
pub use server::{Bound, bind, serve};
