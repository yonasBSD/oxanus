use serde::Serialize;

#[derive(Serialize, oxana::Queue)]
#[oxana(key = "static_queue", discovery_interval_ms = 250)]
struct StaticQueue;

fn main() {}
