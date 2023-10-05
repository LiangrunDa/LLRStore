pub mod ecs_kv;
pub mod kv_ecs;
pub mod kv_kv;
pub mod membership_kv;
pub mod kv_membership;

/// the default port of the KV server for client requests
pub const KV_CLIENT_DEFAULT_PORT: u16 = 5551;
/// the default port of the ECS server for KV server requests
pub const ECS_KV_DEFAULT_PORT: u16 = 5144;
/// the default port of the KV server for ECS server requests
pub const KV_ECS_DEFAULT_PORT: u16 = 5145;
/// the default port of the KV server for KV server requests from other KV servers
pub const KV_KV_DEFAULT_PORT: u16 = 5146;
/// the default port of the ECS server for KV server requests
pub const MEM_KV_DEFAULT_PORT: u16 = 5144;
/// the default port of the KV server for ECS server requests
pub const KV_MEM_DEFAULT_PORT: u16 = 5145;
/// the default gossip port
pub const GOSSIP_DEFAULT_PORT: u16 = 5147;
