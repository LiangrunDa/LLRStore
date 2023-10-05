#[derive(Debug, Clone)]
pub struct Config {
    name: Option<String>,
    addr: std::net::IpAddr,
    port: u16,
    custom_info: Option<Vec<u8>>
    // gossip_interval: std::time::Duration,
    // ping_interval: std::time::Duration,
    // ping_timeout: std::time::Duration,
}

impl Config {
    pub fn name(&self) -> Option<String> {
        self.name.clone()
    }
    pub fn addr(&self) -> std::net::IpAddr {
        self.addr
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn custom_info(&self) -> Option<Vec<u8>> {
        self.custom_info.clone()
    }
}

impl Config {
    pub fn new(name: Option<String>, addr: std::net::IpAddr, port: u16, custom_info: Option<Vec<u8>>) -> Self {
        Self { name, addr, port, custom_info }
    }
}
