use anyhow::anyhow;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::string::FromUtf8Error;

// watch out: hash and partial eq may have to be implemented manually
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    inner: String,
}

impl Key {
    pub fn new(key: String) -> Self {
        Self { inner: key }
    }
    /// Returns the length of the string in _bytes_.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl From<String> for Key {
    fn from(key: String) -> Self {
        Self { inner: key }
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self {
            inner: value.to_string(),
        }
    }
}

impl From<Key> for Vec<u8> {
    fn from(key: Key) -> Self {
        key.inner.into_bytes()
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_bytes()
    }
}

impl TryFrom<&[u8]> for Key {
    type Error = FromUtf8Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        String::from_utf8(value.to_vec()).map(|s| Self { inner: s })
    }
}

impl TryFrom<Vec<u8>> for Key {
    type Error = FromUtf8Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        String::from_utf8(value).map(|s| Self { inner: s })
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RingPos {
    hash: [u8; Self::KEY_LEN],
}

impl RingPos {
    // size in bytes
    const KEY_LEN: usize = 16;

    #[cfg(test)]
    fn new(hash: [u8; Self::KEY_LEN]) -> Self {
        Self { hash }
    }
    pub const fn len() -> usize {
        Self::KEY_LEN
    }
    pub const fn min() -> Self {
        Self {
            hash: [0x00; Self::KEY_LEN],
        }
    }
    #[cfg(test)]
    const fn quarter() -> Self {
        Self {
            hash: [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff,
                0xff, 0xff,
            ],
        }
    }
    #[cfg(test)]
    const fn mid() -> Self {
        Self {
            hash: [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff,
            ],
        }
    }
    #[cfg(test)]
    const fn three_quarters() -> Self {
        Self {
            hash: [
                0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff,
            ],
        }
    }
    pub const fn max() -> Self {
        Self {
            hash: [0xff; Self::KEY_LEN],
        }
    }
    fn previous(&self) -> Self {
        let mut hash = self.hash.clone();
        for i in (0..Self::KEY_LEN).rev() {
            if hash[i] == 0 {
                hash[i] = 0xff;
            } else {
                hash[i] -= 1;
                break;
            }
        }
        Self { hash }
    }
    pub fn from_socket_addr(addr: &std::net::SocketAddr) -> Self {
        let string = format!("{}:{}", addr.ip(), addr.port());
        let md5_hash = Md5::digest(string.as_bytes());
        Self {
            hash: md5_hash.into(),
        }
    }
    pub fn from_key(key: &Key) -> Self {
        let md5_hash = Md5::digest(key.as_ref());
        Self {
            hash: md5_hash.into(),
        }
    }
    pub fn from_str<T: AsRef<str>>(value: T) -> Self {
        let md5_hash = Md5::digest(value.as_ref().as_bytes());
        Self {
            hash: md5_hash.into(),
        }
    }
}

impl From<&SocketAddr> for RingPos {
    fn from(addr: &SocketAddr) -> Self {
        Self::from_socket_addr(addr)
    }
}

impl From<&Key> for RingPos {
    fn from(key: &Key) -> Self {
        Self::from_key(&key)
    }
}

impl AsRef<[u8]> for RingPos {
    fn as_ref(&self) -> &[u8] {
        self.hash.as_ref()
    }
}

impl TryFrom<Vec<u8>> for RingPos {
    type Error = Vec<u8>;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self {
            hash: value.try_into()?,
        })
    }
}

impl TryFrom<String> for RingPos {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        <Self as FromStr>::from_str(&value)
    }
}

impl FromStr for RingPos {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 16];
        // use base16ct to decode hex string
        let res = base16ct::lower::decode(s.trim(), &mut bytes);
        if res.is_ok() {
            Ok(RingPos { hash: bytes })
        } else {
            Err(anyhow!("invalid ring position {}", s))
        }
    }
}

impl Display for RingPos {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buf = [0u8; Self::KEY_LEN * 2];
        let str = base16ct::lower::encode_str(&self.hash, &mut buf).unwrap();
        write!(f, "{}", str)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingRange {
    /// inclusive
    from: RingPos,
    /// exclusive
    to: RingPos,
}

// useful, to make use of the std::mem::take trick
impl Default for RingRange {
    fn default() -> Self {
        Self {
            from: RingPos::min(),
            to: RingPos::max(),
        }
    }
}

impl RingRange {
    pub fn new(from: RingPos, to: RingPos) -> Self {
        Self { from, to }
    }
    pub fn full_range() -> Self {
        Self {
            from: RingPos::min(),
            to: RingPos::max(), // should it be min(), too? If so, adjust is_empty()
        }
    }
    pub fn contains(&self, pos: &RingPos) -> bool {
        if self.from < self.to {
            self.from <= *pos && *pos < self.to
        } else {
            self.from <= *pos || *pos < self.to
        }
    }
    pub fn from(&self) -> &RingPos {
        &self.from
    }
    pub fn to(&self) -> &RingPos {
        &self.to
    }
    pub fn wraps(&self) -> bool {
        self.from > self.to
    }
    pub fn overlaps(&self, other: &Self) -> bool {
        self.contains(&other.from) || self.contains(&other.to.previous())
    }
    pub fn is_subset(&self, of: &Self) -> bool {
        of.contains(&self.from) && of.contains(&self.to.previous())
    }
    pub fn is_empty(&self) -> bool {
        self.from == self.to
    }

    pub fn difference(&self, new: &Self) -> Option<Vec<Self>> {
        if self.is_subset(new) {
            // also covers equality
            return None;
        }
        if new.is_subset(&self) {
            if self.from == new.from {
                // filter out empty ranges (case self.from == new.from)
                return Some(vec![Self::new(new.to.clone(), self.to.clone())]);
            }
            return Some(vec![
                Self::new(self.from.clone(), new.from.clone()),
                Self::new(new.to.clone(), self.to.clone()),
            ]);
        }
        if new.from <= self.from {
            Some(vec![Self::new(new.to.clone(), self.to.clone())])
        } else {
            Some(vec![Self::new(self.from.clone(), new.from.clone())])
        }
    }

    pub fn consecutive_ranges_iter(&self) -> RingRangesIter {
        if self.wraps() {
            RingRangesIter {
                first_range: Some(RingRange::new(self.from, RingPos::max())),
                second_range: Some(RingRange::new(RingPos::min(), self.to)),
            }
        } else {
            RingRangesIter {
                first_range: Some(self.clone()),
                second_range: None,
            }
        }
    }
}

impl From<&RingRange> for protocol::kv_kv::KeyRange {
    fn from(range: &RingRange) -> Self {
        Self {
            from: range.from.to_string(),
            to: range.to.to_string(),
        }
    }
}

pub struct RingRangesIter {
    first_range: Option<RingRange>,
    // in case of a wrapping range we have a second range
    second_range: Option<RingRange>,
}

impl Iterator for RingRangesIter {
    type Item = RingRange;

    fn next(&mut self) -> Option<Self::Item> {
        self.first_range.take().or_else(|| self.second_range.take())
    }
}

impl Display for RingRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.wraps() {
            write!(f, "[{}, {})", self.from, self.to)
        } else {
            write!(f, "[{}, {})", self.from, RingPos::max())?;
            write!(f, " U [{} {})", RingPos::min(), self.to)
        }
    }
}

impl Debug for RingRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug, Clone)]
pub struct MetaData {
    // map from SocketAddr RingPos to Keys RingRange
    map: HashMap<RingPos, RingRange>,
    // sorted RingPos in the ring
    hash_ring: Vec<RingPos>,
    // RingPos to socket addr for encoding
    to_addr: HashMap<RingPos, SocketAddr>,
}

impl Default for MetaData {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            hash_ring: Vec::new(),
            to_addr: HashMap::new(),
        }
    }
}

// just used for serialization and deserialization of the
// keyrange and keyrange_read commands
#[derive(Debug, Clone)]
pub struct RangeServerInfo {
    pub range: RingRange,
    addr: SocketAddr,
}

impl RangeServerInfo {
    pub fn client_addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn kv_addr(&self, kv_port: u16) -> SocketAddr {
        let mut addr = self.addr;
        addr.set_port(kv_port);
        addr
    }
}

impl Display for RangeServerInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{},{}", self.range.from, self.range.to, self.addr)
    }
}

impl FromStr for RangeServerInfo {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        let from = parts.next().ok_or_else(|| anyhow!("missing from"))?;
        let to = parts.next().ok_or_else(|| anyhow!("missing to"))?;
        let addr = parts.next().ok_or_else(|| anyhow!("missing addr"))?;
        Ok(Self {
            range: RingRange::new(
                <RingPos as FromStr>::from_str(from)?,
                <RingPos as FromStr>::from_str(to)?,
            ),
            addr: addr.parse()?,
        })
    }
}

// <kr-from>, <kr-to>, <ip:port>; <kr-from>, <kr-to>, <ip:port>;
impl From<MetaData> for String {
    fn from(value: MetaData) -> Self {
        let mut buf = String::new();
        for pos in value.hash_ring {
            let range = &value.map[&pos];
            // convert range.from to hex string

            buf.push_str(&format!(
                "{},{},{};",
                range.from, range.to, value.to_addr[&pos]
            ));
        }
        buf
    }
}

impl TryFrom<String> for MetaData {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut map = HashMap::new();
        let mut hash_ring = Vec::new();
        let mut to_addr = HashMap::new();
        // every server node is seperated by ;
        let mut iter = value.split(';');

        while let Some(node) = iter.next() {
            // every value is seperated by ,
            let mut node_iter = node.split(',');
            if node_iter.clone().count() != 3 {
                continue;
            }

            let from = node_iter.next().and_then(|s| {
                let hex_string = s.trim().to_string();
                hex_string.try_into().ok()
            });

            let to = node_iter.next().and_then(|s| {
                let hex_string = s.trim().to_string();
                hex_string.try_into().ok()
            });

            let addr = node_iter.next().and_then(|s| {
                let s = s.trim();
                s.parse::<SocketAddr>().ok()
            });

            if from.is_none() || to.is_none() || addr.is_none() {
                return Err(anyhow!("Could not parse {} as metadata", value));
            }

            let addr_hash = RingPos::from_socket_addr(&addr.unwrap());

            let range = RingRange::new(from.unwrap(), to.unwrap());

            map.insert(addr_hash.clone(), range.clone());
            hash_ring.push(addr_hash.clone());
            to_addr.insert(addr_hash.clone(), addr.unwrap());
        }
        hash_ring.sort();
        Ok(Self {
            map,
            hash_ring,
            to_addr,
        })
    }
}

impl MetaData {
    // TODO: remove and replace with default()
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            hash_ring: Vec::new(),
            to_addr: HashMap::new(),
        }
    }

    pub fn all_coordinators(&self) -> Vec<RangeServerInfo> {
        self.hash_ring
            .iter()
            .map(|pos| {
                let range = self.map[pos].clone();
                let addr = self.to_addr[pos];
                RangeServerInfo { range, addr }
            })
            .collect()
    }

    pub fn all_replicas(&self, replication_factor: NonZeroUsize) -> Vec<RangeServerInfo> {
        self.hash_ring
            .iter()
            .flat_map(|pos| {
                // for each server node, get all of its server ranges including
                // the one it is only a replica of
                let origin_addr = self.to_addr[pos];
                self.get_server_ranges(pos.clone(), replication_factor)
                    .into_iter()
                    .map(move |mut range| {
                        range.addr = origin_addr;
                        range
                    })
            })
            .collect()
    }

    fn insert_into_ring(&mut self, pos: RingPos) {
        let index = self.hash_ring.binary_search(&pos).unwrap_or_else(|x| x);
        self.hash_ring.insert(index, pos);
    }

    fn remove_from_ring(&mut self, pos: RingPos) {
        let index = self.hash_ring.binary_search(&pos).unwrap_or_else(|x| x);
        self.hash_ring.remove(index);
    }

    fn get_successor(&self, pos: RingPos) -> Option<RingPos> {
        if self.hash_ring.len() == 1 {
            return None;
        }
        let index = self.hash_ring.binary_search(&pos).unwrap_or_else(|x| x);
        if index == self.hash_ring.len() - 1 {
            Some(self.hash_ring[0])
        } else {
            Some(self.hash_ring[index + 1])
        }
    }

    fn get_predecessor(&self, pos: RingPos) -> Option<RingPos> {
        if self.hash_ring.len() == 1 {
            None
        } else {
            let index = self.hash_ring.binary_search(&pos).unwrap_or_else(|x| x);
            if index == 0 {
                Some(self.hash_ring[self.hash_ring.len() - 1])
            } else {
                Some(self.hash_ring[index - 1])
            }
        }
    }

    fn insert_server(&mut self, pos: RingPos, addr: SocketAddr) {
        self.insert_into_ring(pos);
        self.to_addr.insert(pos, addr);

        let pred = self.get_predecessor(pos);
        let succ = self.get_successor(pos);

        if pred.is_none() {
            // only one server in the ring, it handles all keys
            // [pos, pos) is the whole key range
            self.map.insert(pos, RingRange::new(pos, pos));
        } else {
            // insert new server's key range
            self.map.insert(pos, RingRange::new(pred.unwrap(), pos));
            // update successor's key range
            self.map
                .insert(succ.unwrap(), RingRange::new(pos, succ.unwrap()));
        }
    }

    pub fn upsert_server(&mut self, addr: SocketAddr) -> Result<bool, ()> {
        let pos = RingPos::from_socket_addr(&addr);
        if self.map.contains_key(&pos) {
            // server already exists, do nothing
            return Ok(false);
        }
        self.insert_server(pos, addr);
        Ok(true)
    }

    // insert into the map if it doesn't exist, otherwise return an error
    pub fn add_server(&mut self, addr: SocketAddr) -> Result<(), ()> {
        let pos = RingPos::from_socket_addr(&addr);
        if !self.map.contains_key(&pos) {
            self.insert_server(pos, addr);
            Ok(())
        } else {
            Err(())
        }
    }

    // remove from the map if it exists, otherwise return an error
    pub fn remove_server(&mut self, addr: SocketAddr) -> Result<(), ()> {
        let pos = RingPos::from_socket_addr(&addr);
        if self.map.contains_key(&pos) {
            let pred = self.get_predecessor(pos);
            let succ = self.get_successor(pos);
            self.remove_from_ring(pos);
            self.to_addr.remove(&pos);
            if pred.is_none() {
                self.map.remove(&pos);
                Ok(())
            } else {
                // update successor's key range
                self.map
                    .insert(succ.unwrap(), RingRange::new(pred.unwrap(), succ.unwrap()));
                // remove server's key range
                self.map.remove(&pos);
                Ok(())
            }
        } else {
            Err(())
        }
    }

    pub fn get_server_list(&self) -> Vec<SocketAddr> {
        self.to_addr.values().map(|x| x.clone()).collect()
    }

    pub fn total_servers(&self) -> usize {
        self.to_addr.len()
    }

    pub fn get_primary_range_by_addr(&self, addr: SocketAddr) -> Option<RingRange> {
        let pos = RingPos::from_socket_addr(&addr);
        self.map.get(&pos).map(|x| x.clone())
    }

    /// Returns a sorted vector where the first element is the primary range
    /// and the rest are secondary ranges sorted in *counter*-clockwise order,
    /// that is:
    /// vec[0] = primary range
    /// vec[1] = secondary range 1 (adjacent in counter-clockwise direction to primary range)
    /// vec[2] = secondary range 2 (adjacent in counter-clockwise direction to secondary range 1)
    /// ...
    pub fn get_server_ranges(
        &self,
        addr: impl Into<RingPos>, // this arg identifies the server (usually a socket addr)
        replication_factor: NonZeroUsize,
    ) -> Vec<RangeServerInfo> {
        let pos: RingPos = addr.into();
        let total_servers = self.hash_ring.len();

        let mut index = match self.hash_ring.binary_search(&pos) {
            Ok(index) => index,
            Err(_potential_index) => return Vec::new(),
        };

        let entries = usize::min(total_servers, usize::from(replication_factor));
        (0..entries)
            .map(|_| {
                let pos = &self.hash_ring[index];
                let range = self.map[pos];
                let addr = self.to_addr[pos];
                // move index counter-clockwise to find ranges given a socket addr
                if index == 0 {
                    index = total_servers - 1;
                } else {
                    index -= 1;
                }
                RangeServerInfo { range, addr }
            })
            .collect()
    }

    /// Returns a sorted vector where the first element is the primary server
    /// for that key and the rest are secondary servers sorted in
    /// *clockwise* order, that is:
    /// vec[0] = primary server responsible for writes (and reads) to that key
    /// vec[1] = secondary server 1 handling only reads of that key
    ///          (adjacent in clockwise direction to primary server)
    /// vec[2] = secondary server 2 handling only reads of that key
    ///          (adjacent in clockwise direction to secondary server 1)
    /// ...
    pub fn get_key_servers(
        &self,
        key: impl Into<RingPos>,
        replication_factor: NonZeroUsize,
    ) -> Vec<RangeServerInfo> {
        let pos: RingPos = key.into();
        let total_servers = self.hash_ring.len();

        let mut index = self
            .hash_ring
            .binary_search(&pos)
            .unwrap_or_else(|potential_index| {
                if potential_index == total_servers {
                    // wrapping case
                    0
                } else {
                    potential_index
                }
            });

        let entries = usize::min(total_servers, usize::from(replication_factor));
        (0..entries)
            .map(|_| {
                let pos = &self.hash_ring[index];
                let range = self.map[pos];
                let addr = self.to_addr[pos];
                // move index clockwise to find servers given a key
                if index == total_servers - 1 {
                    index = 0;
                } else {
                    index += 1;
                }
                RangeServerInfo { range, addr }
            })
            .collect()
    }

    pub fn get_pred_addr<T: Into<RingPos>>(&self, addr: T) -> Option<SocketAddr> {
        let pos: RingPos = addr.into();
        self.get_predecessor(pos)
            .and_then(|x| self.to_addr.get(&x).map(|x| x.clone()))
    }

    pub fn get_succ_addr<T: Into<RingPos>>(&self, addr: T) -> Option<SocketAddr> {
        let pos: RingPos = addr.into();
        self.get_successor(pos)
            .and_then(|x| self.to_addr.get(&x).map(|x| x.clone()))
    }

    pub fn get_server_addr_by_key<T: Into<RingPos>>(&self, key: T) -> Option<SocketAddr> {
        let pos: RingPos = key.into();
        for (server, range) in self.map.iter() {
            if range.contains(&pos) {
                return self.to_addr.get(server).map(|x| x.clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test() {
        let hash = [192u8];
        let mut buf = [0u8; 2];
        let str = base16ct::lower::encode_str(&hash, &mut buf).unwrap();
        println!("{}", str);
    }

    #[test]
    fn test_metadata_encoding() {
        let mut metadata = MetaData::new();
        let server0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let server1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);
        let server2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8082);

        let server0_hash = RingPos::from_socket_addr(&server0);
        let server1_hash = RingPos::from_socket_addr(&server1);
        let server2_hash = RingPos::from_socket_addr(&server2);

        let mut hashes = vec![
            (server0, server0_hash),
            (server1, server1_hash),
            (server2, server2_hash),
        ];
        // sort pairs by hash value
        hashes.sort_by(|a, b| a.1.cmp(&b.1));

        metadata.add_server(hashes[0].0).unwrap();
        metadata.add_server(hashes[1].0).unwrap();
        metadata.add_server(hashes[2].0).unwrap();

        let encoded: String = metadata.try_into().unwrap();
        let expect = format!(
            "{}, {}, {}; {}, {}, {}; {}, {}, {}",
            hashes[2].1,
            hashes[0].1,
            hashes[0].0,
            hashes[0].1,
            hashes[1].1,
            hashes[1].0,
            hashes[1].1,
            hashes[2].1,
            hashes[2].0
        );
        assert_eq!(encoded, expect);
    }

    #[test]
    fn test_metadata_decoding() {
        let mut metadata = MetaData::new();
        let server0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let server1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);
        let server2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8082);

        let server0_hash = RingPos::from_socket_addr(&server0);
        let server1_hash = RingPos::from_socket_addr(&server1);
        let server2_hash = RingPos::from_socket_addr(&server2);

        let mut hashes = vec![
            (server0, server0_hash),
            (server1, server1_hash),
            (server2, server2_hash),
        ];
        // sort pairs by hash value
        hashes.sort_by(|a, b| a.1.cmp(&b.1));

        metadata.add_server(hashes[0].0).unwrap();
        metadata.add_server(hashes[1].0).unwrap();
        metadata.add_server(hashes[2].0).unwrap();

        let encoded: String = metadata.try_into().unwrap();
        let mut decoded: MetaData = encoded.try_into().unwrap();

        let range0 = decoded.get_primary_range_by_addr(hashes[0].0).unwrap();
        assert_eq!(range0, RingRange::new(hashes[2].1, hashes[0].1));

        let range1 = decoded.get_primary_range_by_addr(hashes[1].0).unwrap();
        assert_eq!(range1, RingRange::new(hashes[0].1, hashes[1].1));

        let range2 = decoded.get_primary_range_by_addr(hashes[2].0).unwrap();
        assert_eq!(range2, RingRange::new(hashes[1].1, hashes[2].1));

        decoded.remove_server(hashes[0].0).unwrap();
        let range1_new = decoded.get_primary_range_by_addr(hashes[1].0).unwrap();
        assert_eq!(range1_new, RingRange::new(hashes[2].1, hashes[1].1));
    }

    #[test]
    fn test_metadata() {
        let mut metadata = MetaData::new();
        let server0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let server1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);
        let server2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8082);

        let server0_hash = RingPos::from_socket_addr(&server0);
        let server1_hash = RingPos::from_socket_addr(&server1);
        let server2_hash = RingPos::from_socket_addr(&server2);

        let mut hashes = vec![
            (server0, server0_hash),
            (server1, server1_hash),
            (server2, server2_hash),
        ];
        // sort pairs by hash value
        hashes.sort_by(|a, b| a.1.cmp(&b.1));

        metadata.add_server(hashes[0].0).unwrap();
        let range0 = metadata.get_primary_range_by_addr(hashes[0].0).unwrap();
        assert_eq!(range0, RingRange::new(hashes[0].1, hashes[0].1));

        metadata.add_server(hashes[1].0).unwrap();
        let range1 = metadata.get_primary_range_by_addr(hashes[1].0).unwrap();
        assert_eq!(range1, RingRange::new(hashes[0].1, hashes[1].1));
        let range0_new = metadata.get_primary_range_by_addr(hashes[0].0).unwrap();
        assert_eq!(range0_new, RingRange::new(hashes[1].1, hashes[0].1));

        metadata.add_server(hashes[2].0).unwrap();
        let range2 = metadata.get_primary_range_by_addr(hashes[2].0).unwrap();
        assert_eq!(range2, RingRange::new(hashes[1].1, hashes[2].1));
        let range1_new = metadata.get_primary_range_by_addr(hashes[1].0).unwrap();
        assert_eq!(range1_new, RingRange::new(hashes[0].1, hashes[1].1));
        let range0_new = metadata.get_primary_range_by_addr(hashes[0].0).unwrap();
        assert_eq!(range0_new, RingRange::new(hashes[2].1, hashes[0].1));

        metadata.remove_server(hashes[1].0).unwrap();
        let range2_new = metadata.get_primary_range_by_addr(hashes[2].0).unwrap();
        assert_eq!(range2_new, RingRange::new(hashes[0].1, hashes[2].1));
        let range0_new = metadata.get_primary_range_by_addr(hashes[0].0).unwrap();
        assert_eq!(range0_new, RingRange::new(hashes[2].1, hashes[0].1));
        let range1_new = metadata.get_primary_range_by_addr(hashes[1].0);
        assert!(range1_new.is_none());

        metadata.remove_server(hashes[0].0).unwrap();
        let range2_new = metadata.get_primary_range_by_addr(hashes[2].0).unwrap();
        assert_eq!(range2_new, RingRange::new(hashes[2].1, hashes[2].1));
    }

    #[test]
    fn test_ring_pos_cmp() {
        let pos1 = RingPos::new([0x88; RingPos::KEY_LEN]);
        let pos2 = RingPos::new([0xff; RingPos::KEY_LEN]);

        assert!(pos1 < pos2);

        let pos1 = RingPos::new([
            0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ]);
        let pos2 = RingPos::new([0, 0, 0, 0, 0, 0, 0, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]);

        assert!(pos1 < pos2);

        let min = RingPos::min();
        assert_eq!(min.previous(), RingPos::max());
    }

    #[test]
    fn test_ring_range() {
        let server1 = RingRange::new(RingPos::quarter(), RingPos::three_quarters());
        let server2 = RingRange::new(RingPos::three_quarters(), RingPos::quarter());
        let server3 = RingRange::new(RingPos::min(), RingPos::quarter());

        // server1 and server2 partition the ring
        assert!(!server1.overlaps(&server2));
        assert!(!server2.overlaps(&server1));

        assert!(server2.overlaps(&server3));
        assert!(server3.overlaps(&server2));

        let min = RingPos::min();
        let quarter = RingPos::quarter();
        let mid = RingPos::mid();
        let three_quarters = RingPos::three_quarters();
        let max = RingPos::max();

        assert!(!server1.contains(&min));
        assert!(server1.contains(&quarter));
        assert!(server1.contains(&mid));
        assert!(!server1.contains(&three_quarters));
        assert!(!server1.contains(&max));

        assert!(server2.contains(&min));
        assert!(!server2.contains(&quarter));
        assert!(!server2.contains(&mid));
        assert!(server2.contains(&max));
        assert!(server2.contains(&three_quarters));
    }

    #[test]
    fn test_difference() {
        // equality
        let a = RingRange::new(RingPos::min(), RingPos::max());
        let b = RingRange::new(RingPos::min(), RingPos::max());
        assert_eq!(a.difference(&b), None);
        assert_eq!(b.difference(&a), None);

        // no overlap
        let a = RingRange::new(RingPos::quarter(), RingPos::mid());
        let b = RingRange::new(RingPos::mid(), RingPos::three_quarters());
        assert_eq!(a.difference(&b), Some(vec!(a.clone())));
        assert_eq!(b.difference(&a), Some(vec!(b.clone())));

        // contains with two ranges
        let a = RingRange::new(RingPos::min(), RingPos::max());
        let b = RingRange::new(RingPos::quarter(), RingPos::three_quarters());
        assert_eq!(
            a.difference(&b),
            Some(vec![
                RingRange::new(RingPos::min(), RingPos::quarter()),
                RingRange::new(RingPos::three_quarters(), RingPos::max()),
            ])
        );
        assert_eq!(b.difference(&a), None);

        // contains
        let a = RingRange::new(RingPos::min(), RingPos::max());
        let b = RingRange::new(RingPos::quarter(), RingPos::three_quarters());
        assert_eq!(
            a.difference(&b),
            Some(vec![
                RingRange::new(RingPos::min(), RingPos::quarter()),
                RingRange::new(RingPos::three_quarters(), RingPos::max()),
            ])
        );

        // partial overlap, from is lower or equal
        let a = RingRange::new(RingPos::min(), RingPos::mid());
        let b = RingRange::new(RingPos::min(), RingPos::quarter());
        assert_eq!(
            a.difference(&b),
            Some(vec!(RingRange::new(RingPos::quarter(), RingPos::mid())))
        );
        assert_eq!(b.difference(&a), None);

        // partial overlap, from is higher
        let a = RingRange::new(RingPos::min(), RingPos::mid());
        let b = RingRange::new(RingPos::quarter(), RingPos::three_quarters());
        assert_eq!(
            a.difference(&b),
            Some(vec!(RingRange::new(RingPos::min(), RingPos::quarter())))
        );
        assert_eq!(
            b.difference(&a),
            Some(vec!(RingRange::new(
                RingPos::mid(),
                RingPos::three_quarters()
            )))
        );

        // both wrap around
        let a = RingRange::new(RingPos::three_quarters(), RingPos::quarter());
        let b = RingRange::new(RingPos::three_quarters(), RingPos::mid());
        assert_eq!(a.difference(&b), None);
        assert_eq!(
            b.difference(&a),
            Some(vec!(RingRange::new(RingPos::quarter(), RingPos::mid())))
        );

        // just one wraps around
        let a = RingRange::new(RingPos::three_quarters(), RingPos::mid());
        let b = RingRange::new(RingPos::min(), RingPos::quarter());
        assert_eq!(
            a.difference(&b),
            Some(vec![
                RingRange::new(RingPos::three_quarters(), RingPos::min()),
                RingRange::new(RingPos::quarter(), RingPos::mid()),
            ])
        );
        assert_eq!(b.difference(&a), None);

        let a = RingRange::new(RingPos::three_quarters(), RingPos::min());
        let b = RingRange::new(RingPos::mid(), RingPos::max());
        assert_eq!(
            b.difference(&a),
            Some(vec![RingRange::new(
                RingPos::mid(),
                RingPos::three_quarters()
            ),])
        );
        assert_eq!(
            a.difference(&b),
            Some(vec![RingRange::new(RingPos::max(), RingPos::min())])
        );
    }
}
