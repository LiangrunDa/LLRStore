#![allow(unused)]
use clap::ValueEnum;
use linked_hash_map::LinkedHashMap;
use shared::{
    key::{Key, RingPos},
    value::{DvvSet, Value, VersionVec},
};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, PartialEq, Eq, Clone, Copy, ValueEnum)]
pub enum CacheEvictionStrategy {
    #[clap(name = "FIFO")]
    /// first in first out
    Fifo,
    #[clap(name = "LRU")]
    /// least recently used
    Lru,
    #[clap(name = "LFU")]
    /// least frequently used
    Lfu,
}

impl Default for CacheEvictionStrategy {
    fn default() -> Self {
        CacheEvictionStrategy::Fifo
    }
}

impl std::fmt::Display for CacheEvictionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheEvictionStrategy::Fifo => write!(f, "FIFO"),
            CacheEvictionStrategy::Lru => write!(f, "LRU"),
            CacheEvictionStrategy::Lfu => write!(f, "LFU"),
        }
    }
}

pub trait Cache {
    fn get(&mut self, key: &RingPos) -> Option<DvvSet>;
    fn put(&mut self, key: &RingPos, dvvset: &DvvSet);
    fn delete(&mut self, key: &RingPos);
    fn size(&self) -> usize;
    fn capacity(&self) -> usize;
}

pub struct FifoCache {
    capacity: usize,
    map: HashMap<RingPos, DvvSet>,
    queue: VecDeque<RingPos>,
}

impl FifoCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            queue: VecDeque::new(),
        }
    }
}

impl Cache for FifoCache {
    fn get(self: &mut FifoCache, key: &RingPos) -> Option<DvvSet> {
        self.map.get(key).cloned()
    }

    fn put(&mut self, key: &RingPos, value: &DvvSet) {
        if self.size() >= self.capacity {
            if let Some(old_key) = self.queue.pop_front() {
                self.map.remove(&old_key);
            }
        }

        self.queue.push_back(key.clone());
        self.map.insert(key.clone(), value.clone());
    }

    fn delete(&mut self, key: &RingPos) {
        self.map.remove(key);
        self.queue.retain(|k| k != key);
    }

    fn size(&self) -> usize {
        self.map.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct LruCache {
    capacity: usize,
    map: LinkedHashMap<RingPos, DvvSet>,
}

impl LruCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: LinkedHashMap::new(),
        }
    }
}

impl Cache for LruCache {
    fn get(self: &mut LruCache, key: &RingPos) -> Option<DvvSet> {
        if let Some(value) = self.map.get_refresh(key) {
            Some(value.clone())
        } else {
            None
        }
    }

    fn put(&mut self, key: &RingPos, value: &DvvSet) {
        if self.map.len() >= self.capacity {
            if let Some((old_key, _)) = self.map.pop_front() {
                self.map.remove(&old_key);
            }
        }

        self.map.insert(key.clone(), value.clone());
    }

    fn delete(&mut self, key: &RingPos) {
        self.map.remove(key);
    }

    fn size(&self) -> usize {
        self.map.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct LfuCache {
    capacity: usize,
    min_freq: usize,
    freq_map: HashMap<RingPos, usize>,
    key_freq_map: HashMap<usize, HashSet<RingPos>>,
    values: HashMap<RingPos, DvvSet>,
}

impl LfuCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            min_freq: 0,
            freq_map: HashMap::new(),
            key_freq_map: HashMap::new(),
            values: HashMap::new(),
        }
    }

    fn update(&mut self, key: RingPos) {
        let freq = self.freq_map[&key];
        self.freq_map.insert(key.clone(), freq + 1);
        self.key_freq_map.get_mut(&freq).unwrap().remove(&key);

        if self.key_freq_map[&freq].is_empty() {
            self.key_freq_map.remove(&freq);
            if self.min_freq == freq {
                self.min_freq += 1;
            }
        }

        self.key_freq_map
            .entry(freq + 1)
            .or_insert_with(HashSet::new)
            .insert(key.clone());
    }
}

impl Cache for LfuCache {
    fn get(&mut self, key: &RingPos) -> Option<DvvSet> {
        if self.values.contains_key(key) {
            let value = self.values.get(key).cloned();
            self.update(key.clone());
            value
        } else {
            None
        }
    }

    fn put(&mut self, key: &RingPos, value: &DvvSet) {
        if self.values.contains_key(key) {
            self.update(key.clone());
            self.values.insert(key.clone(), value.clone());
        } else {
            if self.values.len() >= self.capacity {
                let key_to_evict = self
                    .key_freq_map
                    .get(&self.min_freq)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap()
                    .clone();
                self.values.remove(&key_to_evict);
                self.freq_map.remove(&key_to_evict);
                self.key_freq_map
                    .get_mut(&self.min_freq)
                    .unwrap()
                    .remove(&key_to_evict);
            }
            self.values.insert(key.clone(), value.clone());
            self.freq_map.insert(key.clone(), 1);
            self.min_freq = 1;
            self.key_freq_map
                .entry(1)
                .or_insert_with(HashSet::new)
                .insert(key.clone());
        }
    }

    fn delete(&mut self, key: &RingPos) {
        if self.values.contains_key(key) {
            let freq = self.freq_map.remove(key).unwrap();
            self.key_freq_map.get_mut(&freq).unwrap().remove(key);
            self.values.remove(key);
        }
    }

    fn size(&self) -> usize {
        self.values.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// This type just exists to support static dispatch and avoid dynamic dispatch
/// with a trait object.
/// See https://www.possiblerust.com/guide/enum-or-trait-object for an explanation.
/// https://crates.io/crates/enum_delegate could reduce the boilerplate below and
/// auto generate the code..
pub enum SupportedCache {
    FifoCache(FifoCache),
    LruCache(LruCache),
    LfuCache(LfuCache),
}

impl SupportedCache {
    pub fn new(strategy: CacheEvictionStrategy, capacity: usize) -> Self {
        match strategy {
            CacheEvictionStrategy::Fifo => SupportedCache::FifoCache(FifoCache::new(capacity)),
            CacheEvictionStrategy::Lru => SupportedCache::LruCache(LruCache::new(capacity)),
            CacheEvictionStrategy::Lfu => SupportedCache::LfuCache(LfuCache::new(capacity)),
        }
    }
}

impl Cache for SupportedCache {
    fn get(self: &mut SupportedCache, key: &RingPos) -> Option<DvvSet> {
        match self {
            SupportedCache::FifoCache(cache) => cache.get(key),
            SupportedCache::LruCache(cache) => cache.get(key),
            SupportedCache::LfuCache(cache) => cache.get(key),
        }
    }

    fn put(&mut self, key: &RingPos, value: &DvvSet) {
        match self {
            SupportedCache::FifoCache(cache) => cache.put(key, value),
            SupportedCache::LruCache(cache) => cache.put(key, value),
            SupportedCache::LfuCache(cache) => cache.put(key, value),
        }
    }

    fn delete(&mut self, key: &RingPos) {
        match self {
            SupportedCache::FifoCache(cache) => cache.delete(key),
            SupportedCache::LruCache(cache) => cache.delete(key),
            SupportedCache::LfuCache(cache) => cache.delete(key),
        }
    }

    fn size(&self) -> usize {
        match self {
            SupportedCache::FifoCache(cache) => cache.size(),
            SupportedCache::LruCache(cache) => cache.size(),
            SupportedCache::LfuCache(cache) => cache.size(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            SupportedCache::FifoCache(cache) => cache.capacity(),
            SupportedCache::LruCache(cache) => cache.capacity(),
            SupportedCache::LfuCache(cache) => cache.capacity(),
        }
    }
}

// TODO: reenable tests once caches are generic over Key and Value
// or rewrite tests to use RingPos and DvvSet
// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_fifo_cache() {
//         let mut cache = FifoCache::new(2);

//         let leo_ring_pos = RingPos::from_key(&Key::new("Leo".to_string()));
//         let liangrun_ring_pos = RingPos::from_key(&Key::new("Liangrun".to_string()));
//         let robert_ring_pos = RingPos::from_key(&Key::new("Robert".to_string()));

//         // Test put
//         cache.put(&leo_ring_pos, &Value::new("Stewen".to_string()));
//         assert_eq!(cache.size(), 1);

//         // Test get
//         assert_eq!(
//             cache.get(&leo_ring_pos),
//             Some(Value::new("Stewen".to_string()))
//         );

//         // Test delete
//         cache.delete(&leo_ring_pos);
//         assert_eq!(cache.get(&leo_ring_pos), None);

//         // Test eviction
//         cache.put(&leo_ring_pos, &Value::new("Stewen".to_string()));
//         cache.put(&liangrun_ring_pos, &Value::new("Da".to_string()));
//         cache.put(&robert_ring_pos, &Value::new("Frank".to_string()));
//         assert_eq!(cache.get(&leo_ring_pos), None);
//         assert_eq!(
//             cache.get(&liangrun_ring_pos),
//             Some(Value::new("Da".to_string()))
//         );
//         assert_eq!(
//             cache.get(&robert_ring_pos),
//             Some(Value::new("Frank".to_string()))
//         );
//     }

//     #[test]
//     fn test_lru_cache() {
//         let mut cache = LruCache::new(2);

//         let leo = RingPos::from_key(&Key::new("Leo".to_string()));
//         let liangrun = RingPos::from_key(&Key::new("Liangrun".to_string()));
//         let robert = RingPos::from_key(&Key::new("Robert".to_string()));

//         // Test put
//         cache.put(&leo, &Value::new("Stewen".to_string()));
//         assert_eq!(cache.size(), 1);

//         // Test get
//         assert_eq!(cache.get(&leo), Some(Value::new("Stewen".to_string())));

//         // Test delete
//         cache.delete(&leo);
//         assert_eq!(cache.get(&leo), None);

//         // Test eviction
//         cache.put(&leo, &Value::new("Stewen".to_string()));
//         cache.put(&liangrun, &Value::new("Da".to_string()));
//         cache.get(&leo); // refresh Leo
//         cache.put(&robert, &Value::new("Frank".to_string())); // this should evict Liangrun as he is least recently used
//         assert_eq!(cache.get(&liangrun), None);
//         assert_eq!(cache.get(&leo), Some(Value::new("Stewen".to_string())));
//         assert_eq!(cache.get(&robert), Some(Value::new("Frank".to_string())));
//     }

//     #[test]
//     fn test_lfu_cache() {
//         let mut cache = LfuCache::new(2);

//         let leo = RingPos::from_key(&Key::new("Leo".to_string()));
//         let liangrun = RingPos::from_key(&Key::new("Liangrun".to_string()));
//         let robert = RingPos::from_key(&Key::new("Robert".to_string()));

//         // Test put
//         cache.put(&leo, &Value::new("Stewen".to_string()));
//         assert_eq!(cache.size(), 1);

//         // Test get
//         assert_eq!(cache.get(&leo), Some(Value::new("Stewen".to_string())));

//         // Test delete
//         cache.delete(&leo);
//         assert_eq!(cache.get(&leo), None);

//         // Test eviction
//         cache.put(&leo, &Value::new("Stewen".to_string()));
//         cache.put(&liangrun, &Value::new("Da".to_string()));
//         cache.get(&leo); // increasse frequency of Leo
//         cache.get(&leo); // increasse frequency of Leo
//         cache.put(&robert, &Value::new("Frank".to_string())); // this should evict Liangrun as he is least frequently used
//         assert_eq!(cache.get(&liangrun), None);
//         assert_eq!(cache.get(&leo), Some(Value::new("Stewen".to_string())));
//         assert_eq!(cache.get(&robert), Some(Value::new("Frank".to_string())));
//     }
// }
