//! This module is for _persistent_ storage of key-value pairs.

// I think a Log and Index storage engine is a good fit for good write performance
// and acceptable read performance. Memory mapped IO is also interesting:
// https://crates.io/crates/memmap2

#![allow(dead_code)]

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use shared::key::RingPos;
use shared::key::RingRange;
use shared::value::DvvSet;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::{
    io::{prelude::*, BufReader, SeekFrom},
    path::PathBuf,
};
use tracing::trace;

pub trait Storage {
    fn get(&self, key: &RingPos) -> Result<Option<DvvSet>>;
    fn get_within_range(&self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>>;
    /// Just stores the dvvset
    fn store(&mut self, key: &RingPos, dvvset: &DvvSet) -> Result<()>;
    /// Deletes the key entirely as if it was never stored in the first place.
    /// Not client-facing.
    /// Client requests should modify the DvvSet obtained by `get` and then
    /// `store` it again.
    fn reset(&mut self, key: &RingPos) -> Result<Option<DvvSet>>;
    /// Only used for getting rid of a range entirely because the server
    /// is not a replica for it anymore. Not client-facing.
    fn reset_within_range(&mut self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>>;
    fn size(&self) -> usize;
}

pub struct LogIndexStorage {
    data_dir: PathBuf,
    disk_log: DiskLog,
    mem_index: MemIndex,
}

impl LogIndexStorage {
    pub fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self> {
        let data_dir: PathBuf = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        let mut mem_index = MemIndex::new();
        let disk_log = DiskLog::from_disk(&data_dir, &mut mem_index)?;
        Ok(Self {
            data_dir,
            disk_log,
            mem_index,
        })
    }
}

impl Storage for LogIndexStorage {
    fn get(&self, key: &RingPos) -> Result<Option<DvvSet>> {
        let mem_index_entry = self.mem_index.get(key);
        match mem_index_entry {
            Some(mem_index_entry) => self
                .disk_log
                .get(&mem_index_entry)?
                .ok_or(anyhow!(
                    "mem_index_entry {:?} points to a non-existent value in disk log",
                    mem_index_entry
                ))
                .map(Some),
            None => Ok(None),
        }
    }

    fn get_within_range(&self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>> {
        let mut result = Vec::new();
        for (ring_pos, mem_index_entry) in self.mem_index.get_within_range(range) {
            let value = self.disk_log.get(mem_index_entry)?.ok_or(anyhow!(
                "mem_index_entry {:?} points to a non-existent value in disk log",
                mem_index_entry
            ))?;
            result.push((ring_pos.clone(), value));
        }
        Ok(result)
    }

    fn store(&mut self, key: &RingPos, value: &DvvSet) -> Result<()> {
        let new_mem_index_entry = self.disk_log.store(key, Some(value))?;
        self.mem_index.put(key.clone(), new_mem_index_entry);
        Ok(())
    }

    fn reset(&mut self, key: &RingPos) -> Result<Option<DvvSet>> {
        match self.mem_index.delete(key) {
            Some(old_mem_index_entry) => {
                let old_value = self.disk_log.get(&old_mem_index_entry);
                let _new_mem_index_entry = self.disk_log.store(key, None)?;
                old_value
            }
            None => {
                trace!("{:?} not found in mem_index, nothing to delete", key);
                Ok(None)
            }
        }
    }

    fn reset_within_range(&mut self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>> {
        self.mem_index
            .delete_within_range(range)
            .into_iter()
            .map(|(ring_pos, old_mem_index_entry)| {
                let old_dvvs = self
                    .disk_log
                    .get(&old_mem_index_entry)?
                    .expect("old value not found");
                let _new_mem_index_entry = self.disk_log.store(&ring_pos, None)?;
                Ok((ring_pos, old_dvvs))
            })
            .collect()
    }

    fn size(&self) -> usize {
        self.mem_index.size()
    }
}

// here is an article on the entire idea: https://arpitbhayani.me/blogs/bitcask

struct DiskLog {
    files: Vec<DiskLogFile>,
}

impl DiskLog {
    fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self> {
        Ok(Self {
            files: vec![DiskLogFile::new(data_dir, 0)?],
        })
    }
    fn from_disk<T: Into<PathBuf>>(data_dir: T, mem_index: &mut MemIndex) -> Result<Self> {
        let data_dir: PathBuf = data_dir.into();
        let mut files = std::fs::read_dir(&data_dir)?
            .filter_map(|path| {
                path.ok()
                    .map(|path| path.path())
                    .filter(|path| {
                        path.is_file() && path.extension() == Some(OsStr::new(DiskLogFile::EXT))
                    })
                    .and_then(|path| {
                        path.file_stem()
                            .and_then(|file_stem| file_stem.to_str())
                            .and_then(|file_stem| file_stem.parse::<FileId>().ok())
                            .map(|file_id| (file_id, path))
                    })
            })
            .map(|(file_id, path)| {
                DiskLogFile::open(file_id, path, mem_index)
                    .map(|disk_log_file| (file_id, disk_log_file))
            })
            .collect::<Result<Vec<(FileId, DiskLogFile)>>>()?;

        files.sort_by_key(|(file_id, _)| *file_id);

        if files.len() == 0 {
            trace!("No disk log files found, starting from scratch");
            return Self::new(data_dir);
        }

        let files = files
            .into_iter()
            .map(|(_, disk_log_file)| disk_log_file)
            .collect();

        Ok(Self { files })
    }
    fn current_file(&mut self) -> (&mut DiskLogFile, FileId) {
        // the last file is always open for appending
        let file_id = self.files.len() - 1;
        (self.files.last_mut().unwrap(), file_id)
    }
    fn get_file(&self, file_id: FileId) -> &DiskLogFile {
        self.files.get(file_id as usize).unwrap()
    }
    fn get(&self, mem_index_entry: &MemIndexEntry) -> Result<Option<DvvSet>> {
        let MemIndexEntry {
            dvvs_offset,
            dvvs_len,
            file_id,
        } = mem_index_entry;
        let disk_log_file = self.get_file(*file_id);

        let mut buffered_reader = BufReader::with_capacity(*dvvs_len as usize, &disk_log_file.file);
        buffered_reader.seek(SeekFrom::Start(*dvvs_offset))?;
        let dvvs: Option<DvvSet> =
            bincode::deserialize_from(&mut buffered_reader).with_context(|| {
                format!(
                    "Deserializing dvvs from file {} at position {}",
                    disk_log_file.path.display(),
                    buffered_reader.stream_position().unwrap()
                )
            })?;
        Ok(dvvs)
    }
    fn store(&mut self, key: &RingPos, value: Option<&DvvSet>) -> Result<MemIndexEntry> {
        let (disk_log_file, file_id) = self.current_file();
        let file = &mut disk_log_file.file;

        // FORMAT: [RingPos][Option<DvvSet>]
        let serialized_ring_pos = bincode::serialize(&key).unwrap();
        let serialized_ring_pos_len = serialized_ring_pos.len() as u64;
        let dvvs_offset = file.seek(SeekFrom::End(0))? + serialized_ring_pos_len;
        let serialized_dvvs = bincode::serialize(&value).unwrap();
        let serialized_dvvs_len = serialized_dvvs.len() as u64;

        file.write_all(&serialized_ring_pos)?;
        file.write_all(&serialized_dvvs)?;

        file.flush()?; // ensure persistency
        Ok(MemIndexEntry {
            file_id,
            dvvs_offset,
            dvvs_len: serialized_dvvs_len,
        })
    }
}

struct DiskLogFile {
    file_id: FileId,
    path: PathBuf,
    file: std::fs::File,
}

impl DiskLogFile {
    const EXT: &'static str = "log";

    fn new<T: Into<PathBuf>>(data_dir: T, file_id: FileId) -> Result<Self> {
        let mut path: PathBuf = data_dir.into();
        path.push(file_id.to_string());
        path.set_extension(Self::EXT);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            file_id,
            path,
            file,
        })
    }
    fn open(file_id: FileId, path: PathBuf, mem_index: &mut MemIndex) -> Result<Self> {
        // IMPROVE: only open last file for appending, others read only
        trace!("opening disk log file: {:?}", path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)?;
        let file = Self {
            file_id,
            path,
            file,
        };
        file.populate_mem_index(mem_index)?;
        Ok(file)
    }
    fn populate_mem_index(&self, mem_index: &mut MemIndex) -> Result<()> {
        let file_size = self.file.metadata()?.len();
        let mut buffered_reader = BufReader::new(&self.file);
        buffered_reader.seek(SeekFrom::Start(0))?;
        loop {
            if buffered_reader.stream_position()? >= file_size {
                break;
            }
            let ring_pos: RingPos = bincode::deserialize_from(&mut buffered_reader)?;
            let dvvs_offset = buffered_reader.stream_position()?;
            let dvvs: Option<DvvSet> = bincode::deserialize_from(&mut buffered_reader)?;
            let dvvs_len = buffered_reader.stream_position()? - dvvs_offset;

            if dvvs.is_none() {
                mem_index.delete(&ring_pos);
            } else {
                let mem_log_entry = MemIndexEntry {
                    file_id: self.file_id,
                    dvvs_offset,
                    dvvs_len,
                };
                mem_index.put(ring_pos, mem_log_entry);
            }
        }
        Ok(())
    }
}

type FileId = usize;
type ByteSize = u64;
type ByteOffset = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemIndexEntry {
    file_id: FileId,
    dvvs_offset: ByteOffset,
    dvvs_len: ByteSize,
}
// timestamp may be added later

struct MemIndex {
    map: BTreeMap<RingPos, MemIndexEntry>,
}

impl MemIndex {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
    fn get(&self, key: &RingPos) -> Option<&MemIndexEntry> {
        self.map.get(key)
    }
    fn put(&mut self, key: RingPos, entry: MemIndexEntry) -> Option<MemIndexEntry> {
        self.map.insert(key, entry)
    }
    fn delete(&mut self, key: &RingPos) -> Option<MemIndexEntry> {
        self.map.remove(key)
    }
    fn get_within_range(&self, range: &RingRange) -> Vec<(&RingPos, &MemIndexEntry)> {
        range
            .consecutive_ranges_iter()
            .fold(Vec::with_capacity(64), |mut acc, range| {
                acc.extend(
                    self.map
                        .range(range.from()..range.to())
                        .map(|(k, v)| (k, v)),
                );
                acc
            })
    }
    fn delete_within_range(&mut self, range: &RingRange) -> Vec<(RingPos, MemIndexEntry)> {
        range
            .consecutive_ranges_iter()
            .fold(Vec::with_capacity(64), |mut acc, range| {
                acc.extend(self.map.range(range.from()..range.to()).map(|(k, _v)| k));
                acc
            })
            .into_iter()
            .map(|k: RingPos| (k, self.map.remove(&k).unwrap()))
            .collect()
    }
    fn size(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn simple_put_get_del() -> Result<()> {
        let data_dir = PathBuf::from("./data/test");
        std::fs::create_dir_all(&data_dir)?;

        let mut log_index_storage = LogIndexStorage::new(&data_dir)?;

        let ring_pos = RingPos::from_str("key");
        let _actor = RingPos::from_str("actor");
        let dvvs = DvvSet::default();

        assert_eq!(log_index_storage.size(), 0);

        log_index_storage.store(&ring_pos, &dvvs)?;

        assert_eq!(log_index_storage.size(), 1);

        let get_res = log_index_storage.get(&ring_pos)?;
        assert_eq!(get_res.as_ref(), Some(&dvvs));

        let delete_res = log_index_storage.reset(&ring_pos)?;
        assert_eq!(
            delete_res.as_ref(),
            Some(&dvvs),
            "deletion should return the old value"
        );

        assert_eq!(
            log_index_storage.get(&ring_pos)?,
            None,
            "after deletion the value should be None"
        );
        assert_eq!(log_index_storage.size(), 0);

        let delete_res = log_index_storage.reset(&ring_pos)?;
        assert_eq!(delete_res.as_ref(), None);

        assert_eq!(log_index_storage.size(), 0);

        std::fs::remove_dir_all(&data_dir)?;
        Ok(())
    }

    #[test]
    #[serial]
    fn bulk_insertion() -> Result<()> {
        let data_dir = PathBuf::from("./data/test");

        std::fs::create_dir_all(&data_dir)?;
        let mut log_index_storage = LogIndexStorage::new(&data_dir)?;

        assert_eq!(0, log_index_storage.size());

        const AMOUNT_ENTRIES: i32 = 1000;

        let key_values = (0..AMOUNT_ENTRIES)
            .into_iter()
            .map(|i| {
                let key = RingPos::from_str(format!("key{:0>4}", i));
                let value = DvvSet::default();
                (key, value)
            })
            .collect::<Vec<_>>();

        for (key, value) in key_values.iter() {
            log_index_storage.store(key, value)?;
        }

        assert_eq!(AMOUNT_ENTRIES as usize, log_index_storage.size());

        for (key, value) in key_values.iter() {
            let retrieved = log_index_storage.get(key)?;
            assert_eq!(retrieved.as_ref(), Some(value));
        }

        std::fs::remove_dir_all(&data_dir)?;
        Ok(())
    }

    #[test]
    #[serial]
    fn interleaved_put_get() -> Result<()> {
        let data_dir = PathBuf::from("./data/test");

        std::fs::create_dir_all(&data_dir)?;
        let mut log_index_storage = LogIndexStorage::new(&data_dir)?;

        let key1 = RingPos::from_str("key1");
        let value1 = DvvSet::default();

        let key2 = RingPos::from_str("key2");
        let value2 = DvvSet::default();

        log_index_storage.store(&key1, &value1)?;

        assert_eq!(log_index_storage.get(&key1)?.as_ref(), Some(&value1));

        log_index_storage.store(&key2, &value2)?;

        assert_eq!(log_index_storage.get(&key1)?.as_ref(), Some(&value1));
        assert_eq!(log_index_storage.get(&key2)?.as_ref(), Some(&value2));

        std::fs::remove_dir_all(&data_dir)?;
        Ok(())
    }
}
