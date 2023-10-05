#![allow(dead_code)]
use crate::causality_tracking::{DottedVersionVectorSet, VersionVector};
use crate::key::RingPos;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::string::FromUtf8Error;

/// Also: server id
pub type Actor = RingPos;
/// The DottedVersionVectorSet type without generics used across the key value store.
pub type DvvSet = DottedVersionVectorSet<Actor, Value>;
/// The VersionVector type without generics used across the key value store.
pub type VersionVec = VersionVector<Actor>;

#[derive(Debug, Clone)]
pub struct Siblings(Vec<Value>);

impl Display for Siblings {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let siblings = self
            .0
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
            .join(",");
        write!(f, "[{siblings}]")
    }
}

impl FromStr for Siblings {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let siblings = s
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| Value::from(s.to_string()))
            .collect::<Vec<Value>>();
        Ok(Self(siblings))
    }
}

impl From<&mut dyn Iterator<Item = Value>> for Siblings {
    fn from(siblings: &mut dyn Iterator<Item = Value>) -> Self {
        Self(siblings.collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Value {
    inner: String,
}

impl Value {
    pub fn new(value: String) -> Self {
        Self { inner: value }
    }
    /// Returns the length of the string in _bytes_.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self { inner: value }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self {
            inner: value.to_string(),
        }
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_bytes()
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = FromUtf8Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        String::from_utf8(value.to_vec()).map(|s| Self { inner: s })
    }
}

impl TryFrom<Vec<u8>> for Value {
    type Error = FromUtf8Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        String::from_utf8(value).map(|s| Self { inner: s })
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
