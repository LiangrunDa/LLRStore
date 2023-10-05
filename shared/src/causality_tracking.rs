use core::hash::Hash;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

/// An alias representing an actor's counter of a vector clock/version vector.
pub type Ctr = u64;

/// An implementation of a Dotted Version Vector Set (DVV-Set).
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct DottedVersionVectorSet<A: Hash + Eq, V> {
    entries: HashMap<A, (Ctr, Vec<V>)>,
}

impl<A: Hash + Eq, V> Default for DottedVersionVectorSet<A, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: Hash + Eq, V> DottedVersionVectorSet<A, V> {
    fn new() -> Self {
        DottedVersionVectorSet {
            entries: HashMap::new(),
        }
    }
}

impl<A: Hash + Eq + Clone, V: Clone> DottedVersionVectorSet<A, V> {
    /// Returns true if `self` is updated by `other` because `other` contains
    /// information which is new to `self`. Otherwise, returns false.
    pub fn sync(&mut self, other: DottedVersionVectorSet<A, V>) -> bool {
        let mut caused_update = false;
        for (id, (other_tip, other_values)) in other.entries.into_iter() {
            if let Some((tip, values)) = self.entries.get_mut(&id) {
                let (new_values, new_tip, changed) =
                    Self::merge(*tip, std::mem::take(values), other_tip, other_values);
                *tip = new_tip;
                *values = new_values;
                caused_update = caused_update || changed;
            } else {
                self.entries.insert(id, (other_tip, other_values));
                caused_update = true;
            }
        }
        caused_update
    }
    /// Returns a triple of (values, tip, changed).
    /// Changed is true if the values output vector is different from the
    /// `values` input vector, otherwise false.
    fn merge(
        tip: Ctr,
        mut values: Vec<V>,
        other_tip: Ctr,
        mut other_values: Vec<V>,
    ) -> (Vec<V>, Ctr, bool) {
        if tip >= other_tip {
            let new_len = (tip
                .saturating_sub(other_tip)
                .saturating_add(other_values.len() as Ctr)) as usize;
            let changed = Self::retain_back(&mut values, new_len);
            (values, tip, changed)
        } else {
            let new_len = (other_tip
                .saturating_sub(tip)
                .saturating_add(values.len() as Ctr)) as usize;
            let _changed = Self::retain_back(&mut other_values, new_len);
            (other_values, other_tip, true)
        }
    }
    /// Is the equivalent to the first() fn from the paper.
    /// It keeps the last `len` elements of the vector.
    /// Returns true if the vector was changed, otherwise false.
    fn retain_back(values: &mut Vec<V>, len: usize) -> bool {
        if len >= values.len() {
            false
        } else {
            if len == 0 {
                values.clear();
            } else {
                values.drain(..len);
            }
            true
        }
    }
    pub fn discard(&mut self, context: &VersionVector<A>) {
        for (id, (tip, values)) in self.entries.iter_mut() {
            let new_len: usize = (tip.saturating_sub(context.get(id))) as usize;
            Self::retain_back(values, new_len);
        }
    }
    pub fn event(&mut self, this_actor: &A, context: &VersionVector<A>, value: V) {
        for (id, (tip, _values)) in self.entries.iter_mut() {
            if id != this_actor {
                *tip = std::cmp::max(*tip, context.get(id));
            }
        }
        if let Some((tip, values)) = self.entries.get_mut(&this_actor) {
            *tip += 1;
            values.push(value);
        } else {
            self.entries.insert(this_actor.clone(), (1, vec![value]));
        }
    }
    /// Convenience method that basically serves a put and returns the
    /// tag of the state along with the current values of the register
    pub fn handle_put(
        &mut self,
        this_actor: &A,
        context: &VersionVector<A>,
        value: V,
    ) -> (VersionVector<A>, impl Iterator<Item = &V>) {
        self.discard(context);
        self.event(this_actor, context, value);
        (self.join(), self.values())
    }
    pub fn handle_delete(
        &mut self,
        _this_actor: &A,
        context: &VersionVector<A>,
    ) -> (VersionVector<A>, impl Iterator<Item = &V>) {
        self.discard(context);
        (self.join(), self.values())
    }
}

impl<A: Hash + Eq + Clone, V> DottedVersionVectorSet<A, V> {
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries
            .values()
            .flat_map(|(_tip, values)| values.iter())
    }
    pub fn into_values(self) -> impl Iterator<Item = V> {
        self.entries
            .into_iter()
            .flat_map(|(_id, (_tip, values))| values.into_iter())
    }
    pub fn empty_register(&self) -> bool {
        self.values().next().is_none()
    }
    pub fn values_with_dot(&self) -> impl Iterator<Item = ((A, Ctr), &V)> {
        self.entries.iter().flat_map(|(id, (tip, values))| {
            let tip = *tip as usize;
            let total_values = values.len();
            values.iter().enumerate().map(move |(index, value)| {
                let dot_tip = (tip - (total_values - 1) + index) as Ctr;
                ((id.clone(), dot_tip), value)
            })
        })
    }
    pub fn join(&self) -> VersionVector<A> {
        self.entries
            .iter()
            .map(|(id, (tip, _values))| (id.clone(), *tip))
            .collect()
    }
}

impl<A: Display + Hash + Eq + Clone, V: Display> Display for DottedVersionVectorSet<A, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let values = self
            .values_with_dot()
            .map(|((id, tip), value)| format!("{value} ({id}, {tip})"))
            .collect::<Vec<_>>()
            .join(", ");
        let join = self.join();
        write!(f, "{values} {join}")
    }
}

impl<A: Hash + Eq, V> FromIterator<(A, (Ctr, Vec<V>))> for DottedVersionVectorSet<A, V> {
    fn from_iter<T: IntoIterator<Item = (A, (Ctr, Vec<V>))>>(iter: T) -> Self {
        Self {
            entries: iter.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionVector<A: Hash + Eq> {
    entries: HashMap<A, Ctr>,
}

impl<A: Hash + Eq> Default for VersionVector<A> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<A: Hash + Eq> VersionVector<A> {
    pub fn empty() -> Self {
        Self::default()
    }
}

impl<A: Hash + Eq> VersionVector<A> {
    pub fn increment(&mut self, actor: A) {
        self.entries
            .entry(actor)
            .and_modify(|ctr| *ctr += 1)
            .or_insert(1);
    }
    pub fn get(&self, actor: &A) -> Ctr {
        *self.entries.get(actor).unwrap_or(&0)
    }
}

impl<A: Hash + Eq> FromIterator<(A, Ctr)> for VersionVector<A> {
    fn from_iter<T: IntoIterator<Item = (A, Ctr)>>(iter: T) -> Self {
        let mut vv = VersionVector::default();
        for (actor, ctr) in iter {
            vv.entries.insert(actor, ctr);
        }
        vv
    }
}

impl<A: Hash + Eq> PartialEq for VersionVector<A> {
    fn eq(&self, other: &Self) -> bool {
        if self.entries.len() != other.entries.len() {
            return false;
        }
        self.entries.iter().all(|(id, ctr)| {
            if let Some(other_ctr) = other.entries.get(id) {
                ctr == other_ctr
            } else {
                false
            }
        })
    }
}

impl<A: Hash + Eq> PartialOrd for VersionVector<A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let mut self_is_bigger = false;
        let mut other_is_bigger = false;

        let common_actors = self
            .entries
            .keys()
            .filter(|id| other.entries.get(id).is_some())
            .collect::<Vec<_>>();

        if self.entries.len() > common_actors.len() {
            // self has actors (clocks) that other does not have
            self_is_bigger = true;
        }
        if other.entries.len() > common_actors.len() {
            // other has actors (clocks) that self does not have
            other_is_bigger = true;
        }

        // compare the common actors
        for actor in common_actors {
            if self_is_bigger && other_is_bigger {
                break;
            }
            let self_ctr = self.entries.get(actor).unwrap();
            let other_ctr = other.entries.get(actor).unwrap();
            if self_ctr > other_ctr {
                self_is_bigger = true;
            } else if other_ctr > self_ctr {
                other_is_bigger = true;
            }
        }

        match (self_is_bigger, other_is_bigger) {
            // concurrent
            (true, true) => None,
            // self is a (strict) descendant of other
            (true, false) => Some(Ordering::Greater),
            // other is a (strict) descendant of self
            (false, true) => Some(Ordering::Less),
            // self and other are equal
            (false, false) => Some(Ordering::Equal),
        }
    }
}

impl<A: Display + Hash + Eq> Display for VersionVector<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries = self
            .entries
            .iter()
            .map(|(id, ctr)| format!("{{{}:{}}}", id, ctr))
            .collect::<Vec<_>>()
            .join(",");
        write!(f, "[{entries}]")
    }
}

impl<A: Hash + Eq + FromStr> FromStr for VersionVector<A> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let to_anyhow_err = || anyhow::anyhow!("Could not parse '{}' as version vector", s);
        let entries = s
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let entry = entry.trim().trim_start_matches('{').trim_end_matches('}');
                let mut entry = entry.split(':');
                let id: A = entry
                    .next()
                    .and_then(|id| id.trim().parse::<A>().ok())
                    .ok_or_else(to_anyhow_err)?;
                let ctr: Ctr = entry
                    .next()
                    .and_then(|id| id.trim().parse::<Ctr>().ok())
                    .ok_or_else(to_anyhow_err)?;
                Ok((id, ctr))
            })
            .collect::<Result<HashMap<A, Ctr>, Self::Err>>()?;
        Ok(VersionVector { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashSet,
        io::{Cursor, Read},
    };

    #[test]
    fn test_version_vector_eq() {
        let vv1 = VersionVector::from_iter(vec![("a", 1), ("b", 2)]);
        assert_eq!(vv1, vv1);

        let vv2 = VersionVector::from_iter(vec![("a", 1), ("b", 2), ("c", 3)]);
        assert_eq!(vv2, vv2);

        assert_ne!(vv2, vv1);
        assert_ne!(vv1, vv2);
    }

    #[test]
    fn test_version_vector_cmp() {
        let vv1 = VersionVector::from_iter(vec![("a", 1), ("b", 2)]);
        let vv2 = VersionVector::from_iter(vec![("a", 1), ("b", 2), ("c", 3)]);
        assert!(vv1 <= vv1);
        assert!(!(vv1 < vv1));
        assert!(vv1 <= vv2);
        assert!(vv2 > vv1);

        // test concurrent case
        let vv1 = VersionVector::from_iter(vec![("a", 1), ("b", 3), ("c", 2)]);
        let vv2 = VersionVector::from_iter(vec![("a", 1), ("b", 2), ("c", 3)]);
        assert_eq!(vv1.partial_cmp(&vv2), None);
    }

    #[test]
    fn test_version_vector_to_from_string() {
        let vv = VersionVector::from_iter(
            vec![("a", 1), ("b", 2), ("c", 3)]
                .into_iter()
                .map(|(id, ctr)| (id.to_string(), ctr)),
        );

        let serialized = vv.to_string();
        let deserialized = serialized.parse::<VersionVector<String>>().unwrap();

        assert_eq!(deserialized, vv);

        let vv = VersionVector::<String>::empty();
        let serialized = vv.to_string();
        let deserialized = serialized.parse::<VersionVector<String>>().unwrap();

        assert_eq!(deserialized, vv);
    }

    #[test]
    fn test_no_sibling_explosion() {
        // testcase visualization here:
        // https://github.com/russelldb/russelldb.github.io/blob/master/no-sib-ex.png
        let mut dvvs: DottedVersionVectorSet<&str, String> = DottedVersionVectorSet::default();

        let server_id: &'static str = "a";

        let (client_y_ctx, values) =
            dvvs.handle_put(&server_id, &VersionVector::empty(), "Bob".to_string());
        assert!(client_y_ctx == VersionVector::from_iter(vec![("a", 1)]));
        assert!(values.collect::<Vec<_>>() == vec!["Bob"]);

        let (client_x_ctx, values) =
            dvvs.handle_put(&server_id, &VersionVector::empty(), "Sue".to_string());
        assert!(client_x_ctx == VersionVector::from_iter(vec![("a", 2)]));
        assert!(values.collect::<Vec<_>>() == vec!["Bob", "Sue"]);

        let (client_y_ctx, values) = dvvs.handle_put(&server_id, &client_y_ctx, "Rita".to_string());
        assert!(client_y_ctx == VersionVector::from_iter(vec![("a", 3)]));
        assert!(values.collect::<Vec<_>>() == vec!["Sue", "Rita"]);

        let (client_x_ctx, values) =
            dvvs.handle_put(&server_id, &client_x_ctx, "Michelle".to_string());
        assert!(client_x_ctx == VersionVector::from_iter(vec![("a", 4)]));
        assert!(values.collect::<Vec<_>>() == vec!["Rita", "Michelle"]);
    }

    #[test]
    fn test_dvvs_add() {
        let mut dvvs: DottedVersionVectorSet<&str, String> = DottedVersionVectorSet::default();

        let server_id: &'static str = "a";

        let (client_ctx, values) =
            dvvs.handle_put(&server_id, &VersionVector::empty(), "Bob".to_string());
        assert!(client_ctx == VersionVector::from_iter(vec![("a", 1)]));
        assert!(values.collect::<Vec<_>>() == vec!["Bob"]);

        let (client_ctx, values) = dvvs.handle_put(&server_id, &client_ctx, "Alice".to_string());
        assert!(client_ctx == VersionVector::from_iter(vec![("a", 2)]));
        let values = values.collect::<Vec<_>>();
        println!("{values:?}");
        assert!(values == vec!["Alice"]);
    }

    #[test]
    fn test_dvvs_delete() {
        let mut dvvs_a = DottedVersionVectorSet::from_iter(vec![
            (
                "a",
                (
                    5,
                    vec!["Jesus", "Maria", "Josef"]
                        .into_iter()
                        .map(String::from)
                        .collect(),
                ),
            ),
            ("b", (2, vec![String::from("James")])),
        ]);

        let server_id: &'static str = "a";

        let (client_x_ctx, values) = dvvs_a.handle_delete(&server_id, &VersionVector::empty());

        assert_eq!(
            client_x_ctx,
            VersionVector::from_iter(vec![("a", 5), ("b", 2)])
        );
        let values = values.cloned().collect::<HashSet<_>>();
        assert!(
            values
                == HashSet::from_iter(
                    vec!["Jesus", "Maria", "Josef", "James"]
                        .into_iter()
                        .map(|x| x.to_string())
                )
        );

        let (client_x_ctx, values) = dvvs_a.handle_delete(&server_id, &client_x_ctx);

        assert_eq!(
            client_x_ctx,
            VersionVector::from_iter(vec![("a", 5), ("b", 2)])
        );
        let values = values.cloned().collect::<HashSet<_>>();
        println!("values: {:?}", values);
        assert!(values == HashSet::new());
    }

    #[test]
    fn test_dvvs_sync_complex() {
        let mut dvvs_a = DottedVersionVectorSet::from_iter(vec![
            ("a", (5, vec!["Jesus", "Maria", "Josef"])),
            ("b", (2, vec!["James"])),
            ("c", (3, vec!["Carsten"])),
        ]);

        let mut dvvs_b = DottedVersionVectorSet::from_iter(vec![
            ("a", (3, vec!["Jerun"])),
            ("b", (3, vec!["Peter"])),
            ("d", (4, vec!["Michelle"])),
        ]);

        assert!(
            dvvs_a.sync(dvvs_b.clone()),
            "B contained new information for A"
        );
        assert!(
            dvvs_b.sync(dvvs_a.clone()),
            "A contained new information for B"
        );

        assert_eq!(dvvs_a, dvvs_b);
        assert_eq!(
            dvvs_a,
            DottedVersionVectorSet::from_iter(vec![
                ("a", (5, vec!["Jesus", "Maria", "Josef"])),
                ("b", (3, vec!["Peter"])),
                ("c", (3, vec!["Carsten"])),
                ("d", (4, vec!["Michelle"])),
            ])
        );
    }

    #[test]
    fn test_dvvs_sync_basic() {
        let actor_a = "a";
        let mut dvvs_a = DottedVersionVectorSet::<&str, &str>::new();
        dvvs_a.event(&actor_a, &VersionVector::empty(), "Katrin");

        let actor_b = "b";
        let mut dvvs_b = DottedVersionVectorSet::<&str, &str>::new();
        dvvs_b.event(&actor_b, &VersionVector::empty(), "Peter");

        assert!(
            dvvs_a.sync(dvvs_b.clone()),
            "B contained new information for A"
        );
        assert!(
            dvvs_b.sync(dvvs_a.clone()),
            "A contained new information for B"
        );

        assert_eq!(dvvs_a, dvvs_b);
        assert_eq!(
            dvvs_a,
            DottedVersionVectorSet::from_iter(vec![
                ("a", (1, vec!["Katrin"])),
                ("b", (1, vec!["Peter"])),
            ])
        );
    }

    #[test]
    fn test_dvvs_sync_changed_indicator() {
        let mut dvvs_a = DottedVersionVectorSet::from_iter(vec![
            ("a", (5, vec!["Jesus", "Maria", "Josef"])),
            ("b", (2, vec!["James"])),
            ("c", (3, vec!["Carsten"])),
        ]);

        let mut dvvs_b = DottedVersionVectorSet::from_iter(vec![
            ("a", (3, vec!["Jerun"])),
            ("b", (2, vec!["James"])),
            ("c", (2, vec!["Michelle"])),
        ]);

        assert_eq!(
            dvvs_a.sync(dvvs_b.clone()),
            false,
            "B contained *no* new information for A"
        );
        assert!(
            dvvs_b.sync(dvvs_a.clone()),
            "A contained new information for B"
        );

        assert_eq!(dvvs_a, dvvs_b);
    }

    #[test]
    fn test_serde() {
        let dvvs = DottedVersionVectorSet::from_iter(vec![
            ("a", (5, vec!["Jesus", "Maria", "Josef"])),
            ("b", (2, vec!["James"])),
            ("c", (3, vec!["Carsten"])),
        ]);

        let serialized = bincode::serialize(&dvvs).unwrap();

        let deserialized: DottedVersionVectorSet<&str, &str> =
            bincode::deserialize(&serialized).unwrap();

        assert_eq!(dvvs, deserialized);
    }

    #[test]
    fn test_dvvs_values_iter() {
        let mut dvvs = DottedVersionVectorSet::<String, String>::default();
        let actor_id = "a".to_string();
        let (_client_ctx, values) =
            dvvs.handle_put(&actor_id, &VersionVector::empty(), "Michelle".to_string());
        let _values = Vec::from_iter(values);

        let mut serialized = bincode::serialize(&dvvs).unwrap();

        // to test if length is required for deserialization ...
        let additional_bytes_after = vec![8u8, 2u8, 4u8];
        serialized.extend_from_slice(&additional_bytes_after);

        // ... even if wrapped in a cursor to deserialize from a Reader...
        let mut cursor = Cursor::new(serialized);
        let deserialized: DottedVersionVectorSet<String, String> =
            bincode::deserialize_from(&mut cursor).unwrap();

        // ... turns out no!
        let mut rest = Vec::new();
        cursor.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, additional_bytes_after);

        assert_eq!(dvvs, deserialized);
    }
}
