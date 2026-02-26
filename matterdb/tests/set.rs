// cspell:ignore oneof

//! Property testing for key set index and value set index as a rust collection.

use std::{collections::HashSet, hash::Hash};

use matterdb::{Fork, KeySetIndex, TemporaryDB, access::AccessExt};
use modifier::Modifier;
use proptest::{
    collection::vec, prop_assert, prop_oneof, proptest, strategy, strategy::Strategy,
    test_runner::TestCaseResult,
};

mod common;

use crate::common::{ACTIONS_MAX_LEN, AsForkAction, ForkAction, FromFork, compare_collections};

#[derive(Debug, Clone)]
enum SetAction<V> {
    // Should be applied to a small subset of values (like modulo 8 for int).
    Put(V),
    // Should be applied to a small subset of values (like modulo 8 for int).
    Remove(V),
    Clear,
    MergeFork,
}

impl<V> AsForkAction for SetAction<V> {
    fn as_fork_action(&self) -> Option<ForkAction> {
        match self {
            SetAction::MergeFork => Some(ForkAction::Merge),
            _ => None,
        }
    }
}

fn generate_action() -> impl Strategy<Value = SetAction<u8>> {
    prop_oneof![
        (0..8u8).prop_map(SetAction::Put),
        (0..8u8).prop_map(SetAction::Remove),
        strategy::Just(SetAction::Clear),
        strategy::Just(SetAction::MergeFork),
    ]
}

impl<V> Modifier<HashSet<V>> for SetAction<V>
where
    V: Eq + Hash,
{
    fn modify(self, set: &mut HashSet<V>) {
        match self {
            SetAction::Put(v) => {
                set.insert(v);
            }
            SetAction::Remove(v) => {
                set.remove(&v);
            }
            SetAction::Clear => set.clear(),
            SetAction::MergeFork => unreachable!(),
        }
    }
}

impl Modifier<KeySetIndex<&Fork, u8>> for SetAction<u8> {
    fn modify(self, set: &mut KeySetIndex<&Fork, u8>) {
        match self {
            SetAction::Put(k) => {
                set.insert(&k);
            }
            SetAction::Remove(k) => {
                set.remove(&k);
            }
            SetAction::Clear => {
                set.clear();
            }
            SetAction::MergeFork => unreachable!(),
        }
    }
}

struct KeySetTest;

impl FromFork for KeySetTest {
    type Index<'a> = KeySetIndex<&'a Fork, u8>;

    fn from_fork(fork: &Fork) -> Self::Index<'_> {
        fork.get_key_set("test")
    }

    fn clear(index: &mut Self::Index<'_>) {
        index.clear();
    }
}

fn compare_key_set(set: &KeySetIndex<&Fork, u8>, ref_set: &HashSet<u8>) -> TestCaseResult {
    for k in ref_set {
        prop_assert!(set.contains(k));
    }
    for k in set {
        prop_assert!(ref_set.contains(&k));
    }
    Ok(())
}

#[test]
fn compare_key_set_to_hash_set() {
    let db = TemporaryDB::new();
    proptest!(|(ref actions in vec(generate_action(), 1..ACTIONS_MAX_LEN))| {
        compare_collections::<KeySetTest, _, _>(&db, actions, compare_key_set)?;
    });
}
