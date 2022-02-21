//! Property testing for persistent iterators. The test checks that persistent iterators do not
//! skip or duplicate items, and that multiple iterators over the same collection are independent.

use proptest::{
    collection::vec, num, prop_assert_eq, prop_oneof, proptest, sample, strategy,
    strategy::Strategy, test_runner::TestCaseResult,
};
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

use matterdb::inspect_database;
use matterdb::{
    access::CopyAccessExt,
    migration::{rollback_migration, PersistentIter, PersistentKeys, Scratchpad},
    Database, Fork, IndexAddress, IndexType, TemporaryDB,
};

const ACTIONS_MAX_LEN: usize = 50;

#[derive(Debug, Clone, Copy)]
struct Collection {
    name: &'static str,
    prefix: Option<u32>,
    ty: IndexType,
}

impl Collection {
    const fn new(name: &'static str, prefix: Option<u32>, ty: IndexType) -> Self {
        Self { name, prefix, ty }
    }

    fn get_address(self) -> IndexAddress {
        let mut addr = IndexAddress::from_root(self.name);
        if let Some(prefix) = self.prefix {
            addr = addr.append_key(&prefix);
        }
        addr
    }

    fn fill(self, fork: &Fork, rng: &mut impl Rng) {
        let addr = self.get_address();
        let item_count = rng.gen_range(25..100);
        match self.ty {
            IndexType::List => {
                let mut list = fork.get_list(addr);
                list.extend((0..item_count).map(|_| rng.gen::<u64>()));
            }
            IndexType::SparseList => {
                let mut list = fork.get_sparse_list(addr);
                for _ in 0..item_count {
                    let index = rng.gen::<u64>() % 256;
                    let value = rng.gen::<u64>();
                    list.set(index, value);
                }
            }

            IndexType::Map => {
                let mut map = fork.get_map(addr);
                for _ in 0..item_count {
                    let key = rng.gen::<u64>() & 0xffff;
                    let value = rng.gen::<u64>();
                    map.put(&key, value);
                }
            }

            IndexType::KeySet => {
                let mut set = fork.get_key_set(addr);
                for _ in 0..item_count {
                    set.insert(&rng.gen::<u64>());
                }
            }

            _ => unreachable!(),
        }
    }
}

const COLLECTIONS: &[Collection] = &[
    Collection::new("list", None, IndexType::List),
    Collection::new("list", Some(1), IndexType::List),
    Collection::new("sparse_list", None, IndexType::SparseList),
    Collection::new("list", Some(3), IndexType::SparseList),
    Collection::new("map", None, IndexType::Map),
    Collection::new("map", Some(1), IndexType::Map),
    Collection::new("key_set", None, IndexType::KeySet),
    Collection::new("set", Some(1), IndexType::KeySet),
];

#[derive(Debug, Clone)]
enum Action {
    CreateIter(Collection),
    AdvanceIter {
        index: usize, // the real index will be taken modulo the number of iterators.
        amount: usize,
    },
    FlushFork,
    MergeFork,
}

fn generate_action(collections: &'static [Collection]) -> impl Strategy<Value = Action> {
    prop_oneof![
        4 => sample::select(collections).prop_map(Action::CreateIter),
        4 => (num::usize::ANY, 1_usize..10).prop_map(|(index, amount)| Action::AdvanceIter {
            index,
            amount,
        }),
        1 => strategy::Just(Action::FlushFork),
        1 => strategy::Just(Action::MergeFork),
    ]
}

// Since collection contents is not the subject of the test, we do not include into `Action`s.
// Instead, we use an RNG to fill each of predefined collections with 25-100 pseudo-random elements.
fn fill_collections(db: &TemporaryDB) {
    const RNG_SEED: [u8; 16] = *b"_seed_seed_seed_";

    let fork = db.fork();
    let mut rng = XorShiftRng::from_seed(RNG_SEED);
    for &collection in COLLECTIONS {
        collection.fill(&fork, &mut rng);
    }
    db.merge(fork.into_patch()).unwrap();
}

fn clear_scratchpad(db: &TemporaryDB) {
    let mut fork = db.fork();
    rollback_migration(&mut fork, "iters");
    db.merge(fork.into_patch()).unwrap();
}

#[derive(Debug)]
struct IterState {
    name: String,
    collection: Collection,
    items: Vec<u64>,
    position: usize,
}

impl IterState {
    fn advance(&mut self, fork: &Fork, amount: usize) {
        self.position += amount;
        let addr = self.collection.get_address();
        let scratchpad = Scratchpad::new("iters", fork);

        match self.collection.ty {
            IndexType::List => {
                let list = fork.get_list::<_, u64>(addr);
                let iter = PersistentIter::new(&scratchpad, &self.name, &list);
                self.items.extend(iter.map(|(_, value)| value).take(amount));
            }
            IndexType::SparseList => {
                let list = fork.get_sparse_list::<_, u64>(addr);
                let iter = PersistentIter::new(&scratchpad, &self.name, &list);
                self.items.extend(iter.map(|(_, value)| value).take(amount));
            }

            IndexType::Map => {
                let map = fork.get_map::<_, u64, u64>(addr);
                let iter = PersistentIter::new(&scratchpad, &self.name, &map);
                self.items.extend(iter.map(|(_, value)| value).take(amount));
            }

            IndexType::KeySet => {
                let set = fork.get_key_set::<_, u64>(addr);
                let iter = PersistentKeys::new(&scratchpad, &self.name, &set);
                self.items.extend(iter.take(amount));
            }

            _ => unreachable!(),
        }
    }

    fn check(&self, fork: &Fork) -> TestCaseResult {
        let addr = self.collection.get_address();
        let expected_items: Vec<_> = match self.collection.ty {
            IndexType::List => {
                let list = fork.get_list::<_, u64>(addr);
                list.iter().take(self.position).collect()
            }
            IndexType::SparseList => {
                let list = fork.get_sparse_list::<_, u64>(addr);
                list.values().take(self.position).collect()
            }

            IndexType::Map => {
                let map = fork.get_map::<_, u64, u64>(addr);
                map.values().take(self.position).collect()
            }

            IndexType::KeySet => {
                let set = fork.get_key_set::<_, u64>(addr);
                set.iter().take(self.position).collect()
            }

            _ => unreachable!(),
        };
        prop_assert_eq!(&expected_items, &self.items);

        Ok(())
    }
}

fn apply_actions(db: &TemporaryDB, actions: Vec<Action>) -> TestCaseResult {
    let mut fork = db.fork();
    let mut iters = vec![];

    for action in actions {
        match action {
            Action::CreateIter(collection) => {
                iters.push(IterState {
                    name: format!("iter{}", iters.len()),
                    collection,
                    items: vec![],
                    position: 0,
                });
            }
            Action::AdvanceIter { index, amount } => {
                if iters.is_empty() {
                    continue;
                }
                let len = iters.len();
                let iter = iters.get_mut(index % len).unwrap();
                iter.advance(&fork, amount);
                iter.check(&fork)?;
            }
            Action::FlushFork => fork.flush(),
            Action::MergeFork => {
                db.merge(fork.into_patch()).unwrap();
                fork = db.fork();
            }
        }
    }

    for iter in &iters {
        iter.check(&fork)?;
    }
    Ok(())
}

#[test]
fn persistent_iters() {
    let db = TemporaryDB::new();
    fill_collections(&db);

    proptest!(|(actions in vec(generate_action(COLLECTIONS), 1..ACTIONS_MAX_LEN))| {
        apply_actions(&db, actions)?;
        clear_scratchpad(&db);
    });
}

#[test]
fn persistent_iters_over_single_collection() {
    let db = TemporaryDB::new();
    fill_collections(&db);

    proptest!(|(actions in vec(generate_action(&COLLECTIONS[0..1]), 1..ACTIONS_MAX_LEN))| {
        apply_actions(&db, actions)?;
        clear_scratchpad(&db);
    });
}
#[test]
fn index_pool() {
    let db = TemporaryDB::new();
    fill_collections(&db);
    let data = inspect_database(&db.fork(), None);
    let expected_result = vec![
        ("key_set".to_string(), IndexType::KeySet),
        ("list".to_string(), IndexType::List),
        ("list\u{0}\u{0}\u{0}\u{0}\u{1}".to_string(), IndexType::List),
        ("list\u{0}\u{0}\u{0}\u{0}\u{3}".to_string(), IndexType::SparseList),
        ("map".to_string(), IndexType::Map),
        ("map\u{0}\u{0}\u{0}\u{0}\u{1}".to_string(), IndexType::Map),
        ("set\u{0}\u{0}\u{0}\u{0}\u{1}".to_string(), IndexType::KeySet),
        ("sparse_list".to_string(), IndexType::SparseList)
    ];
    assert_eq!(data, expected_result);
}
