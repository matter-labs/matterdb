//! Shared code among all migration examples. The migration follows the following scenario:
//!
//! 1. We create and fill database with random data according to schema defined in the
//!    `migration::v1` module with the `create_initial_data` method.
//! 2. We perform migration from the `v1` schema to the `v2` schema
//!    with the help of the `migrate` function.
//!    The method transforms the data in the old schema to conform to the new schema.
//!    The old data is **not** removed at this stage; rather, it exists alongside
//!    the migrated data. This is useful in case the migration needs to be reverted for some reason.
//! 3. We complete the migration by calling `flush_migration`. This moves the migrated data
//!    to its intended place and removes the old data marked for removal.

#![allow(clippy::wildcard_imports)]

use std::sync::Arc;

use matterdb::{
    Database, Entry, Group, ListIndex, MapIndex, Snapshot, TemporaryDB,
    access::{Access, AccessExt, FromAccess, Prefixed},
    migration::{Migration, flush_migration},
};
use matterdb_derive::{BinaryValue, FromAccess};
use rand::{Rng, seq::IndexedRandom};

const USER_COUNT: usize = 10_000;

pub(crate) type PublicKey = u16;
pub(crate) type Hash = u32;

pub(crate) mod v1 {
    use super::*;

    #[derive(Debug, bincode::Encode, bincode::Decode, BinaryValue)]
    #[binary_value(codec = "bincode")]
    pub(crate) struct Wallet {
        pub public_key: PublicKey, // << removed in `v2`
        pub username: String,
        pub balance: u32,
    }

    #[derive(Debug, FromAccess)]
    pub(crate) struct Schema<T: Access> {
        pub ticker: Entry<T::Base, String>,
        pub divisibility: Entry<T::Base, u8>,
        pub wallets: MapIndex<T::Base, PublicKey, Wallet>,
        pub histories: Group<T, PublicKey, ListIndex<T::Base, Hash>>,
    }

    impl<T: Access> Schema<T> {
        pub(crate) fn new(access: T) -> Self {
            Self::from_root(access).unwrap()
        }

        pub(crate) fn print_wallets(&self) {
            for (public_key, wallet) in self.wallets.iter().take(10) {
                println!("Wallet[{public_key:?}] = {wallet:?}");
                println!(
                    "History = {:?}",
                    self.histories.get(&public_key).iter().collect::<Vec<_>>()
                );
            }
        }
    }
}

/// Creates initial DB with some random data.
fn create_initial_data() -> TemporaryDB {
    let db = TemporaryDB::new();
    let fork = db.fork();

    {
        const NAMES: &[&str] = &["Alice", "Bob", "Carol", "Dave", "Eve"];

        let mut schema = v1::Schema::new(Prefixed::new("test", &fork));
        schema.ticker.set("XNM".to_owned());
        schema.divisibility.set(8);

        let mut rng = rand::rng();
        for user_id in 0..USER_COUNT {
            let public_key = u16::try_from(user_id).unwrap();
            let username = (*NAMES.choose(&mut rng).unwrap()).to_string();
            let wallet = v1::Wallet {
                public_key,
                username,
                balance: rng.random_range(0..1_000),
            };
            schema.wallets.put(&public_key, wallet);

            let history_len = rng.random_range(0..10);
            schema.histories.get(&public_key).extend(0..history_len);
        }
    }

    fork.get_list("unrelated.list").extend(vec![1, 2, 3]);
    db.merge(fork.into_patch()).unwrap();
    db
}

pub(crate) mod v2 {
    use super::*;

    #[derive(Debug, bincode::Encode, bincode::Decode, BinaryValue)]
    #[binary_value(codec = "bincode")]
    pub(crate) struct Wallet {
        pub username: String,
        pub balance: u32,
        pub history_hash: Hash, // << new field
    }

    #[derive(Debug, bincode::Encode, bincode::Decode, BinaryValue)]
    #[binary_value(codec = "bincode")]
    pub(crate) struct Config {
        pub ticker: String,
        pub divisibility: u8,
    }

    #[derive(Debug, FromAccess)]
    pub(crate) struct Schema<T: Access> {
        pub config: Entry<T::Base, Config>,
        pub wallets: MapIndex<T::Base, PublicKey, Wallet>,
        pub histories: Group<T, PublicKey, ListIndex<T::Base, Hash>>,
    }

    impl<T: Access> Schema<T> {
        pub(crate) fn new(access: T) -> Self {
            Self::from_root(access).unwrap()
        }

        pub(crate) fn print_wallets(&self) {
            for (public_key, wallet) in self.wallets.iter().take(10) {
                println!("Wallet[{public_key:?}] = {wallet:?}");
                println!(
                    "History = {:?}",
                    self.histories.get(&public_key).iter().collect::<Vec<_>>()
                );
            }
        }
    }
}

/// Checks that we have old and new data in the storage after migration.
fn check_data_before_flush(snapshot: &dyn Snapshot) {
    let old_schema = v1::Schema::new(Prefixed::new("test", snapshot));
    assert_eq!(old_schema.ticker.get().unwrap(), "XNM");
    // The new data is present, too, in the unmerged form.
    let new_schema = v2::Schema::new(Migration::new("test", snapshot));
    assert_eq!(new_schema.config.get().unwrap().ticker, "XNM");
}

/// Checks that old data was replaced by new data in the storage.
fn check_data_after_flush(snapshot: &dyn Snapshot) {
    let new_schema = v2::Schema::new(Prefixed::new("test", snapshot));
    assert_eq!(new_schema.config.get().unwrap().divisibility, 8);
    assert!(!snapshot.get_entry::<_, u8>("test.divisibility").exists());
}

/// Performs common migration logic.
pub(crate) fn perform_migration<F>(migrate: F)
where
    F: FnOnce(Arc<dyn Database>),
{
    // Creating a temporary DB and filling it with some data.
    let db: Arc<dyn Database> = Arc::new(create_initial_data());

    let fork = db.fork();
    {
        // State before migration.
        let old_data = Prefixed::new("test", fork.readonly());
        let old_schema = v1::Schema::new(old_data.clone());
        println!("Before migration:");
        old_schema.print_wallets();
    }

    // Execute data migration logic.
    migrate(Arc::clone(&db));

    // At this point the old data and new data are still present in the storage,
    // but new data is in the unmerged form.

    // Check that DB contains old and new data.
    let snapshot = db.snapshot();
    check_data_before_flush(snapshot.as_ref());
    // Finalize the migration by calling `flush_migration`.
    let mut fork = db.fork();
    flush_migration(&mut fork, "test");

    // At this point the new indexes have replaced the old ones in the fork.
    // And indexes are aggregated in the default namespace.

    // Check that indexes are updated.
    let patch = fork.into_patch();
    check_data_after_flush(&patch);
    // When the patch is merged, the situation remains the same.
    db.merge(patch).unwrap();
    // Check that data was updated after merge.
    let snapshot = db.snapshot();
    check_data_after_flush(snapshot.as_ref());

    // Print DB state after migration is completed.
    let schema = v2::Schema::new(Prefixed::new("test", snapshot.as_ref()));
    println!("After migration:");
    schema.print_wallets();
}
