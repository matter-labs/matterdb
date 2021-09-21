use matterdb::access::{Access, AccessExt, FromAccess, RawAccess, RawAccessMut};
use matterdb::{
    inspect_database, BinaryKey, BinaryValue, Database, IndexAddress, IndexType, MapIndex,
    TemporaryDB,
};
use matterdb_derive::FromAccess;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Wrong type of index for this operations")]
    WrongType,
    #[error("Index not found")]
    IndexNotFound,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct StoredTransaction {
    test1: i64,
    test2: i32,
}

impl BinaryValue for StoredTransaction {
    fn to_bytes(&self) -> Vec<u8> {
        vec![]
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> anyhow::Result<Self> {
        Ok(Self { test1: 2, test2: 3 })
    }
}

#[derive(Debug, FromAccess)]
pub struct MempoolSchema<T: Access> {
    pub transactions: MapIndex<T::Base, String, StoredTransaction>,
}
impl<T: Access> MempoolSchema<T> {
    pub fn new(access: T) -> Self {
        Self::from_access(access, IndexAddress::from_root("mempool")).unwrap()
    }
}

pub struct CLIInterface<T: RawAccess> {
    indexes: HashMap<String, IndexType>,
    access: T,
}

impl<T: RawAccess> CLIInterface<T> {
    pub fn new(access: T, sub_prefix: Option<String>) -> CLIInterface<T> {
        let indexes = inspect_database(access.clone(), sub_prefix)
            .into_iter()
            .map(|(index, index_type)| (index, index_type))
            .collect();
        Self { indexes, access }
    }
    pub fn indexes(&self) -> &HashMap<String, IndexType> {
        &self.indexes
    }
    pub fn list<V: BinaryValue>(
        &self,
        index_name: String,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<V>, Error> {
        self.verify_index(&index_name, IndexType::List)?;
        Ok(self
            .access
            .get_list(index_name)
            .iter()
            .skip(offset)
            .take(limit)
            .collect())
    }

    pub fn get_from_list_by_index<V: BinaryValue>(
        &self,
        index_name: &str,
        index: u64,
    ) -> Result<Option<V>, Error> {
        self.verify_index(&index_name, IndexType::List)?;
        Ok(self.access.get_list(index_name).get(index))
    }

    // pub fn map<V: BinaryValue, K: BinaryKey>(
    //     &self,
    //     index_name: String,
    //     limit: usize,
    //     offset: usize,
    // ) -> Result<Vec<(K, V)>, Error> {
    //     self.verify_index(&index_name, IndexType::Map)?;
    //     Ok(self
    //         .access
    //         .get_map::<String, K, V>(index_name)
    //         .into_iter()
    //         .map(|(key, value)| (key, value))
    //         .collect())
    // }

    fn verify_index(&self, index_name: &str, index_type: IndexType) -> Result<(), Error> {
        let index = self.indexes.get(index_name).ok_or(Error::IndexNotFound)?;
        if *index != index_type {
            Err(Error::WrongType)
        } else {
            Ok(())
        }
    }
}

fn main() {
    println!("Hello, world!");
}

#[test]
fn list_cli() {
    let db = TemporaryDB::new();
    let fork = db.fork();
    let schema = MempoolSchema::new(&fork);
    // schema.transactions_get("Test".to_string());
    // // Create some unrelated indexes.
    // fork.get_list("ba").push(10_u8);
    // fork.get_list("ba").push(10_u8);
    // fork.get_list("ba").push(10_u8);
    // fork.get_list("ba").push(10_u8);
    // fork.get_list("baz").push("zopa".to_string());
    // fork.get_entry(("ba", "r")).set(0_u8);
    // fork.get_entry("bar_").set(0_u8);
    // fork.get_entry("fo").set(0_u8);
    // fork.get_entry(("fo", "oo")).set(0_u8);
    // fork.get_entry("foo1").set(0_u8);
    // fork.get_entry("test").set(0_u8);
    // fork.get_entry(("te", "st")).set(0_u8);
    // fork.get_entry("test_test").set(0_u8);
    // let cli = CLIInterface::new(&fork, None);
    // println!("{:?}", cli.list::<u8>("ba".to_string(), 10, 0).unwrap());
    // println!("{:?}", cli.list::<u8>("bar_".to_string(), 10, 0));
    // println!("{:?}", cli.list::<Vec<u8>>("baz".to_string(), 10, 0));
}
