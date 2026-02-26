//! Extension traits to simplify index instantiation.

use crate::{
    BinaryKey, BinaryValue, Entry, Fork, Group, IndexAddress, KeySetIndex, ListIndex, MapIndex,
    Snapshot, SparseListIndex,
    access::{Access, FromAccess, RawAccess},
    views::IndexType,
};

/// Extension trait allowing for easy access to indexes from any type implementing
/// [`Access`].
///
/// # Implementation details
///
/// This trait is essentially a thin wrapper around [`FromAccess`]. Where [`FromAccess`] returns
/// an access error, the methods of this trait will `unwrap()` the error and panic.
pub trait AccessExt {
    /// Shortcut for [`Self::Ref`]`::Base`, similar to `Item` in `IntoIterator`. Allows to express
    /// type boundaries more concisely.
    type Base<'a>: RawAccess
    where
        Self: 'a;

    /// [`Access`] that this extension is based on: either `Self` or `&'a Self`.
    type Ref<'a>: Access<Base = Self::Base<'a>>
    where
        Self: 'a;

    fn get_ref(&self) -> Self::Ref<'_>;

    /// Returns a group of indexes. All indexes in the group have the same type.
    /// Indexes are initialized lazily; i.e., no initialization is performed when the group
    /// is created.
    ///
    /// Note that unlike other methods, this one requires address to be a string.
    /// This is to prevent collisions among groups.
    fn get_group<'s, K, I>(&'s self, name: impl Into<String>) -> Group<Self::Ref<'s>, K, I>
    where
        K: BinaryKey + ?Sized,
        I: FromAccess<Self::Ref<'s>>,
    {
        Group::from_access(self.get_ref(), IndexAddress::from_root(name))
            .unwrap_or_else(|e| panic!("MerkleDB error: {e}"))
    }

    /// Gets an entry index with the specified address.
    ///
    /// # Panics
    ///
    /// If the index exists, but is not an entry.
    fn get_entry<I, V>(&self, addr: I) -> Entry<Self::Base<'_>, V>
    where
        I: Into<IndexAddress>,
        V: BinaryValue,
    {
        Entry::from_access(self.get_ref(), addr.into())
            .unwrap_or_else(|e| panic!("MerkleDB error: {e}"))
    }

    /// Gets a list index with the specified address.
    ///
    /// # Panics
    ///
    /// If the index exists, but is not a list.
    fn get_list<I, V>(&self, addr: I) -> ListIndex<Self::Base<'_>, V>
    where
        I: Into<IndexAddress>,
        V: BinaryValue,
    {
        ListIndex::from_access(self.get_ref(), addr.into())
            .unwrap_or_else(|e| panic!("MerkleDB error: {e}"))
    }

    /// Gets a map index with the specified address.
    ///
    /// # Panics
    ///
    /// If the index exists, but is not a map.
    fn get_map<I, K, V>(&self, addr: I) -> MapIndex<Self::Base<'_>, K, V>
    where
        I: Into<IndexAddress>,
        K: BinaryKey + ?Sized,
        V: BinaryValue,
    {
        MapIndex::from_access(self.get_ref(), addr.into())
            .unwrap_or_else(|e| panic!("MerkleDB error: {e}"))
    }

    /// Gets a sparse list index with the specified address.
    ///
    /// # Panics
    ///
    /// If the index exists, but is not a sparse list.
    fn get_sparse_list<I, V>(&self, addr: I) -> SparseListIndex<Self::Base<'_>, V>
    where
        I: Into<IndexAddress>,
        V: BinaryValue,
    {
        SparseListIndex::from_access(self.get_ref(), addr.into())
            .unwrap_or_else(|e| panic!("MerkleDB error: {e}"))
    }

    /// Gets a key set index with the specified address.
    ///
    /// # Panics
    ///
    /// If the index exists, but is not a key set.
    fn get_key_set<I, K>(&self, addr: I) -> KeySetIndex<Self::Base<'_>, K>
    where
        I: Into<IndexAddress>,
        K: BinaryKey + ?Sized,
    {
        KeySetIndex::from_access(self.get_ref(), addr.into())
            .unwrap_or_else(|e| panic!("MerkleDB error: {e}"))
    }

    /// Gets index type at the specified address, or `None` if there is no index.
    fn index_type<I>(&self, addr: I) -> Option<IndexType>
    where
        I: Into<IndexAddress>,
    {
        self.get_ref()
            .get_index_metadata(addr.into())
            .unwrap_or_else(|e| panic!("MerkleDB error: {e}"))
            .map(|metadata| metadata.index_type())
    }
}

impl<T: Access> AccessExt for T {
    type Base<'a>
        = <Self as Access>::Base
    where
        Self: 'a;
    type Ref<'a>
        = Self
    where
        Self: 'a;

    fn get_ref(&self) -> Self::Ref<'_> {
        self.clone()
    }
}

impl AccessExt for Fork {
    type Base<'a> = &'a Self;
    type Ref<'a> = &'a Self;

    fn get_ref(&self) -> Self::Ref<'_> {
        self
    }
}

impl AccessExt for dyn Snapshot {
    type Base<'a> = &'a Self;
    type Ref<'a> = &'a Self;

    fn get_ref(&self) -> Self::Ref<'_> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{AccessExt, IndexType};
    use crate::{Database, TemporaryDB, access::Prefixed, migration::Migration};

    #[test]
    fn index_type_works() {
        let db = TemporaryDB::new();
        let fork = db.fork();
        fork.get_list("list").extend(vec![1, 2, 3]);
        assert_eq!(fork.index_type("list"), Some(IndexType::List));
        fork.get_map(("fam", &0_u8)).put(&1_u8, 2_u8);
        assert_eq!(fork.index_type(("fam", &0_u8)), Some(IndexType::Map));
        assert_eq!(fork.index_type(("fam", &1_u8)), None);

        let patch = fork.into_patch();
        {
            let patch = patch.as_ref();
            assert_eq!(patch.index_type("list"), Some(IndexType::List));
            assert_eq!(patch.index_type(("fam", &0_u8)), Some(IndexType::Map));
            assert_eq!(patch.index_type(("fam", &1_u8)), None);
        }

        db.merge(patch).unwrap();
        let snapshot = db.snapshot();
        assert_eq!(snapshot.index_type("list"), Some(IndexType::List));
        assert_eq!(snapshot.index_type(("fam", &0_u8)), Some(IndexType::Map));
        assert_eq!(snapshot.index_type(("fam", &1_u8)), None);
    }

    #[test]
    fn index_type_in_migration() {
        let db = TemporaryDB::new();
        let mut fork = db.fork();
        fork.get_list("some.list").extend(vec![1, 2, 3]);
        fork.get_entry(("some.entry", &0_u8)).set("!".to_owned());
        fork.get_entry(("some.entry", &1_u8)).set("!!".to_owned());

        {
            let migration = Migration::new("some", &fork);
            migration.get_list("list").extend(vec![4, 5, 6]);
            migration.create_tombstone(("entry", &0_u8));
            assert_eq!(migration.index_type("list"), Some(IndexType::List));
            assert_eq!(
                migration.index_type(("entry", &0_u8)),
                Some(IndexType::Tombstone)
            );
            assert_eq!(migration.index_type(("entry", &1_u8)), None);
        }
        fork.flush_migration("some");

        let patch = fork.into_patch();
        let ns = Prefixed::new("some", patch.as_ref());
        assert_eq!(ns.clone().index_type("list"), Some(IndexType::List));
        assert_eq!(ns.clone().index_type(("entry", &0_u8)), None);
        assert_eq!(
            ns.clone().index_type(("entry", &1_u8)),
            Some(IndexType::Entry)
        );

        db.merge(patch).unwrap();
        let snapshot = db.snapshot();
        assert_eq!(snapshot.index_type("some.list"), Some(IndexType::List));
        assert_eq!(snapshot.index_type(("some.entry", &0_u8)), None);
        assert_eq!(
            snapshot.index_type(("some.entry", &1_u8)),
            Some(IndexType::Entry)
        );
    }
}
