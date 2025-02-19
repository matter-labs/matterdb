pub use self::{
    address::{IndexAddress, ResolvedAddress},
    metadata::{
        BinaryAttribute, GroupKeys, IndexMetadata, IndexState, IndexType, IndexesPool,
        ViewWithMetadata,
    },
};

use std::{
    borrow::{Borrow, Cow},
    fmt, iter,
    iter::Peekable,
    marker::PhantomData,
};

use crate::{
    db::{Change, ChangesMut, ChangesRef, ForkIter, ViewChanges},
    views::address::key_bytes,
    BinaryKey, BinaryValue, Iter as BytesIter, Iterator as BytesIterator, Snapshot,
};

mod address;
mod metadata;
#[cfg(test)]
mod tests;

/// Represents current view of the database by specified `address` and
/// changes that took place after that view had been created. `View`
/// implementation provides an interface to work with related `changes`.
#[derive(Debug)]
pub enum View<T: RawAccess> {
    Real(ViewInner<T>),
    Phantom,
}

pub struct ViewInner<T: RawAccess> {
    address: ResolvedAddress,
    index_access: T,
    changes: T::Changes,
}

impl<T: RawAccess> fmt::Debug for ViewInner<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ViewInner")
            .field("address", &self.address)
            .finish()
    }
}

/// Utility trait to provide optional references to `ViewChanges`.
pub trait ChangeSet {
    fn as_ref(&self) -> Option<&ViewChanges>;
    /// Provides mutable reference to changes. The implementation for a `RawAccessMut` type
    /// should always return `Some(_)`.
    fn as_mut(&mut self) -> Option<&mut ViewChanges>;
}

/// No-op implementation used in `Snapshot`.
impl ChangeSet for () {
    fn as_ref(&self) -> Option<&ViewChanges> {
        None
    }
    fn as_mut(&mut self) -> Option<&mut ViewChanges> {
        None
    }
}

impl ChangeSet for ChangesRef<'_> {
    fn as_ref(&self) -> Option<&ViewChanges> {
        Some(&*self)
    }
    fn as_mut(&mut self) -> Option<&mut ViewChanges> {
        None
    }
}

impl ChangeSet for ChangesMut<'_> {
    fn as_ref(&self) -> Option<&ViewChanges> {
        Some(&*self)
    }
    fn as_mut(&mut self) -> Option<&mut ViewChanges> {
        Some(&mut *self)
    }
}

/// Allows to read data from the database. The data consists of a snapshot and
/// changes relative to this snapshot. Depending on the implementation, the changes
/// can be empty, immutable or mutable.
///
/// This trait is rarely needs to be used directly; [`Access`] is a more high-level trait
/// encompassing access to database. In particular, using `snapshot()` method to convert
/// the implementation into `&dyn Snapshot` is logically incorrect, because the snapshot
/// may not reflect the most recent state of `RawAccess`.
///
/// [`Access`]: trait.Access.html
pub trait RawAccess: Clone {
    /// Type of the `changes()` that will be applied to the database.
    type Changes: ChangeSet;

    /// Reference to a `Snapshot`. This is the base relative to which the changes are defined.
    fn snapshot(&self) -> &dyn Snapshot;
    /// Returns changes related to specific `address` compared to the `snapshot()`.
    fn changes(&self, address: &ResolvedAddress) -> Self::Changes;
}

/// Allows to mutate data in indexes.
///
/// This is a marker trait that is used as a bound for mutable operations on indexes.
/// It can be used in the same way for high-level database objects:
///
/// # Example
///
/// ```
/// use matterdb::{access::{Access, RawAccessMut}, ListIndex, MapIndex};
///
/// pub struct Schema<T: Access> {
///     list: ListIndex<T::Base, String>,
///     map: MapIndex<T::Base, u64, u64>,
/// }
///
/// impl<T: Access> Schema<T>
/// where
///     T::Base: RawAccessMut,
/// {
///     pub fn mutate(&mut self) {
///         self.list.push("foo".to_owned());
///         self.map.put(&1, 2);
///     }
/// }
/// ```
pub trait RawAccessMut: RawAccess {}

impl<'a, T> RawAccessMut for T where T: RawAccess<Changes = ChangesMut<'a>> {}

/// Converts index access to a readonly presentation. The conversion operation is cheap.
pub trait AsReadonly: RawAccess {
    /// Readonly version of the access.
    type Readonly: RawAccess;

    /// Performs the conversion.
    fn as_readonly(&self) -> Self::Readonly;
}

macro_rules! impl_snapshot_access {
    ($typ:ty) => {
        impl RawAccess for $typ {
            type Changes = ();

            fn snapshot(&self) -> &dyn Snapshot {
                self.as_ref()
            }

            fn changes(&self, _address: &ResolvedAddress) -> Self::Changes {}
        }

        impl AsReadonly for $typ {
            type Readonly = Self;

            fn as_readonly(&self) -> Self::Readonly {
                self.clone()
            }
        }
    };
}

impl_snapshot_access!(&'_ dyn Snapshot);
impl_snapshot_access!(&'_ Box<dyn Snapshot>);
impl_snapshot_access!(std::rc::Rc<dyn Snapshot>);
impl_snapshot_access!(std::sync::Arc<dyn Snapshot>);

impl<T: RawAccess> ViewInner<T> {
    fn snapshot(&self) -> &dyn Snapshot {
        self.index_access.snapshot()
    }

    fn get_bytes(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.changes
            .as_ref()
            .map_or(Err(()), |changes| changes.get(key))
            // At this point, `Err(_)` signifies that we need to retrieve data from the snapshot.
            .unwrap_or_else(|()| self.snapshot().get(&self.address, key))
    }

    fn multi_get_bytes<I>(&self, keys: I) -> Vec<Option<Vec<u8>>>
    where
        I: iter::Iterator<Item = Vec<u8>>,
    {
        let changes = self.changes.as_ref();

        let (mut res, db_keys) = keys.into_iter().enumerate().fold(
            (Vec::new(), Vec::new()),
            |(mut res, mut db_keys), (idx, key)| {
                if let Ok(item) = changes
                    .and_then(|changes| changes.get(&key).transpose())
                    .transpose()
                {
                    res.push(item);
                } else {
                    res.push(None);
                    db_keys.push((idx, key));
                }

                (res, db_keys)
            },
        );

        let db_res = self.snapshot().multi_get(
            &self.address,
            &mut db_keys.iter().map(|(_, key)| key.as_ref()),
        );

        for ((idx, _), item) in db_keys.into_iter().zip(db_res) {
            res[idx] = item;
        }

        res
    }

    fn contains_raw_key(&self, key: &[u8]) -> bool {
        self.changes
            .as_ref()
            .map_or(Err(()), |changes| changes.contains(key))
            // At this point, `Err(_)` signifies that we need to retrieve data from the snapshot.
            .unwrap_or_else(|()| self.snapshot().contains(&self.address, key))
    }

    fn iter_bytes(&self, from: &[u8]) -> BytesIter<'_> {
        use std::ops::Bound::{Included, Unbounded};

        let changes_iter = self
            .changes
            .as_ref()
            .map(|changes| changes.data.range::<[u8], _>((Included(from), Unbounded)));

        let is_cleared = self.changes.as_ref().map_or(false, ViewChanges::is_cleared);
        if is_cleared {
            // Ignore all changes from the snapshot.
            Box::new(ChangesIter::new(changes_iter.unwrap()))
        } else {
            Box::new(ForkIter::new(
                self.snapshot().iter(&self.address, from),
                changes_iter,
            ))
        }
    }
}

impl<T: RawAccess> View<T> {
    /// Creates a new view for an index with the specified address.
    pub(crate) fn new(index_access: T, address: impl Into<ResolvedAddress>) -> Self {
        let address = address.into();
        let changes = index_access.changes(&address);
        Self::Real(ViewInner {
            address,
            index_access,
            changes,
        })
    }

    /// Creates a new phantom view. The phantom views do not borrow changes and do not retain
    /// resolved address / access.
    pub(crate) fn new_phantom() -> Self {
        Self::Phantom
    }

    /// Returns the access this view is attached to. If this view is phantom, returns `None`.
    pub(crate) fn access(&self) -> Option<&T> {
        match self {
            Self::Real(ViewInner { index_access, .. }) => Some(index_access),
            Self::Phantom => None,
        }
    }

    fn get_bytes(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Real(inner) => inner.get_bytes(key),
            Self::Phantom => None,
        }
    }

    fn multi_get_bytes<I>(&self, keys: I) -> Vec<Option<Vec<u8>>>
    where
        I: Iterator<Item = Vec<u8>>,
    {
        match self {
            Self::Real(inner) => inner.multi_get_bytes(keys),
            Self::Phantom => vec![None; keys.count()],
        }
    }

    fn contains_raw_key(&self, key: &[u8]) -> bool {
        match self {
            Self::Real(inner) => inner.contains_raw_key(key),
            Self::Phantom => false,
        }
    }

    fn iter_bytes(&self, from: &[u8]) -> BytesIter<'_> {
        match self {
            Self::Real(inner) => inner.iter_bytes(from),
            Self::Phantom => Box::new(EmptyIterator),
        }
    }

    /// Returns a value of *any* type corresponding to the key of *any* type.
    pub fn get<K, V>(&self, key: &K) -> Option<V>
    where
        K: BinaryKey + ?Sized,
        V: BinaryValue,
    {
        self.get_bytes(&key_bytes(key)).map(|v| {
            BinaryValue::from_bytes(Cow::Owned(v)).expect("Error while deserializing value")
        })
    }

    pub fn multi_get<K, V, I>(&self, keys: I) -> Vec<Option<V>>
    where
        K: BinaryKey + ?Sized,
        V: BinaryValue,
        I: IntoIterator,
        I::Item: Borrow<K>,
    {
        self.multi_get_bytes(&mut keys.into_iter().map(|key| key_bytes(key.borrow())))
            .into_iter()
            .map(|v| {
                v.map(|v| {
                    BinaryValue::from_bytes(Cow::Owned(v)).expect("Error while deserializing value")
                })
            })
            .collect()
    }

    /// Returns `true` if the index contains a value of *any* type for the specified key of
    /// *any* type.
    pub fn contains<K>(&self, key: &K) -> bool
    where
        K: BinaryKey + ?Sized,
    {
        self.contains_raw_key(&key_bytes(key))
    }

    /// Returns an iterator over the entries of the index in ascending order. The iterator element
    /// type is *any* key-value pair. An argument `subprefix` allows specifying a subset of keys
    /// for iteration.
    pub fn iter<P, K, V>(&self, subprefix: &P) -> Iter<'_, K, V>
    where
        P: BinaryKey + ?Sized,
        K: BinaryKey + ?Sized,
        V: BinaryValue,
    {
        let iter_prefix = key_bytes(subprefix);
        Iter {
            base_iter: self.iter_bytes(&iter_prefix),
            prefix: iter_prefix,
            detach_prefix: false,
            ended: false,
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    /// Returns an iterator over the entries of the index in ascending order starting from the
    /// specified key. The iterator element type is *any* key-value pair. An argument `subprefix`
    /// allows specifying a subset of iteration.
    pub fn iter_from<P, F, K, V>(&self, subprefix: &P, from: &F) -> Iter<'_, K, V>
    where
        P: BinaryKey,
        F: BinaryKey + ?Sized,
        K: BinaryKey + ?Sized,
        V: BinaryValue,
    {
        let iter_prefix = key_bytes(subprefix);
        let iter_from = key_bytes(from);
        Iter {
            base_iter: self.iter_bytes(&iter_from),
            prefix: iter_prefix,
            detach_prefix: false,
            ended: false,
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    /// Sets a key / value pair in the view storage, unless the view is backed by a readonly access
    /// (in which case, the changes are forgotten).
    ///
    /// # Return value
    ///
    /// Returns whether the changes were saved.
    pub(crate) fn put_or_forget<K, V>(&mut self, key: &K, value: V) -> bool
    where
        K: BinaryKey + ?Sized,
        V: BinaryValue,
    {
        if let Self::Real(inner) = self {
            if let Some(changes) = inner.changes.as_mut() {
                changes
                    .data
                    .insert(concat_keys!(key), Change::Put(value.into_bytes()));
                return true;
            }
        }
        false
    }
}

impl<T: RawAccessMut> View<T> {
    fn changes_mut(&mut self) -> &mut ViewChanges {
        const ACCESS_ERROR: &str =
            "Attempt to modify a readonly view of the database using a generic access. \
             The caller should check the access type before calling any mutable methods";

        match self {
            Self::Real(ViewInner { changes, .. }) => changes.as_mut().expect(ACCESS_ERROR),
            Self::Phantom => panic!("{}", ACCESS_ERROR),
        }
    }

    /// Inserts a key-value pair into the fork.
    pub fn put<K, V>(&mut self, key: &K, value: V)
    where
        K: BinaryKey + ?Sized,
        V: BinaryValue,
    {
        self.changes_mut()
            .data
            .insert(concat_keys!(key), Change::Put(value.into_bytes()));
    }

    /// Removes a key from the view.
    pub fn remove<K>(&mut self, key: &K)
    where
        K: BinaryKey + ?Sized,
    {
        self.changes_mut()
            .data
            .insert(concat_keys!(key), Change::Delete);
    }

    /// Clears the view removing all its elements.
    pub fn clear(&mut self) {
        self.changes_mut().clear();
    }
}

/// A bytes iterator implementation that has no items.
struct EmptyIterator;

impl BytesIterator for EmptyIterator {
    fn next(&mut self) -> Option<(&[u8], &[u8])> {
        None
    }

    fn peek(&mut self) -> Option<(&[u8], &[u8])> {
        None
    }
}

pub struct ChangesIter<'a, T: Iterator + 'a> {
    inner: Peekable<T>,
    _lifetime: PhantomData<&'a ()>,
}

/// Iterator over a set of changes.
impl<'a, T> ChangesIter<'a, T>
where
    T: Iterator<Item = (&'a Vec<u8>, &'a Change)>,
{
    pub fn new(iterator: T) -> Self {
        ChangesIter {
            inner: iterator.peekable(),
            _lifetime: PhantomData,
        }
    }
}

impl<'a, T> BytesIterator for ChangesIter<'a, T>
where
    T: Iterator<Item = (&'a Vec<u8>, &'a Change)>,
{
    fn next(&mut self) -> Option<(&[u8], &[u8])> {
        loop {
            match self.inner.next() {
                Some((key, &Change::Put(ref value))) => {
                    return Some((key.as_slice(), value.as_slice()));
                }
                Some((_, &Change::Delete)) => {}
                None => {
                    return None;
                }
            }
        }
    }

    fn peek(&mut self) -> Option<(&[u8], &[u8])> {
        loop {
            match self.inner.peek() {
                Some((key, Change::Put(ref value))) => {
                    return Some((key.as_slice(), value.as_slice()));
                }
                Some((_, Change::Delete)) => {
                    // Advance the iterator. Since we've already peeked the value,
                    // we can safely drop it.
                    self.inner.next();
                }
                None => {
                    return None;
                }
            }
        }
    }
}

/// An iterator over the entries of a `View`.
///
/// This struct is created by the [`iter`] or
/// [`iter_from`] method on [`View`]. See its documentation for details.
///
/// [`iter`]: struct.BaseIndex.html#method.iter
/// [`iter_from`]: struct.BaseIndex.html#method.iter_from
/// [`BaseIndex`]: struct.BaseIndex.html
pub struct Iter<'a, K: ?Sized, V> {
    base_iter: BytesIter<'a>,
    prefix: Vec<u8>,
    detach_prefix: bool,
    ended: bool,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<'a, K: ?Sized, V> fmt::Debug for Iter<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Iter(..)")
    }
}

impl<'a, K, V> Iter<'a, K, V>
where
    K: BinaryKey + ?Sized,
    V: BinaryValue,
{
    /// Drops the keys returned by the underlying iterator without parsing them.
    pub(crate) fn drop_key_type(self) -> Iter<'a, (), V> {
        Iter {
            base_iter: self.base_iter,
            prefix: self.prefix,
            detach_prefix: self.detach_prefix,
            ended: self.ended,
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    /// Drops the values returned by the underlying iterator without parsing them.
    pub(crate) fn drop_value_type(self) -> Iter<'a, K, ()> {
        Iter {
            base_iter: self.base_iter,
            prefix: self.prefix,
            detach_prefix: self.detach_prefix,
            ended: self.ended,
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: BinaryKey + ?Sized,
    V: BinaryValue,
{
    type Item = (K::Owned, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }

        if let Some((key_slice, value_slice)) = self.base_iter.next() {
            if key_slice.starts_with(&self.prefix) {
                let key = if self.detach_prefix {
                    // Since we've checked `start_with`, slicing here cannot panic.
                    K::read(&key_slice[self.prefix.len()..])
                } else {
                    K::read(key_slice)
                };
                let value = V::from_bytes(Cow::Borrowed(value_slice))
                    .expect("Unable to decode value from bytes");
                return Some((key, value));
            }
        }

        self.ended = true;
        None
    }
}
