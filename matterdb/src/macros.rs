//! Macros useful for work with types that implement `BinaryKey` and `BinaryValue` traits.

/// Fast concatenation of byte arrays and/or keys that implements `BinaryKey` trait.
macro_rules! concat_keys {
    (@capacity $key:expr) => ( $key.size() );
    (@capacity $key:expr, $($tail:expr),+) => (
        BinaryKey::size($key) + concat_keys!(@capacity $($tail),+)
    );
    ($($key:expr),+) => ({
        let capacity = concat_keys!(@capacity $($key),+);

        let mut buf = vec![0; capacity];
        let mut _pos = 0;
        $(
            _pos += BinaryKey::write($key, &mut buf[_pos.._pos + BinaryKey::size($key)]);
        )*
        buf
    });
}
