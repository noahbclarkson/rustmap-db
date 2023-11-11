//! Reference module for values in rustmap-db structures.
//!
//! This module provides the `ValueRef` struct, which is a wrapper around a reference to a value
//! stored in the `HashMap`. It allows for read-only access to the values within the map.

use std::hash::Hash;

/// A reference to a value in a `HashMap`.
///
/// `ValueRef` is a wrapper around a reference to a value in the map, providing a way
/// to read values without taking ownership of them. This is useful when you want to inspect
/// values stored in the map without affecting their state or ownership.
pub struct ValueRef<'a, K, V> {
    inner: dashmap::mapref::one::Ref<'a, K, V>,
}

impl<'a, K, V> ValueRef<'a, K, V>
where
    K: Eq + Hash,
{
    /// Creates a new `ValueRef` from a reference to a key-value pair in the map.
    ///
    /// # Arguments
    ///
    /// * `inner` - A reference to the key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustmap_db::structures::ValueRef;
    /// let value_ref = ValueRef::new(inner_ref);
    /// ```
    pub fn new(inner: dashmap::mapref::one::Ref<'a, K, V>) -> Self {
        Self { inner }
    }

    /// Returns a reference to the value.
    ///
    /// This method allows you to read the value associated with the key without cloning it.
    ///
    /// # Examples
    ///
    /// ```
    /// let value = value_ref.value();
    /// println!("The value is: {:?}", value);
    /// ```
    pub fn value(&self) -> &V {
        self.inner.value()
    }

    /// Returns a reference to the key.
    ///
    /// This method allows you to read the key associated with the value.
    ///
    /// # Examples
    ///
    /// ```
    /// let key = value_ref.key();
    /// println!("The key is: {:?}", key);
    /// ```
    pub fn key(&self) -> &K {
        self.inner.key()
    }

    /// Returns a reference to the key-value pair.
    ///
    /// This method allows you to access both the key and the value without taking ownership.
    ///
    /// # Examples
    ///
    /// ```
    /// let (key, value) = value_ref.pair();
    /// println!("The key is: {:?}, and the value is: {:?}", key, value);
    /// ```
    pub fn pair(&self) -> (&K, &V) {
        self.inner.pair()
    }

    /// Consumes the `ValueRef`, returning the owned key-value pair.
    ///
    /// This method allows you to convert the `ValueRef` into an owned key-value pair,
    /// taking ownership of both the key and the value.
    ///
    /// # Examples
    ///
    /// ```
    /// let (key, value) = value_ref.into_owned();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the key or value cannot be cloned (which should not occur with normal usage).
    pub fn into_owned(self) -> (K, V)
    where
        K: Clone,
        V: Clone,
    {
        (self.inner.key().clone(), self.inner.value().clone())
    }
}