use std::hash::Hash;

/// A reference to a value in a HashMap.
pub struct ValueRef<'a, K, V> {
    inner: dashmap::mapref::one::Ref<'a, K, V>,
}

impl<'a, K, V> ValueRef<'a, K, V>
where
    K: Eq + Hash,
{
    /// Create a new Value Reference
    pub fn new(inner: dashmap::mapref::one::Ref<'a, K, V>) -> Self {
        Self { inner }
    }
    /// Returns a reference to the value.
    pub fn value(&self) -> &V {
        self.inner.value()
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.inner.key()
    }

    /// Returns a reference to the key-value pair.
    pub fn pair(&self) -> (&K, &V) {
        self.inner.pair()
    }

    /// Returns the key-value pair.
    pub fn into_owned(self) -> (K, V)
    where
        K: Clone,
        V: Clone,
    {
        (self.inner.key().clone(), self.inner.value().clone())
    }
}
