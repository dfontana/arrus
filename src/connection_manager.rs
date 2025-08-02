use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A generic connection manager trait that abstracts HashMap operations
/// for managing socket connections across different transport types
pub trait ConnectionManager<T: Clone> {
    /// Insert a new connection
    async fn insert(&self, id: u32, connection: T);

    /// Remove a connection by ID, returning the removed connection if it existed
    async fn remove(&self, id: u32) -> Option<T>;
}

/// A concrete implementation using Arc<Mutex<HashMap>>
#[derive(Clone)]
pub struct HashMapConnectionManager<T: Clone> {
    connections: Arc<Mutex<HashMap<u32, T>>>,
}

impl<T: Clone> HashMapConnectionManager<T> {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T: Clone> Default for HashMapConnectionManager<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> ConnectionManager<T> for HashMapConnectionManager<T> {
    async fn insert(&self, id: u32, connection: T) {
        self.connections.lock().await.insert(id, connection);
    }

    async fn remove(&self, id: u32) -> Option<T> {
        self.connections.lock().await.remove(&id)
    }
}
