use crate::storage::*;

const DEFAULT_DB_FILE: &str = "data.db";

/// Returns the storage engine and the index engine and ensures
/// that everything is set up correctly.
pub fn setup_db() -> (StorageEngine, ()) {
    let storage = StorageEngine::new(DEFAULT_DB_FILE);
    let index = ();
    (storage, index)
}
