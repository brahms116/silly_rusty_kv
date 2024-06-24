use crate::hash_storage::*;
use crate::storage::*;

const DEFAULT_DB_FILE: &str = "data.db";

const DEFAULT_HASH_DB_FILE: &str = "hash_data.db";
const DEFAULT_HASH_DIRECTORY_FILE: &str = "hash_dir.db";

/// Returns the 2 storage engines available
/// that everything is set up correctly.
pub async fn setup_db() -> (StorageEngine, HashStorage) {
    let storage = StorageEngine::new(DEFAULT_DB_FILE);
    let hash_storage = HashStorage::new(DEFAULT_HASH_DIRECTORY_FILE, DEFAULT_HASH_DB_FILE).await;
    (storage, hash_storage)
}
