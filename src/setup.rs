use crate::hash_storage::*;
use crate::wal::*;

const DEFAULT_DB_FILE: &str = "data.db";

const DEFAULT_HASH_DB_FILE: &str = "hash_data.db";
const DEFAULT_HASH_DIRECTORY_FILE: &str = "hash_dir.db";

pub async fn setup_db() -> (HashStorage, Wal) {
    let hash_storage = HashStorage::new(DEFAULT_HASH_DIRECTORY_FILE, DEFAULT_HASH_DB_FILE).await;
    (hash_storage, Wal::new())
}
