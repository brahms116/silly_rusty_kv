use std::fs::{File, OpenOptions};
pub fn reset_or_create_file(name: &str) -> File {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(name)
        .unwrap()
}
