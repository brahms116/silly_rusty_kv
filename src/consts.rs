pub const PAGE_SIZE: usize = 4096;

pub const BUCKET_HEADER: usize = 1;

pub const HASH_RECORD_HEADER: usize = 1;

pub const HASH_LENGTH: usize = 8;

pub const HASH_VALUE_HEADER: usize = 2;

pub const MAX_HASH_VALUE_SIZE: usize =
    PAGE_SIZE - BUCKET_HEADER - HASH_RECORD_HEADER - HASH_LENGTH - HASH_VALUE_HEADER;
