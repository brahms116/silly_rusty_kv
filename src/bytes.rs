pub trait ByteLength {
    fn byte_len(&self) -> usize;
}

pub trait IntoBytes {
    fn into_bytes(self) -> Vec<u8>;
}

pub trait ParseFromBytes<'a, T>: Sized
where
    T: Iterator<Item = &'a u8>,
{
    type Error;
    type Metadata;
    fn from_bytes(bytes: &T, metadata: Self::Metadata) -> Result<(Self, T), Self::Error>;
}
