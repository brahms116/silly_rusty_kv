pub trait ByteLength {
    fn byte_len(&self) -> usize;
}

pub trait IntoBytes {
    fn into_bytes(self) -> Vec<u8>;
}

pub trait ParseFromBytes<T>: Sized
where
    T: Iterator<Item = u8>,
{
    type Error;
    fn from_bytes(bytes: T) -> Result<(Self, T), Self::Error>;
}
