use std::str::FromStr;

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

impl IntoBytes for PutCommand {
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let key_bytes = self.0.into_bytes();
        let value_bytes = self.1.into_bytes();
        let key_len = key_bytes.len() as u8;
        let value_len = value_bytes.len() as u16;
        let header: u8 = 2;
        let value_len_slice = value_len.to_le_bytes();

        bytes.push(header);
        bytes.push(key_len);
        bytes.extend(value_len_slice);
        bytes.extend(key_bytes);
        bytes.extend(value_bytes);
        bytes
    }
}

impl<T> ParseFromBytes<T> for PutCommand
where
    T: Iterator<Item = u8>,
{
    type Error = ();

    fn from_bytes(mut bytes: T) -> Result<(Self, T), Self::Error> {
        let key_len = bytes.next().ok_or(())?;
        let value_len_bytes: Vec<u8> = bytes.by_ref().take(2).collect();
        let value_len = u16::from_le_bytes([value_len_bytes[0], value_len_bytes[1]]);
        let key_bytes: Vec<u8> = bytes.by_ref().take(key_len as usize).collect();
        let value_bytes: Vec<u8> = bytes.by_ref().take(value_len as usize).collect();
        let key = String::from_utf8(key_bytes).map_err(|_| ())?;
        let value = String::from_utf8(value_bytes).map_err(|_| ())?;
        Ok((PutCommand(key, value), bytes))
    }
}

impl<T> ParseFromBytes<T> for DeleteCommand
where
    T: Iterator<Item = u8>,
{
    type Error = ();

    fn from_bytes(mut bytes: T) -> Result<(Self, T), ()> {
        let key_len = bytes.next().ok_or(())?;
        let key_bytes: Vec<u8> = bytes.by_ref().take(key_len as usize).collect();
        let key = String::from_utf8(key_bytes).map_err(|_| ())?;
        Ok((DeleteCommand(key), bytes))
    }
}

impl<T> ParseFromBytes<T> for Mutation
where
    T: Iterator<Item = u8>,
{
    type Error = ();

    fn from_bytes(mut bytes: T) -> Result<(Self, T), Self::Error> {
        let header = bytes.next().ok_or(())?;
        match header {
            1 => {
                let (cmd, rest) = DeleteCommand::from_bytes(bytes)?;
                Ok((Mutation::Delete(cmd), rest))
            }
            2 => {
                let (cmd, rest) = PutCommand::from_bytes(bytes)?;
                Ok((Mutation::Put(cmd), rest))
            }
            _ => Err(()),
        }
    }
}

pub fn get_value_from_buffer<T: Iterator<Item = u8>>(bytes: T) -> Result<Option<String>, ()> {
    let mut rest = bytes;
    let mut value: Option<String> = None;
    while let Ok((mutation, new_rest)) = Mutation::from_bytes(rest) {
        match mutation {
            Mutation::Put(PutCommand(k, v)) => {
                value = Some(v);
            }
            Mutation::Delete(DeleteCommand(k)) => {
                value = None;
            }
        }
        rest = new_rest;
    }
    Ok(value)
}

pub fn get_value_from_mutations_ref<'a, T: Iterator<Item = &'a Mutation>>(
    muts: T,
    key: &str,
) -> Option<String> {
    let mut value: Option<String> = None;
    for m in muts {
        match m {
            Mutation::Put(PutCommand(k, v)) => {
                if k == key {
                    value = Some(v.clone());
                }
            }
            Mutation::Delete(DeleteCommand(k)) => {
                if k == key {
                    value = None;
                }
            }
        }
    }
    value
}

pub fn get_value_from_mutations<T: Iterator<Item = Mutation>>(
    muts: &mut T,
    key: &str,
) -> Option<String> {
    let mut value: Option<String> = None;
    for m in muts {
        match m {
            Mutation::Put(PutCommand(k, v)) => {
                if k == key {
                    value = Some(v);
                }
            }
            Mutation::Delete(DeleteCommand(k)) => {
                if k == key {
                    value = None;
                }
            }
        }
    }
    value
}

pub fn parse_buffer_to_mutations<T: Iterator<Item = u8>>(bytes: T) -> Result<Vec<Mutation>, ()> {
    let mut mutations = Vec::new();
    let mut rest = bytes;
    while let Ok((mutation, new_rest)) = Mutation::from_bytes(rest) {
        mutations.push(mutation);
        rest = new_rest;
    }
    Ok(mutations)
}

impl ByteLength for PutCommand {
    fn byte_len(&self) -> usize {
        let key_len = self.0.as_bytes().len();
        let value_len = self.1.as_bytes().len();
        1 + 1 + 2 + key_len + value_len
    }
}

impl ByteLength for DeleteCommand {
    fn byte_len(&self) -> usize {
        let key_len = self.0.as_bytes().len();
        1 + 1 + key_len
    }
}

impl IntoBytes for DeleteCommand {
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let key_bytes = self.0.into_bytes();
        let key_len = key_bytes.len() as u8;
        let header: u8 = 1;

        bytes.push(header);
        bytes.push(key_len);
        bytes.extend(key_bytes);
        bytes
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PutCommand(pub String, pub String);

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteCommand(pub String);

#[derive(Debug, Clone, PartialEq)]
pub struct GetCommand(pub String);

#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Put(PutCommand),
    Delete(DeleteCommand),
}

impl ByteLength for Mutation {
    fn byte_len(&self) -> usize {
        match self {
            Mutation::Put(cmd) => cmd.byte_len(),
            Mutation::Delete(cmd) => cmd.byte_len(),
        }
    }
}

impl IntoBytes for Mutation {
    fn into_bytes(self) -> Vec<u8> {
        match self {
            Mutation::Put(cmd) => cmd.into_bytes(),
            Mutation::Delete(cmd) => cmd.into_bytes(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Put(PutCommand),
    Delete(DeleteCommand),
    Get(GetCommand),
    Exit,
}

impl FromStr for Command {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // need a better split here soon
        let mut iter = s.split_whitespace();
        let cmd = iter.next().ok_or("No command")?;
        match cmd {
            "PUT" => {
                let key = iter.next().ok_or("No key")?;
                let value = iter.next().ok_or("No value")?;
                Ok(Command::Put(PutCommand(key.to_string(), value.to_string())))
            }
            "DELETE" => {
                let key = iter.next().ok_or("No key")?;
                Ok(Command::Delete(DeleteCommand(key.to_string())))
            }
            "GET" => {
                let key = iter.next().ok_or("No key")?;
                Ok(Command::Get(GetCommand(key.to_string())))
            }
            "EXIT" => Ok(Command::Exit),
            _ => Err("Unknown command".to_string()),
        }
    }
}
