use std::str::FromStr;

pub trait ByteLength {
    fn byte_len(&self) -> usize;
}

pub trait IntoBytes {
    fn into_bytes(self) -> Vec<u8>;
}

pub trait FromBytes: Sized {
    type Error;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error>;
}

impl IntoBytes for PutCommand {
    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let key_bytes = self.0.into_bytes();
        let value_bytes = self.1.into_bytes();
        let key_len = key_bytes.len() as u8;
        let value_len = value_bytes.len() as u16;
        let header: u8 = 1;
        let value_len_slice = value_len.to_le_bytes();

        bytes.push(header);
        bytes.push(key_len);
        bytes.extend(value_len_slice);
        bytes.extend(key_bytes);
        bytes.extend(value_bytes);
        bytes
    }
}

impl FromBytes for PutCommand {
    type Error = ();

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let header = bytes[0];
        if header != 1 {
            return Err(());
        }
        let key_len = bytes[1];
        let value_len = u16::from_le_bytes([bytes[2], bytes[3]]);
        let key = String::from_utf8(bytes[4..4 + key_len as usize].to_vec()).unwrap();
        let value = String::from_utf8(
            bytes[4 + key_len as usize..4 + key_len as usize + value_len as usize].to_vec(),
        )
        .unwrap();
        Ok(PutCommand(key, value))
    }
}

impl FromBytes for DeleteCommand {
    type Error = ();

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let header = bytes[0];
        if header != 0 {
            return Err(());
        }
        let key_len = bytes[1];
        let key = String::from_utf8(bytes[2..2 + key_len as usize].to_vec()).unwrap();
        Ok(DeleteCommand(key))
    }
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
        let header: u8 = 0;

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

impl FromBytes for Mutation {
    type Error = ();

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let header = bytes[0];
        match header {
            0 => Ok(Mutation::Delete(DeleteCommand::from_bytes(bytes)?)),
            1 => Ok(Mutation::Put(PutCommand::from_bytes(bytes)?)),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Put(PutCommand),
    Delete(DeleteCommand),
    Get(GetCommand),
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
            _ => Err("Unknown command".to_string()),
        }
    }
}
