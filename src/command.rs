use std::str::FromStr;


#[derive(Debug, Clone, PartialEq)]
pub struct PutCommand(pub String, pub String);

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteCommand(pub String);

#[derive(Debug, Clone, PartialEq)]
pub struct GetCommand(pub String);

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
