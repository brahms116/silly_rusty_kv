use crate::command::*;
use std::collections::HashMap;
use uuid::Uuid;

pub struct Wal {
    data: HashMap<String, Vec<Mutation>>,
}

impl Wal {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn begin(&mut self) -> String {
        let key = Uuid::new_v4().to_string();
        self.data.insert(key.clone(), vec![]);
        key
    }

    pub fn retrieve_mutations(&mut self, key: &str) -> Option<Vec<Mutation>> {
        self.data.remove(key)
    }

    pub fn get(&self, key: &str, cmd: &GetCommand) -> Option<Option<String>> {
        if let Some(ms) = self.data.get(key) {
            for m in ms {
                match m {
                    Mutation::Put(PutCommand(ck, cv)) => {
                        if ck == &cmd.0 {
                            return Some(Some(cv.to_string()));
                        }
                    }
                    Mutation::Delete(DeleteCommand(ck)) => {
                        if ck == &cmd.0 {
                            return Some(None);
                        }
                    }
                }
            }
            None
        } else {
            None
        }
    }

    pub fn mutate(&mut self, key: &str, cmd: Mutation) -> Result<(), ()> {
        let ms = self.data.get_mut(key).ok_or(())?;
        ms.push(cmd);
        Ok(())
    }
}
