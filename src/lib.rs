#![allow(dead_code)]
#![allow(unused_variables)]


#[cfg(test)]
mod test;

mod consts;
mod command;
mod repl;
mod setup;
mod execute;
mod storage;
mod parse;
mod stdin;
mod hash_storage;
mod bytes;
mod handle_command;

// Queue of commands
// Takes in commands and executes them

pub use repl::*;
pub use stdin::*;
