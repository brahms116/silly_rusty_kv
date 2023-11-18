#![allow(dead_code)]
#![allow(unused_variables)]


mod consts;
mod command;
mod repl;
mod setup;
mod execute;
mod storage;
mod parse;

// Queue of commands
// Takes in commands and executes them

pub use repl::*;
