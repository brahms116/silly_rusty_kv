# Toy database

A toy experiment to implement a database with Rust.

## Usage

Run the current repl with:

```
cargo run --release -- --repl
```

Commands include:

-   `PUT kev "value"`: Insert a key-value pair into the database
-   `GET key`: Retrieve the value for a key
-   `DELETE key`: Delete a key-value pair from the database

## Features

## TODOs:

-   [x] Implment a hash storage engine for the database
    -   [x] What performance impacts does this have?
            Turns out it's fast because I don't call fsync, but if I do, it slows down significantly
-   [ ] Impelment a server client thingo so I can keep the
        database running different commands for it
    -   [ ] Make distinct modes in running the db, repl, stdin etc
    -   [ ] Implement the tcp endpoint
-   [ ] Implement a in memory WAL for the database
    -  [ ] Implement the transaction command
           This will need a new type, splitting the current command into control and non-control
-   [ ] Refactor the legacy appendonly thing to contain use the in memory WAL and the hash table as an index
