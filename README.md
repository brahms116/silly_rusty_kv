# Toy database

A toy experiment to implement an append-only database with Rust.

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

-   [ ] Implment an index for the database
    -   [ ] What performance impacts does this have?
-   [ ] Investigate why and if holding the stdin pipe for longer is causing the program
        to block writes.
-   [ ] Impelment a server client thingo so I can keep the
        database running different commands for it
