# Toy database

A toy experiment to implement a persistent key value store with Rust.

## Usage

Run the current repl with:

```
cargo run --release -- --repl
```

Run the server with...
The server will start on `localhost:5476`. You can connect to it with `nc`

```
cargo run --release
nc localhost 5476
```

Run through stdin with:

```
cat commands.txt | cargo run --release -- --stdin
```

## Features

Commands include:

-   `PUT key "value"`: Insert a key-value pair into the database
-   `GET key`: Retrieve the value for a key
-   `DELETE key`: Delete a key-value pair from the database
-   `EXIT`: Close the database, ignored in stdin and server modes
-   `BEGIN`: Start a transaction
-   `COMMIT`: Commit a transaction
-   `ROLLBACK`: Rollback a transaction

## Testing and benching

Very primitive testing and benching is done in a separate repo [here](https://github.com/brahms116/silly_rusty_kv_test/tree/main)

## TODOs:

-   [x] Implment a hash storage engine for the database
    -   [x] What performance impacts does this have?
            Turns out it's fast because I don't call fsync, but if I do, it slows down significantly
-   [x] Impelment a server client thingo so I can keep the
        database running whilst executing different commands from different connections against it
    -   [x] Make distinct modes in running the db, repl, stdin etc
    -   [x] Implement the tcp endpoint
-   [x] Implement a in memory WAL for the database
-   [ ] Make it possible to close the database from the client tcp socket instead of ignoring the EXIT command
-   [ ] Work on cleanup from signals
-   [ ] Reimplmenet the legacy appendonly storage engine and modify it contain use the in memory WAL and the hash table as an index
-   [ ] Clean up the code
