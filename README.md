# Java Redis Clone

A Redis-like in-memory database built from scratch using **Java 21**.

This project implements TCP client–server communication, the Redis Serialization Protocol (RESP), key-value storage, key expiration, snapshot persistence, master–replica replication, Redis Streams, blocking reads, and transactions.

The goal of this project is to understand how an in-memory database works internally, including networking, concurrency, persistence, and replication.

> This is an educational Redis-like implementation, not a production-ready replacement for Redis.

## Tech Stack

- Java 21
- Maven
- Java Socket Programming
- Java Collections and ConcurrentHashMap
- Multithreading and Synchronization
- Redis Serialization Protocol (RESP)
- Redis CLI for testing

## Features

| Category | Implemented Features |
|---|---|
| Networking | TCP server, multiple client connections |
| Protocol | RESP command parsing and response serialization |
| Basic commands | `PING`, `ECHO`, `SET`, `GET` |
| Key expiration | `SET key value PX milliseconds` |
| Persistence | RDB loading, snapshot saving using `SAVE` |
| Replication | Master–replica handshake, initial synchronization, live command propagation |
| Replication commands | `INFO replication`, `WAIT` |
| Streams | `XADD`, `XRANGE`, `XREAD`, blocking `XREAD` |
| Transactions | `MULTI`, `EXEC`, `DISCARD` |
| Configuration | Custom port, data directory, database filename, and replica configuration |

## System Architecture

```text
                       REDIS CLI
                           |
                           | TCP / RESP
                           v
                  +------------------+
                  |   RedisServer    |
                  |   ServerSocket   |
                  +------------------+
                           |
                           v
                  +------------------+
                  |  ClientHandler   |
                  +------------------+
                           |
                           v
                  +------------------+
                  |   RESP Parser    |
                  +------------------+
                           |
                           v
                  +----------------------+
                  | TransactionProcessor |
                  +----------------------+
                           |
                           v
                  +------------------+
                  | CommandDispatcher|
                  +------------------+
                           |
              +------------+------------+
              |                         |
              v                         v
       +-------------+           +-------------+
       | RedisStore  |           | StreamStore |
       +-------------+           +-------------+
              |                         |
              +------------+------------+
                           |
                +----------+----------+
                |                     |
                v                     v
           Persistence            Replication
                |                     |
                v                     v
            Snapshot              Replica
             files               Server
```

### Request Processing

When a client executes:

```bash
redis-cli -p 6379 SET name Ansh
```

The request follows this flow:

1. `redis-cli` sends the command over a TCP connection.
2. `ClientHandler` reads the incoming bytes.
3. The RESP parser converts the request into command arguments.
4. `TransactionProcessor` determines whether to execute or queue the command.
5. `CommandDispatcher` selects the appropriate command implementation.
6. `SET` stores the value in `RedisStore`.
7. The server serializes the response and sends it to the client.

## Getting Started

### Prerequisites

- Java 21
- Maven
- `redis-cli` for testing

### 1. Clone the Repository

```bash
git clone https://github.com/Ansh-dhama/Redis.git
cd Redis
```

### 2. Build the Project

```bash
mvn clean compile
```

### 3. Start the Master Server

```bash
java -cp target/classes org.example.Main \
  --port 6379 \
  --dir /tmp/redis-master \
  --dbfilename dump.rdb
```

The server listens on port `6379`.

### 4. Connect Using Redis CLI

Open a new terminal:

```bash
redis-cli -p 6379
```

Test the connection:

```redis
PING
```

Expected response:

```text
PONG
```

> Keep the Java server running while executing commands. Stop running server processes before using `mvn clean compile`, because the build deletes and recreates the compiled class files.

## Supported Commands

### 1. Basic Key-Value Operations

Set a value:

```bash
redis-cli -p 6379 SET name Ansh
```

Get the value:

```bash
redis-cli -p 6379 GET name
```

Expected result:

```text
"Ansh"
```

### 2. Key Expiration

Set a key that expires after five seconds:

```bash
redis-cli -p 6379 SET otp 123456 PX 5000
```

Read it immediately:

```bash
redis-cli -p 6379 GET otp
```

After five seconds:

```bash
redis-cli -p 6379 GET otp
```

Expected result:

```text
(nil)
```

**Implementation:** Each stored entry can have an expiration timestamp. When a key is accessed, the server checks whether it has expired and removes it if necessary.

This approach is known as **lazy expiration**.

## Persistence

The project supports saving data to disk and restoring it after a server restart.

Save the current data:

```bash
redis-cli -p 6379 SAVE
```

The project uses separate files for ordinary key-value data and streams:

```text
/tmp/redis-master/
├── dump.rdb
└── dump.rdb.streams
```

- `dump.rdb` stores ordinary key-value data.
- `dump.rdb.streams` stores stream entries using a custom snapshot format.

Restart the master with the same data directory and database filename to reload the saved data.

> Stream snapshots use a custom format and are not compatible with the native Redis RDB stream representation.

## Master–Replica Replication

The project supports running a master and replica as separate Java processes.

### 1. Start the Master

```bash
java -cp target/classes org.example.Main \
  --port 6379 \
  --dir /tmp/redis-master \
  --dbfilename dump.rdb
```

### 2. Start the Replica

Open a second terminal:

```bash
java -cp target/classes org.example.Main \
  --port 6380 \
  --dir /tmp/redis-replica \
  --dbfilename dump.rdb \
  --replicaof localhost 6379
```

### Initial Synchronization

The replica connects to the master and performs the following steps:

```text
Replica                 Master
   |                      |
   |-------- PING -------->|
   |<------- PONG ---------|
   |                      |
   |------ REPLCONF ------>|
   |<-------- OK ----------|
   |                      |
   |-------- PSYNC ------->|
   |<----- FULLRESYNC -----|
   |<----- RDB Data -------|
   |<--- Stream Entries ---|
   |                      |
   |   Live replication   |
   |<----------------------|
```

During initial synchronization, the replica receives the master's ordinary key-value snapshot and stream entries.

After synchronization, the master propagates new supported write commands to the replica.

### Test Live Replication

Write to the master:

```bash
redis-cli -p 6379 SET city Meerut
```

Read from the replica:

```bash
redis-cli -p 6380 GET city
```

Expected result:

```text
"Meerut"
```

### Replication Acknowledgments

The project also implements `WAIT`, which requests acknowledgments from replicas.

In an interactive connection to the master:

```bash
redis-cli -p 6379
```

Execute:

```redis
SET language Java
WAIT 1 1000
```

With one connected replica that acknowledges the write, the expected `WAIT` result is:

```text
(integer) 1
```

`WAIT` does not guarantee that the acknowledged data has been permanently saved to the replica's disk.

## Redis Streams

Streams store ordered entries identified by IDs containing a timestamp and sequence number.

### Add Stream Entries

```bash
redis-cli -p 6379 XADD orders '*' item pizza
redis-cli -p 6379 XADD orders '*' item burger
```

Each command returns the generated stream entry ID.

### Read Stream Entries

```bash
redis-cli -p 6379 XRANGE orders - +
```

This returns the stored entries in stream order.

### Blocking Stream Reads

Open one terminal:

```bash
redis-cli -p 6379 XREAD BLOCK 10000 STREAMS orders '$'
```

While that command waits, open another terminal and add an entry:

```bash
redis-cli -p 6379 XADD orders '*' item sandwich
```

The blocked reader receives the new entry.

**Implementation details:**

- A `ConcurrentHashMap` stores the streams.
- Each stream maintains an ordered collection of entries.
- IDs use a timestamp and sequence number.
- Synchronization protects stream updates.
- `wait()` and `notifyAll()` support blocking reads.

### Stream Replication

New stream entries are propagated from the master to the replica using their generated IDs.

Test:

```bash
redis-cli -p 6379 XADD orders '*' item pasta

redis-cli -p 6380 XRANGE orders - +
```

The replica should contain the new entry with the same ID as the master.

## Transactions

The project supports queuing commands and executing them together using `MULTI` and `EXEC`.

Open an interactive connection:

```bash
redis-cli -p 6379
```

Execute:

```redis
MULTI
SET name Ansh
SET city Meerut
EXEC
```

The commands are queued after `MULTI` and executed when `EXEC` is received.

### Discard a Transaction

```redis
MULTI
SET temporary value
DISCARD
```

`DISCARD` clears the queued commands without executing them.

**Implementation details:**

- Transaction state is maintained separately for each client connection.
- Commands are queued during `MULTI`.
- `EXEC` processes the queued commands in order.
- A shared lock prevents other normal commands from interleaving during transaction execution.

This implementation does not provide SQL-style rollback or the full transaction feature set of production Redis.

## Key Data Structures and Algorithms

| Feature | Data Structure / Algorithm |
|---|---|
| Key-value storage | `ConcurrentHashMap` |
| Key expiration | Expiration timestamp and lazy deletion |
| Command execution | Command pattern and command dispatcher |
| Network communication | TCP sockets and RESP parsing |
| Stream storage | Concurrent map and ordered entry lists |
| Stream IDs | Timestamp and sequence number |
| Blocking reads | `wait()`, `notifyAll()`, and timeout handling |
| Transactions | Per-client command queue and synchronized execution |
| Persistence | Snapshot serialization and deserialization |
| Replication | Initial synchronization, command propagation, and offset tracking |

## Current Limitations

This project focuses on learning Redis internals. It does not implement every production Redis feature.

Current limitations include:

- No automatic master failover.
- No full production-grade replication recovery or partial resynchronization.
- No Append-Only File (AOF) persistence.
- Stream persistence uses a custom file format.
- The implementation supports only a subset of Redis commands and data types.
- Transactions do not support SQL-style rollback.
- Snapshot persistence does not guarantee recovery of every write made after the most recent successful save.
- The implementation has not been validated for production workloads or full Redis protocol compatibility.

## Future Improvements

Possible extensions include:

- Append-Only File (AOF) persistence.
- Active expiration using a background cleanup process.
- Authentication and access control.
- Additional data types such as lists, sets, and sorted sets.
- Replication backlog and partial resynchronization.
- Automatic failover.
- Improved stream indexing and memory management.
- Automated tests, benchmarks, and load testing.

## What I Learned

Building this project helped me understand:

- How a TCP server communicates with multiple clients.
- How Redis commands are encoded and decoded using RESP.
- How concurrent in-memory storage works.
- How TTL-based expiration can be implemented.
- How database snapshots are saved and restored.
- How master–replica synchronization and acknowledgments work.
- How streams support ordered entries and blocking reads.
- How transaction commands can be queued and executed safely.

## Author

**Ansh Dhama**

GitHub: [Ansh-dhama](https://github.com/Ansh-dhama)

LinkedIn: [ansh-dhama-java](https://www.linkedin.com/in/ansh-dhama-java/)
