# Reverse Engineering a Log-Structured Storage Engine 🧠💾

This project is a personal exploration of **database internals** inspired by the book [Designing Data-Intensive Applications (DDA)](https://dataintensive.net/) by Martin Kleppmann. It documents my process of **rebuilding a key-value storage engine** from scratch using Go, while digging into the low-level operations that power databases like Bitcask and LevelDB.

---

## 📚 Learning Goals

The main purpose of this project was **not just to build**, but to **understand**:

- How databases handle **disk I/O**
- What makes a database **durable**, **concurrent**, and **crash-safe**
- How modern key-value stores like **Bitcask** manage data files
- The role of **Write-Ahead Logging (WAL)**, **indexing**, and **file formats**

---

## ✅ Achievements

### 📁 Log-Structured Design

I implemented a **log-structured** approach where all writes are **appended** to a data file, similar to Bitcask. This makes write operations fast and naturally supports crash recovery via WAL replay.

---

### 🔁 Write-Ahead Log (WAL)

I explored and designed a WAL that:

- Stores key/value write operations as append-only entries
- Can be replayed on crash to rebuild the in-memory index
- Uses basic formatting principles: `length → op → key → value`
- Supports fsync and batched sync for durability

---

### 📍 Byte Offsets and File Seeking

- Learned how to track and store **byte offsets** for each key-value entry
- Rebuilt an **in-memory key directory** during startup by scanning the file and tracking offsets
- Understood how `io.Reader.ReadBytes('\n')` updates file cursor positions

---

### 🔒 Write Serialization

I implemented **concurrency-safe writes** using multiple strategies:

1. **Buffered Channels + Write Goroutine**

   - A dedicated goroutine owns all writes
   - Write requests are queued into a buffered channel and processed sequentially
   - Ensures thread-safe file access and clean separation of IO

2. **sync.RWMutex**
   - Blocks writes during reads to guarantee consistent views
   - Explored its pros and cons compared to message passing

---

### 💡 OS-Level Syncing

- Learned how OS handles buffered writes vs flushing:
  - `fsync()` — forces flush from OS page cache to disk
  - `O_SYNC` — bypasses buffering, syncing on every write
- Understood trade-offs between durability and performance
- Experimented with `O_SYNC` mode to enforce crash-safety without manual `fsync`

---

### 🔍 Indexing + Recovery

- Explored how Bitcask uses a **keydir** (in-memory index) and **hint files** (segment-level metadata)
- Understood startup strategies:
  - Scan full log to rebuild index
  - Use hint files to shortcut recovery
- Learned how partial indexing works and how merge compaction affects recovery

---

## 📖 Reading Status

I’m reading _Designing Data-Intensive Applications_ alongside this project, currently on **Chapter 3: Storage and Retrieval**. The concepts from the book heavily influence the architecture and design choices I make.

---

## 💻 Technologies

- **Language**: Go
- **File IO**: `os`, `io`, `bufio`, direct syscalls
- **Concurrency**: `sync`, channels, goroutines
- **Low-Level Concepts**: Byte offsets, file descriptors, `O_SYNC`, write locks

---

## 🧠 Reflections

> _“It’s easier to build a system from scratch than optimize an existing one.”_

This project proves that point. Understanding how every byte is written, read, or synced has given me more confidence to approach real-world databases. Instead of relying on black-box tools, I now have a mental model of how they actually work.

---

## 🚧 Future Work

- Merge compaction logic
- TTL and deletion support
- Custom WAL entry formats with checksums
- Snapshotting keydir to disk
- Benchmarking sync strategies (fsync vs O_SYNC vs batching)

---

## 🗂️ Project Structure (Simplified)

```bash
.
├── main.go
├── storage/
├── config/
│   ├── segment.go       # Segment Configurations e.g Maximum segment size
├── core/
│   ├── Lss.go           # WAL write & replay logic
│   ├── Hashtable.go     # In-memory keydir
│   ├── Segment.go
└── README.md
```
