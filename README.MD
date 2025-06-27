# ethos

> 🧼 **Depissified Node**  
> This software contains no telemetry, no analytics, no phone-home traps, and no compromise.  
> Built under surveillance. Hardened under fire.  
> No pissie survived the pipeline.

**ethos** is a zero-allocation, branch-minimized Ethereum log subscriber engineered for sub-microsecond latency. It connects directly to a WebSocket provider (e.g., Infura), decodes incoming logs in-place, deduplicates events across reorgs, and emits only unique results.

This project was built for speed-critical environments — saturated CPU cores, pinned threads, and direct JSON parsing without a library.

---

## Features

- 🔁 **Reorg-tolerant deduplication ring** (L1 cache–resident, 32-byte slots)  
- 🧠 **Zero-alloc JSON parser** with fused key probes and 8-byte stride scan  
- 🌐 **Direct WebSocket parser** with RFC 6455 masking and custom framing  
- ⚙️ **Manual GC control**, TLS handling, and OS-specific syscall glue (`epoll` or `kqueue`)  
- 🔐 Compatible with [Infura Polygon Mainnet](https://polygon.technology)

---

## Usage

### Prerequisites

- Go 1.21+
- Linux or macOS
- Access to a WebSocket endpoint (e.g., Infura project ID)

### Build

#### macOS (kqueue):
```bash
go build -o ethos_darwin ./main_darwin.go
./ethos_darwin
````

#### Linux (epoll):

```bash
go build -o ethos_linux ./main_linux.go
./ethos_linux
```

---

## Architecture

### Deduplication Ring

```go
type dedupeSlot struct {
  blk, tx, log uint32
  tagHi, tagLo uint64
  age          uint32
}
```

* **Reorg-safe** up to 12 blocks deep
* Fast hashing via 64-bit mixer
* O(1) overwrite with no branching

---

### WebSocket Handling

* Frames decoded **in-place** via `readFrame()`
* RFC 6455 masking applied manually
* Fragmented frames rejected for simplicity and speed

---

### JSON Event Parsing

```go
handleFrame(p []byte)
```

* Scans for 6 keys: `address`, `data`, `topics`, `blockNumber`, `transactionIndex`, `logIndex`
* Extracts slices directly from WebSocket buffer (`wsBuf`)
* Dedupe check applied via:

```go
if deduper.Check(blk32, tx32, log32, v.TagHi, v.TagLo, latestBlk)
```

* Emits logs only if **new** under current canonical block

---

## Performance Notes

* All `[]byte` → `string` coercions are zero-copy (`b2s`)
* All string/number slicing is bounds-safe but allocation-free
* Manual GC triggers based on soft/hard heap thresholds

---

## Constants (tune in `constants.go`)

```go
const (
  ringBits      = 14         // 2^14 = 16K slots
  maxReorg      = 12         // tolerate 12-block forks
  heapSoftLimit = 32 << 20   // 32 MiB
  wsDialAddr    = "polygon-mainnet.infura.io:443"
  ...
)
```

Change these to adapt memory usage, ring size, or endpoint.

---

## Output Format

Example emitted log:

```
[EVENT]
  address   = 0xa49a...
  data      = 0x0000...
  topics    = 0x3734...,0x0000...
  block     = 0x45a9f75
  txIndex   = 0x1
  logIndex  = 0x61
```

Each line is extracted directly from the raw JSON, deduplicated by block/tx/log + tag entropy.

---

## License

MIT — free to use, modify, and optimize. Contributions welcome.
