# RepCRec-SSI
Serializable Snapshot Isolation (SSI) version of the Replicated Concurrency Control and Recovery project for the Advanced Database class at NYU.

## Features

- **Snapshot Isolation (SI)**: each transaction reads from a consistent snapshot taken at `begin(T)` time.
- **Serializable Snapshot Isolation (SSI)**: builds a serialization graph on commit and aborts transactions that would introduce cycles, ensuring full serializability.
- **Available-Copies replication**:
  - Even-indexed variables (`x2, x4, …`) are replicated at all sites.
  - Odd-indexed variables (`x1, x3, …`) live at a single site `((index % 10) + 1)`.
- **Site failure and recovery**:
  - Transactions that already wrote to a site are aborted when that site fails.
  - Recovery follows the project spec: odd variables become readable immediately; even (replicated) variables only become readable after a committed write at that site.
- **Read-your-writes**: transactions always see their own buffered writes, even before commit.
- **Global transaction manager and per-site data manager** (no external web server, no lock table).
- **Configurable number of sites and variables** via `config.py` or command-line flags.
- **Clean, modular Python implementation** with explicit SI/SSI logic and Available-Copies semantics.

## Running
`python -m RepCRec.start test/test01.in`
or with output options:
```bash
python -m RepCRec.start test/test01.in -o output.log
```
- file_path: path to the input file.
- -o / --out-file (optional): log file; if omitted, logs go to stdout.
- -i / --stdin (flag): read instructions from stdin instead of a file.

## Test
The project includes a collection of test inputs (e.g., test/testXX.in) that exercise:

- Site failure and recovery behavior.
- Available-Copies semantics.
- SI and SSI behavior on overlapping reads/writes.
- Edge cases such as partial failures, delayed recovery, and multi-site replication.
