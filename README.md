# DLT PipeLine for a Retail Discount Rules Engine.

Created a Scalable, parallel processing, Declarative Pipeline in vanilla Scala capable of handling millions of transaction in parallel using pure functional programming.

---

## Problem Statement

A large retail store requires a rule-based engine that automatically qualifies order transactions for discounts and calculates the final price. The engine must be scalable, maintainable, and follow strict functional programming principles.

---

## Functional Programming Constraints

All versions of this project follow these rules:

- **No `var`** — only `val` allowed
- **No mutable data structures** — immutable collections only
- **No loops** — only recursion and higher-order functions (`map`, `flatMap`, `foldLeft`, etc.)
- **Pure functions** — output depends solely on input, no side effects, total functions
- **Functional error handling** — `Try`, `Success`, `Failure` instead of exceptions

---

## Discount Rules

| Qualifying Rule | Calculation |
|----------------|-------------|
| __Expiry Date__: Less than 30 days to expiry (from transaction date) | `(30 - daysRemaining)%` — e.g. 29 days → 1%, 28 days → 2% |
| __Product Category__: Cheese & Wine | 10% & 5% respectively |
| __Lucky Date__: Sold on March 23rd | 50% |
| __Bulk Purchase__: Quantity 6–9 units | 5% |
| __Bulk Purchase__: Quantity 10–14 units | 7% |
| __Bulk Purchase__: Quantity 15+ units | 10% |
| __App Promo__: Sold through App channel | Quantity rounded up to nearest multiple of 5, capped at 25% |
| __Card Promo__: Payment via Visa | 5% |

---

## Final Discount

Top 2 qualifying discounts are averaged. If no discount qualifies, the order gets 0%.

---

## Project Architecture

```
CSV File
   │
   ▼
readFile()          ← lazy Iterator, low memory
   │
   ▼
lineToOrder()       ← parse each line into an Order case class
   │
   ▼
applyRules()        ← apply all discount rules, store results in discounts list
   │
   ▼
calcFinalDiscount() ← sort descending, take top 2, average
   │
   ▼
writeChunkToDb()    ← batch insert to PostgreSQL, commit every 100K rows
   │
   ▼
log()               ← every step logged to file + console with timestamps
```

---

## Version History

### V1 — Raw (`MainV1Raw.scala`)
**The baseline.** A simple, working proof of concept.

```scala
val ordersDeducted = readFile(ordersFilePath)
  .map(lineToOrder)
  .map(order => applyRules(order, rules))
  .map(order => calcFinalDiscount(order))

ordersDeducted.foreach(println)
```

**What it does:**
- Reads the file into a `List[String]` all at once
- Loads entire file into memory
- Parses and applies rules sequentially
- Prints results to console — no DB, no logging

**What it lacks:**
- No error handling — crashes on bad data
- No logging
- No database output
- No parallel file processing

---

### V2 — Enhanced (`MainV2Enhanced.scala`)
**Added production features:** error handling, logging, and SQLite database output.

**Key changes from V1:**

```scala
// V1: crashes on bad data
def lineToOrder(line: String): Order = { ... }

// V2: wraps in Try — skips bad rows safely
def lineToOrder(line: String): Try[Order] = Try { ... }
```

```scala
// V2: Added logging
def log(level: String, message: String): Unit = {
  val fw = new BufferedWriter(new FileWriter(logPath, true))
  fw.write(s"${Instant.now()} $level $message\n")
  fw.close()
}
```

```scala
// V2: Added SQLite output with single transaction
def writeToDb(orders: List[Order]): Try[Unit] = Try {
  Class.forName("org.sqlite.JDBC")
  val conn = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
  conn.setAutoCommit(false)
  orders.foreach(order => insertOrder(conn, order))
  conn.commit()
  conn.close()
}
```

**What improved:**
- Error handling, structured logging, database persistence.

**What it still lacks:**
- No parallelism, SQLite (single-writer), all data in memory.

---

### V3 — New Rules (`MainV3NewRules.scala`)
**Added the App and Visa discount rules** following new business requirements.

**Key changes from V2:**

```scala
// App discount: quantity rounded up to nearest multiple of 5
def discountApp(order: Order): Double = {
  if (order.channel == "App") {
    val discount: Double = Math.ceil(order.qty / 5.0) * 5 / 100.0
    if (discount >= 0.25) 0.25   // capped at 25%
    else discount
  }
  else 0.00
}

// Visa payment discount
def discountVisa(order: Order): Double = {
  order.payMethod match {
    case "Visa" => 0.05
    case _      => 0.00
  }
}
```

**What improved:**
- 2 new discount rules added to the scalable rules list.

**What it still lacks:**
- No parallelism, no connection pooling, all data in memory.

---

### V4 — Parallel + Logger (`MainV4ParAndLogger.scala`)
**Major performance upgrade:** switched to PostgreSQL, added parallel processing, enhanced logging with task timing.

**Key changes from V3:**

```scala
// Switched from SQLite to PostgreSQL — added .env file for production-grade credential management
val dotenv = Dotenv.configure().filename(envFile).load()
val dbUrl  = dotenv.get("DB_URL")
val dbUser = dotenv.get("DB_USER")
val dbPass = dotenv.get("DB_PASS")
```

```scala
// Added parallel processing with .par — uses all available CPU cores
val ordersDeducted = orders
  .par
  .map(order => applyRules(order, rules))
  .map(order => calcFinalDiscount(order))
  .toList
```

```scala
// Added timed() helper for step-level performance logging
def timed[A](stepName: String)(block: => A): A = {
  val start = System.currentTimeMillis()
  log("INFO", s"$stepName — started")
  val result = block
  val elapsed = (System.currentTimeMillis() - start) / 1000.0
  log("INFO", s"$stepName — finished in ${elapsed}s")
  result
}
```

```scala
// Switched from insertOrder (one PreparedStatement per row)
// to insertBatch (one PreparedStatement per batch, addBatch/executeBatch)
def insertBatch(conn: Connection, batch: List[Order]): Unit = {
  val stmt = conn.prepareStatement(sql)
  batch.foreach { order =>
    stmt.setString(1, order.timeStamp.toString)
    // ...
    stmt.addBatch()      // queue instead of execute immediately
  }
  stmt.executeBatch()    // one round trip for the whole batch
  stmt.close()
}
```

**What improved:**
- Parallelism, PostgreSQL, batched inserts, timed logging, credentials hidden in `.env`.

**What it still lacks:**
- Entire file still loaded into memory — breaks at 10M rows.

---

### V5 — Final (`Main.scala`)
**Solved the memory problem** for 10M+ rows using chunked streaming with parallelism within each chunk.

**Key changes from V4:**

```scala
// readFile now returns a lazy Iterator — not a List
// Never loads the full file into memory
def readFile(...): Try[Iterator[String]] =
  Try(Source.fromFile(fileName, codec).getLines().drop(1))
```

```scala
// Process the file in 1M row chunks sequentially
// Within each chunk: .par for CPU parallelism
// Within each chunk's DB write: commit every 100K rows
lines
  .grouped(chunkSize)           // 1M rows at a time — lazy
  .zipWithIndex
  .foreach { case (chunk, chunkIdx) =>

    val orders = chunk.toList   // materialize only 1M rows
      .par                      // parallel rule application
      .flatMap(...)
      .map(order => applyRules(order, rules))
      .map(order => calcFinalDiscount(order))
      .toList

    writeChunkToDb(orders, chunkNum)  // one connection per chunk
  }
```

```scala
// One DB connection per chunk — commits every 100K rows
// zipWithIndex replaces var counter — stays purely functional
def writeChunkToDb(orders: List[Order], chunkNum: Int): Try[Unit] = Try {
  val conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)
  conn.setAutoCommit(false)
  orders.grouped(batchSize).zipWithIndex.foreach { case (batch, batchIdx) =>
    insertBatch(conn, batch)
    conn.commit()
    log("INFO", s"Chunk $chunkNum — committed batch $batchIdx")
  }
  conn.close()
}
```

**What improved:**
- Handles 10M+ rows without running out of memory, per-chunk connection management, granular progress logging.

---

## Memory & Performance Summary

| Version | Memory Usage | Parallelism | DB | Handles Millions of rows? |
|---------|-------------|-------------|-----|-------------------|
| V1 | Full file in RAM | None | None | ❌ |
| V2 | Full file in RAM | None | SQLite | ❌ |
| V3 | Full file in RAM | None | SQLite | ❌ |
| V4 | Full file in RAM | `.par` on all rows | PostgreSQL | ✅ (breaks ~2M) |
| V5 | 1M rows max in RAM | `.par` within chunk | PostgreSQL | ✅ (handles millions) |

---

## Scalability Design

```
File (10M rows)
├── Chunk 1 (1M rows)
│     ├── .par → apply rules → calcFinalDiscount
│     └── DB write → commit every 100K → close connection
├── Chunk 2 (1M rows)
│     ├── .par → apply rules → calcFinalDiscount
│     └── DB write → commit every 100K → close connection
└── Chunk N ...
```

Peak memory at any point: **~1M rows + processing overhead** — regardless of file size.

---

## Final Version — Deep Walkthrough (`Main.scala`)

This section walks through every design decision in the final version, explaining the **what**, the **why**, and the **how** of each component.

---

### 1. Configuration Block — Single Source of Truth

```scala
val nAvg      = 2         // number of top discounts to average
val chunkSize = 1000000   // number of rows per chunk (1M) — processed sequentially
val batchSize = 100000    // number of rows per DB commit (100K)
```

All tunable parameters live at the very top of the file. To change behavior — batch size, chunk size, number of averaged discounts — you change **one number in one place**. No magic numbers scattered through the code.

---

### 2. Credentials via `.env` — Industry Standard Security

```scala
val dotenv = Dotenv.configure().filename(envFile).load()
val dbUrl  = dotenv.get("DB_URL")
val dbUser = dotenv.get("DB_USER")
val dbPass = dotenv.get("DB_PASS")
```

Database credentials are **never hardcoded**. They are read from a `.env` file that is excluded from version control via `.gitignore`. A `.env.example` template is committed instead, so teammates know what variables are needed without exposing real credentials. This is the industry standard approach used in production systems.

---

### 3. The `Order` Case Class — Immutable Data Envelope

```scala
case class Order(
    timeStamp:  Instant,      // exact moment of transaction (UTC)
    name:       String,       // product name — used to extract category
    expiryDate: LocalDate,    // product expiry — used for expiry discount
    qty:        Int,          // quantity — used for bulk and App discounts
    unitPrice:  Double,
    totalPrice: Double,       // = qty * unitPrice, computed at parse time
    channel:    String,       // "App", "Store", etc.
    payMethod:  String,       // "Visa", "Cash", etc.
    discounts:  List[Double], // all computed discount values — one per rule
    discount:   Double,       // final averaged discount
    finalPrice: Double        // = totalPrice * (1 - discount)
)
```

The `Order` case class acts as the **data envelope** that flows through the entire pipeline. It starts with empty `discounts`, `discount = 0.0`, and `finalPrice = totalPrice` at parse time — then gets progressively enriched at each stage. Since case classes are immutable, every transformation returns a **new** `Order` via `.copy()`. The original is never mutated.

---

### 4. `timed()` — A Generic Transparent Timer

```scala
def timed[A](stepName: String)(block: => A): A = {
  val start   = System.currentTimeMillis()
  log("INFO", s"$stepName — started")
  val result  = block         // execute whatever was passed in
  val elapsed = (System.currentTimeMillis() - start) / 1000.0
  log("INFO", s"$stepName — finished in ${elapsed}s")
  result                      // return result completely unchanged
}
```

`timed` is a **generic higher-order function** — it accepts any block of code as input, wraps it with timing logs, and returns the result exactly as if `timed` wasn't there. The `[A]` type parameter means it works with any return type — `List[Order]`, `Unit`, `Try[Unit]`, anything. Usage:

```scala
// wrap any block — timed doesn't care what's inside
val orders = timed("Chunk 1 — parsing") {
  chunk.toList.par.flatMap(...).toList
}
```

This pattern is called a **transparent wrapper** — the wrapped code behaves identically, you just get free timing logs around it.

---

### 5. `readFile()` — Lazy Iterator, Not a List

```scala
def readFile(fileName: String, codec: String = Codec.default.toString): Try[Iterator[String]] =
  Try(Source.fromFile(fileName, codec).getLines().drop(1))  // drop header row
```

`getLines()` returns a **lazy `Iterator`** — it reads one line at a time from disk on demand, never pulling the whole file into memory at once. `.drop(1)` skips the CSV header row. Wrapped in `Try` so a missing or unreadable file returns a `Failure` instead of crashing the engine.

**Why not `.toList`?** On a 10M row file, `.toList` would load ~2–3GB into RAM immediately. The `Iterator` keeps memory near zero — lines only exist in RAM for the instant they are being processed.

---

### 6. Discount Rules — Pure Functions, Scalable by Design

Every rule follows the exact same contract: takes an `Order`, returns a `Double`. No side effects, no shared state, no dependencies on anything outside the function.

```scala
// 29 days remaining → 1%, 1 day remaining → 29%
def discountExpiry(order: Order): Double = {
  val daysRemaining = ChronoUnit.DAYS.between(toDate(order.timeStamp), order.expiryDate)
  if (daysRemaining < 30) (30 - daysRemaining) / 100.0
  else 0.00
}

// quantity 3 → ceil(3/5)*5 = 5% | quantity 6 → ceil(6/5)*5 = 10% | capped at 25%
def discountApp(order: Order): Double = {
  if (order.channel == "App") {
    val discount = Math.ceil(order.qty / 5.0) * 5 / 100.0
    if (discount >= 0.25) 0.25
    else discount
  }
  else 0.00
}
```

The rules are collected into a `List[Order => Double]` — a **list of functions**. This makes the engine open for extension, closed for modification: adding a new rule is a one-line change to the list, and nothing else in the codebase is touched.

---

### 7. `applyRules()` and `calcFinalDiscount()` — Immutable Enrichment

```scala
// Step 1: run every rule, collect all results into the discounts list
def applyRules(order: Order, rules: List[Order => Double]): Order = {
  val discounts = rules.map(rule => rule(order))  // e.g. [0.05, 0.10, 0.00, 0.00, 0.05, 0.05]
  order.copy(discounts = discounts)               // new Order, original untouched
}

// Step 2: pick the top 2, average them, compute final price
def calcFinalDiscount(order: Order): Order = {
  val topDiscounts = order.discounts.sortBy(-_).take(nAvg)   // [0.10, 0.05]
  val avgDiscount  = topDiscounts.sum / topDiscounts.length   // 0.075 = 7.5%
  order.copy(
    discount   = avgDiscount,
    finalPrice = order.totalPrice * (1 - avgDiscount)         // apply to price
  )
}
```

`.copy()` is the functional way to "update" an immutable case class — it creates a **new instance** with only the specified fields changed, everything else preserved exactly as-is. The original `Order` is never modified.

---

### 8. `insertBatch()` — One Network Round Trip Per Batch

```scala
def insertBatch(conn: Connection, batch: List[Order]): Unit = {
  val stmt = conn.prepareStatement(sql)  // compile the SQL query ONCE per batch
  batch.foreach { order =>
    stmt.setString(1, order.timeStamp.toString)
    // ... set all 10 fields ...
    stmt.addBatch()       // queue this row — don't send to DB yet
  }
  stmt.executeBatch()     // send ALL rows in one single network round trip
  stmt.close()
}
```

Without `addBatch/executeBatch`, each row would require its own network round trip to PostgreSQL — 100,000 rows = 100,000 round trips. With JDBC batching, 100,000 rows = **1 round trip**. The `PreparedStatement` is also compiled only once per batch, saving query planning overhead on the database side.

---

### 9. `writeChunkToDb()` — Controlled Commits, No `var` Needed

```scala
def writeChunkToDb(orders: List[Order], chunkNum: Int): Try[Unit] = Try {
  val conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)
  initDb(conn)
  conn.setAutoCommit(false)                      // take manual control of commits
  orders
    .grouped(batchSize)                          // split 1M orders into groups of 100K
    .zipWithIndex                                // track batch number without a var
    .foreach { case (batch, batchIdx) =>
      insertBatch(conn, batch)                   // send 100K rows to PostgreSQL
      conn.commit()                              // flush to disk
      log("INFO", s"Chunk $chunkNum — committed batch $batchIdx")
    }
  conn.close()
}
```

`setAutoCommit(false)` means PostgreSQL won't write to disk after every single row — instead we control exactly when to commit. Every 100K rows gives us:
- **Performance** — fewer disk flushes, PostgreSQL can optimize each write
- **Safety** — if something fails mid-chunk, only the current 100K batch is lost, not everything
- **Visibility** — we log exactly how many rows are safely written at every checkpoint

`zipWithIndex` replaces what would otherwise be a `var batchNum = 0` counter — keeping the code 100% functional with no mutable state, satisfying the FP constraint.

---

### 10. The Pipeline — Everything Working Together

```scala
readFile(ordersFilePath) match {
  case Failure(e) => log("ERROR", s"Failed to read file: ${e.getMessage}")

  case Success(lines) =>
    timed("Full pipeline") {
      lines
        .grouped(chunkSize)          // (1) read 1M lines at a time — disk stays lazy
        .zipWithIndex                // (2) track chunk number for logging
        .foreach { case (chunk, chunkIdx) =>

          val chunkNum = chunkIdx + 1

          val orders = timed(s"Chunk $chunkNum — parsing") {
            chunk.toList             // (3) pull 1M lines into RAM — only moment we materialize
              .par                   // (4) distribute work across all CPU cores
              .flatMap { line =>
                lineToOrder(line).toOption  // (5) parse each line, skip any that fail
              }
              .map(applyRules(_, rules))    // (6) run all 6 rules in parallel
              .map(calcFinalDiscount)       // (7) average top 2 discounts in parallel
              .toList                       // (8) collect results back to List for DB write
          }

          timed(s"Chunk $chunkNum — writing to DB") {
            writeChunkToDb(orders, chunkNum) match {
              case Success(_) => log("INFO", s"Chunk $chunkNum — done, ${orders.length} orders written")
              case Failure(e) => log("ERROR", s"Chunk $chunkNum — failed: ${e.getMessage}")
            }
          }
          // (9) chunk goes out of scope here
          //     garbage collector frees the 1M rows from RAM
          //     next iteration reads the next 1M rows fresh from disk
        }
    }
}
```

Steps 4–7 run **in parallel** within each chunk via `.par`, using all available CPU cores simultaneously. The outer `foreach` over chunks is **sequential** — one chunk fully completes before the next begins. This gives parallelism where it counts while keeping peak RAM bounded to one chunk at a time.

---

### 11. Memory Profile During Execution

```
Time →

RAM  ↑
     │    ┌──────────┐              ┌──────────┐              ┌──────────┐
 1M  │    │  Chunk 1 │              │  Chunk 2 │              │  Chunk 3 │
rows │    │  in RAM  │              │  in RAM  │              │  in RAM  │
     │    │ .par+DB  │              │ .par+DB  │              │ .par+DB  │
  0  │────┘          └──────────────┘          └──────────────┘          └──
                  GC frees                  GC frees
                  Chunk 1                   Chunk 2
```

Each chunk is processed and written to the DB, then goes out of scope and is garbage collected before the next chunk loads. **Peak RAM never exceeds approximately one chunk worth of data — regardless of how large the total file is.**

---

## Adding New Rules

The rules engine is designed to scale. To add a new discount rule:

1. Write a function `def discountXxx(order: Order): Double`
2. Add it to the `rules` list — nothing else changes

```scala
val rules: List[Order => Double] = List(
  discountExpiry,
  discountProductCategory,
  discountQty,
  discountMarch,
  discountApp,
  discountVisa,
  discountYourNewRule   // ← just add it here!
)
```

---

## Project Setup

**Prerequisites:** Java 8+, Scala 2.13, SBT, PostgreSQL

**1. Clone the repo and create your `.env` file:**
```
DB_URL=jdbc:postgresql://localhost:5432/orders_db
DB_USER=your_user
DB_PASS=your_password
```

**2. Run:**
```bash
sbt run
```

**3. Switch between sample and full dataset** by uncommenting the relevant line:
```scala
//val ordersFilePath = "src/main/resources/TRX1000.csv"   // sample
//val ordersFilePath = "src/main/resources/TRX1M.csv"     // 1M rows
val ordersFilePath = "src/main/resources/TRX10M.csv"      // 10M rows
```

---

## Dependencies (`build.sbt`)

```scala
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
libraryDependencies += "org.postgresql"          %  "postgresql"                 % "42.7.3"
libraryDependencies += "io.github.cdimascio"     %  "dotenv-java"               % "2.3.2"
```

---

## Log Format

All events are logged to `logs/rules_engine.log` in the following format:

```
TIMESTAMP                    LOGLEVEL   MESSAGE
2026-04-22T10:31:00.000Z     INFO       Rules engine started
2026-04-22T10:31:00.012Z     INFO       Processing file: src/main/resources/TRX10M.csv
2026-04-22T10:31:00.015Z     INFO       File opened successfully
2026-04-22T10:31:00.016Z     INFO       Full pipeline — started
2026-04-22T10:31:00.017Z     INFO       Chunk 1 — started
2026-04-22T10:31:00.018Z     INFO       Chunk 1 — parsing — started
2026-04-22T10:31:07.420Z     INFO       Chunk 1 — parsing — finished in 7.4s
2026-04-22T10:31:07.421Z     INFO       Chunk 1 — parsed and processed 1000000 orders
2026-04-22T10:31:07.422Z     INFO       Chunk 1 — writing to DB — started
2026-04-22T10:31:10.100Z     INFO       Chunk 1 — committed batch 0 (100000 rows so far)
2026-04-22T10:31:12.800Z     INFO       Chunk 1 — committed batch 1 (200000 rows so far)
...
2026-04-22T10:45:00.000Z     INFO       Rules engine finished
```

---

__Project by:__ Omar Galal El-Deen — ITI Data Management Track, Functional Programming in Scala.
