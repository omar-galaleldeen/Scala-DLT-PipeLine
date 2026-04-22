# DLT PipeLine for a Retail Discount Rules Engine.
Created a Scalable, parallel processing, Declarative Pipeline in vanilla Scala capable of handling millions of transaction in parallel using pure functional programming.
---

## Problem Statement

A large retail store requires a rule-based engine that automatically qualifies order transactions for discounts and calculates the final price. The engine must be scalable, maintainable, and follow strict functional programming principles.

---

## Functional Programming Constraints

All versions of this project follow these rules:

- **No `var`s** — only `val` allowed
- **No mutable data structures** — immutable collections only
- **No loops** — only recursion and higher-order functions (`map`, `flatMap`, `foldLeft`, etc.)
- **Pure functions** — output depends solely on input, no side effects, total functions
- **Functional error handling** — `Try`, `Success`, `Failure` instead of exceptions

---

## Discount Rules

| Qualifying Rule | Calculation |
|----------------|-------------|
| Less than 30 days to expiry (from transaction date) | `(30 - daysRemaining)%` — e.g. 29 days → 1%, 28 days → 2% |
| Product is Cheese | 10% |
| Product is Wine | 5% |
| Sold on March 23rd | 50% |
| Quantity 6–9 units | 5% |
| Quantity 10–14 units | 7% |
| Quantity 15+ units | 10% |
| Sold through App channel | Quantity rounded up to nearest multiple of 5, capped at 25% |
| Payment via Visa | 5% |

**Final discount:** Top 2 qualifying discounts are averaged. If no discount qualifies, the order gets 0%.

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
- No Parallel file processing

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

**What improved:** error handling, structured logging, database persistence
**What it still lacks:** no parallelism, SQLite (single-writer), all data in memory

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

**What improved:** 2 new discount rules added to the scalable rules list
**What it still lacks:** no parallelism, no connection pooling, all data in memory

---

### V4 — Parallel + Logger (`MainV4ParAndLogger.scala`)
**Major performance upgrade:** switched to PostgreSQL, added parallel processing, enhanced logging with task timing.

**Key changes from V3:**

```scala
// Switched from SQLite to PostgreSQL and added a .env file for credentials and production-grade practices.
val dotenv = Dotenv.configure().filename(envFile).load()
val dbUrl  = dotenv.get("DB_URL")
val dbUser = dotenv.get("DB_USER")
val dbPass = dotenv.get("DB_PASS")
```

```scala
// Added parallel processing with .par
val ordersDeducted = orders
  .par                                    // uses all CPU cores
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

**What improved:** parallelism, PostgreSQL, batched inserts, timed logging, credentials hidden in `.env`
**What it still lacks:** entire file still loaded into memory — breaks at 10M rows

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
// One DB connection per chunk — commits every 100K rows (configurable)
def writeChunkToDb(orders: List[Order], chunkNum: Int): Try[Unit] = Try {
  val conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)
  conn.setAutoCommit(false)
  orders.grouped(batchSize).zipWithIndex.foreach { case (batch, batchNum) =>
    insertBatch(conn, batch)
    conn.commit()
    log("INFO", s"Chunk $chunkNum — committed batch $batchNum")
  }
  conn.close()
}
```

**What improved:** handles 10M+ rows without running out of memory, per-chunk connection management, granular progress logging

---

## Memory & Performance Summary

| Version | Memory Usage | Parallelism | DB | Handles Millions of rows? |
|---------|-------------|-------------|-----|-------------------|
| V1 | Full file in RAM | None | None | ❌ |
| V2 | Full file in RAM | None | SQLite | ❌ |
| V3 | Full file in RAM | None | SQLite | ❌ |
| V4 | Full file in RAM | `.par` on all rows | PostgreSQL | ✅ (breaks ~2M) |
| V5 | 1M rows max in RAM | `.par` within chunk | PostgreSQL | ✅ (Easily handles millions) |

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
TIMESTAMP   LOGLEVEL   MESSAGE
2026-04-22T10:31:00Z INFO Rules engine started
2026-04-22T10:31:01Z INFO File opened successfully: src/main/resources/TRX10M.csv
2026-04-22T10:31:01Z INFO Chunk 1 — parsing — started
2026-04-22T10:31:08Z INFO Chunk 1 — parsing — finished in 7.2s
2026-04-22T10:31:08Z INFO Chunk 1 — parsed and processed 1000000 orders
2026-04-22T10:31:08Z INFO Chunk 1 — writing to DB — started
2026-04-22T10:31:12Z INFO Chunk 1 — committed batch 0 (100000 rows so far)
...
2026-04-22T10:45:00Z INFO Rules engine finished
```

---

*Project by: Omar Galal El-Deen — ITI Data Management Track, Functional Programming in Scala*
