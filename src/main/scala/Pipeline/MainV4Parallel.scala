package Pipeline

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time._
import java.time.temporal.ChronoUnit
import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try}

object MainV4Parallel extends App {

  // Configuration Variables
  val separator = ","
  val idxTimeStamp = 0
  val idxName = 1
  val idxExpiryDate = 2
  val idxQty = 3
  val idxUnitPrice = 4
  val idxChannel = 5
  val idxPayMeth = 6

  // file paths:
  val ordersFilePath = "src/main/resources/TRX1000.csv"
  val dbPath = "database/orders.db"
  val logPath = "logs/rules_engine.log"

  // Business variables
  val nAvg = 2 // number of top discounts to average

  // Initializing
  new File("logs").mkdirs() // create logs folder if it doesn't exist
  new File("database").mkdirs() // create folder if it doesn't exist

// ============================================================
// Classes
// ============================================================
  // Case class for orders
  case class Order(
    timeStamp:  Instant,
    name:       String,
    expiryDate: LocalDate,
    qty:        Int,
    unitPrice:  Double,
    totalPrice: Double,
    channel:    String,
    payMethod:  String,
    discounts:  List[Double], // a list of all the calculated discounts
    discount:   Double,       // the final discount after averaging
    finalPrice: Double        // = totalPrice - finalDiscount
  )

// ============================================================
// LOGGING
// ============================================================
  // Note: used claude for this
  def log(level: String, message: String): Unit = {
    val logLine = s"${Instant.now()} $level $message\n"
    val fw = new BufferedWriter(new FileWriter(logPath, true)) // append mode
    fw.write(logLine)
    fw.close()
  }


// ============================================================
// Helpers
// ============================================================

  // helper to extract LocalDate from Instant
  def toDate(instant: Instant): LocalDate =
    instant.atZone(ZoneId.of("UTC")).toLocalDate

  // parse a single line into an Order, wrapped in Try for safety
  def lineToOrder(line: String): Try[Order] = Try {
    val parts = line.split(separator)
    Order(
      timeStamp  = Instant.parse(parts(idxTimeStamp)),
      name       = parts(idxName),
      expiryDate = LocalDate.parse(parts(idxExpiryDate)),
      qty        = parts(idxQty).toInt,
      unitPrice  = parts(idxUnitPrice).toDouble,
      totalPrice = parts(idxQty).toInt * parts(idxUnitPrice).toDouble,
      channel    = parts(idxChannel),
      payMethod  = parts(idxPayMeth),
      discounts  = List(), // empty list in the beginning
      discount   = 0.00,   // zero in the beginning
      finalPrice = parts(idxQty).toInt * parts(idxUnitPrice).toDouble // same as total
    )
  }

  // read file, wrapped in Try for safety
  def readFile(fileName: String, codec: String = Codec.default.toString): Try[List[String]] =
    Try(Source.fromFile(fileName, codec).getLines().toList.tail)

// ============================================================
// DISCOUNT RULES
// ============================================================

  // Expiry days discount: < 30 days remaining -> (30 - days)% discount
  def discountExpiry(order: Order): Double = {
    val daysRemaining = ChronoUnit.DAYS.between(toDate(order.timeStamp), order.expiryDate)
    if (daysRemaining < 30) {(30 - daysRemaining) / 100.0}
    else {0.00}
  }

  // Product category discount:
  def discountProductCategory(order: Order): Double = {
    val category = order.name.split("-")(0).trim
    category match {
      case "Cheese" => {0.10}
      case "Wine"   => {0.05}
      case _        => {0.00}
    }
  }

  // Quantity discount:
  def discountQty(order: Order): Double = {
    if (order.qty >= 6  && order.qty <= 9)       {0.05}
    else if (order.qty >= 10 && order.qty <= 14) {0.07}
    else if (order.qty >= 15)                    {0.10}
    else                                         {0.00}
  }

  // March 23rd special discount:
  def discountMarch(order: Order): Double = {
    val date = toDate(order.timeStamp)
    if (date.getMonthValue == 3 && date.getDayOfMonth == 23) {0.50}
    else {0.00}
  }

  // App discount
  def discountApp(order: Order): Double = {
    if (order.channel == "App") {
      val discount: Double = Math.ceil(order.qty / 5) * 5 / 100.00
      if (discount >= 0.25 ) {0.25}
      // Note to Eng Youssef: I don't think a company should allow a discount more than this
      else {discount}
    }
    else {0.00}
  }

  // Visa discount
  def discountVisa(order: Order): Double = {
     order.payMethod match {
      case "Visa" => {0.05}
      case _        => {0.00}
    }
  }

// ============================================================
// RULE LIST
// ============================================================
  // add new rules here as you scale the pipeline
  val rules: List[Order => Double] = List(
    discountExpiry,
    discountProductCategory,
    discountQty,
    discountMarch,
    discountApp,
    discountVisa

  )

  // apply all rules to an order, store results in discounts list
  def applyRules(order: Order, rules: List[Order => Double]): Order = {
    val discounts = rules.map(rule => rule(order)) // take each rule and apply it to the order
    order.copy(discounts = discounts)
  }

  // take top nAvg discounts and average them
  def calcFinalDiscount(order: Order): Order = {
    val topDiscounts = order.discounts.sortBy(-_).take(nAvg)
    val avgDiscount  = topDiscounts.sum / topDiscounts.length
    // return the order with the final discount and the final price
    order.copy(
      discount   = avgDiscount,
      finalPrice = order.totalPrice * (1 - avgDiscount)
    )
  }

  // ============================================================
  // DATABASE: Loading
  // ============================================================
  // create table if it doesn't exist
  def initDb(conn: Connection): Unit = {
    val sql =
      """CREATE TABLE IF NOT EXISTS orders (
        |  timestamp   TEXT,
        |  name        TEXT,
        |  expiry_date TEXT,
        |  qty         INTEGER,
        |  unit_price  REAL,
        |  total_price REAL,
        |  channel     TEXT,
        |  pay_method  TEXT,
        |  discount    REAL,
        |  final_price REAL
        |)""".stripMargin
    conn.createStatement().execute(sql)
  }

  // insert a single order into the database
  def insertOrder(conn: Connection, order: Order): Unit = {
    val sql =
      """INSERT INTO orders
        |(timestamp, name, expiry_date, qty, unit_price, total_price, channel, pay_method, discount, final_price)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
    val stmt: PreparedStatement = conn.prepareStatement(sql)
    stmt.setString(1, order.timeStamp.toString)
    stmt.setString(2, order.name)
    stmt.setString(3, order.expiryDate.toString)
    stmt.setInt   (4, order.qty)
    stmt.setDouble(5, order.unitPrice)
    stmt.setDouble(6, order.totalPrice)
    stmt.setString(7, order.channel)
    stmt.setString(8, order.payMethod)
    stmt.setDouble(9, order.discount)
    stmt.setDouble(10, order.finalPrice)
    stmt.execute()
  }

  // write all orders to the database in a single transaction for performance
  def writeToDb(orders: List[Order]): Try[Unit] = Try {
    Class.forName("org.sqlite.JDBC")
    val conn = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    initDb(conn)
    conn.setAutoCommit(false)                          // disable auto commit
    orders.foreach(order => insertOrder(conn, order))  // all inserts in one transaction
    conn.commit()                                      // commit all at once
    conn.close()
  }

  // ============================================================
  // PIPELINE
  // ============================================================
  log("INFO", "Rules engine started")

  readFile(ordersFilePath) match {
    case Failure(e) =>
      log("ERROR", s"Failed to read file: ${e.getMessage}")

    case Success(lines) =>
      log("INFO", s"Successfully read ${lines.length} lines from $ordersFilePath")

      // parse lines, skip any that fail
      val orders = lines.flatMap { line =>
        lineToOrder(line) match {
          case Success(order) => Some(order)
          case Failure(e)     =>
            log("WARN", s"Failed to parse line: $line — ${e.getMessage}")
            None
        }
      }
      log("INFO", s"Successfully parsed ${orders.length} orders")

      // apply discount rules
      val ordersDeducted = orders
        .map(order => applyRules(order, rules))
        .map(order => calcFinalDiscount(order))
      log("INFO", "Discount rules applied to all orders")

      // write to database
      writeToDb(ordersDeducted) match {
        case Success(_) => log("INFO", s"Successfully wrote ${ordersDeducted.length} orders to $dbPath")
        case Failure(e) => log("ERROR", s"Failed to write to database: ${e.getMessage}")
      }

      // for testing
      //ordersDeducted.take(10).foreach(println)
  }

  log("INFO", "Rules engine finished")
}