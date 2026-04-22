package unenhanced

import scala.io.{Codec, Source}
import java.time._
import java.time.temporal.ChronoUnit

object MainV1Raw extends App {

  // Configuration Variables
  val separator = ","
  val idxTimeStamp = 0
  val idxName = 1
  val idxExpiryDate = 2
  val idxQty = 3
  val idxUnitPrice = 4
  val idxChannel = 5
  val idxPayMeth = 6

  // Business variables
  val nAvg = 2 // number of discounts to avg

  // Time and Date functions
  val todayDate = LocalDate.now()
  val now = LocalDateTime.now()
  val ts = Instant.now()

  // Case class for orders
  case class Order(
    timeStamp: Instant,
    name: String,
    expiryDate: LocalDate,
    qty: Int,
    unitPrice: Double,
    totalPrice: Double,
    channel: String,
    payMethod: String,
    discounts: List[Double],
    discount: Double,
    finalPrice: Double
  )

  // Helpers
  def lineToOrder(line: String): Order = {
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
      discounts  = List(),
      discount   = 0.00,
      finalPrice = parts(idxQty).toInt * parts(idxUnitPrice).toDouble,
    )
  }

  // helper to extract Date from timestamp
  def toDate(instant: Instant): LocalDate =
    instant.atZone(ZoneId.of("UTC")).toLocalDate

// Rules
  // Expiry days discount rule
  def discountExpiry(order: Order): Double = {
    val daysRemaining = ChronoUnit.DAYS.between(toDate(order.timeStamp), order.expiryDate)
    if (daysRemaining < 30) {(30 - daysRemaining) / 100.0}
    else {0.00}
  }
  // Product category discount rule (cheese and wine)
  def discountProductCategory(order: Order): Double = {
    val category = order.name.split("-")(0).trim
    category match {
      case "Cheese" => 0.10
      case "Wine"   => 0.05
      case _        => 0.00
    }
  }
  // Quantity bought discount rule
  def discountQty(order: Order): Double = {
    if      (order.qty >= 6  && order.qty <= 9)  {0.05}
    else if (order.qty >= 10 && order.qty <= 14) {0.07}
    else if (order.qty >= 15) {0.10}
    else  {0.00}
  }
  // Bought in 23rd of March discount rule
  def discountMarch(order: Order): Double = {
    val date = toDate(order.timeStamp)
    if (date.getMonthValue == 3 && date.getDayOfMonth == 23) 0.50
    else {0.00}
  }

// Rule list
  // add new rules here as you sclae
  val rules: List[Order => Double] = List(
    discountExpiry,
    discountProductCategory,
    discountQty,
    discountMarch
  )

  // get order, apply rules, return order with updated discount list
  def applyRules(order: Order, rules: List[Order => Double]): Order = {
    val discounts = rules.map(rule => rule(order))
    order.copy(discounts = discounts)
  }

  // take an order with a list of discounts and return the final discount
  def calcFinalDiscount(order: Order): Order = {
    val topDiscounts = order.discounts.sortBy(-_).take(nAvg)
    val avgDiscount = topDiscounts.sum / topDiscounts.length

    order.copy(
      discount = avgDiscount,
      finalPrice = order.totalPrice * (1 - avgDiscount)
      )
  }


// Pipeline
  // file reader
  def readFile(fileName: String, codec: String = Codec.default.toString): List[String] = {
    Source.fromFile(fileName, codec).getLines().toList.tail
  }

  // reading the file
  val ordersFilePath: String = "src/main/resources/TRX1000.csv"

  // applying transformations
  val ordersDeducted = readFile(ordersFilePath)
    .map(lineToOrder)
    .map(order => applyRules(order, rules))
    .map(order => calcFinalDiscount(order))

  ordersDeducted.foreach(println)
}