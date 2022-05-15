object Domain {
  type UserId = String
  type Profile = String
  type Product = String
  type OrderId = String

  case class Order(
      orderId: OrderId,
      user: UserId,
      products: List[Product],
      amount: Double
  )
  case class Discount(profile: Profile, amount: Double) // in percentage points
  case class Payment(orderId: OrderId, status: String)
}
