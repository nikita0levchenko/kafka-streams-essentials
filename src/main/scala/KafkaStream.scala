import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

object KafkaStream extends App {
  implicit def serde[A >: Null: Decoder: Encoder]: Serde[A] = {
    val serializer: A => Array[Byte] = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) =>
      decode[A](new String(bytes)).toOption

    Serdes.fromFn[A](serializer, deserializer)
  }

  // topology
  val builder: StreamsBuilder = new StreamsBuilder()

  import Domain._
  import Topics._

  //KStream
  val usersWithOrders: KStream[UserId, Order] =
    builder.stream[UserId, Order](OrdersByUserTopic)

  //KTable
  val userProfilesTable: KTable[UserId, Profile] =
    builder.table[UserId, Profile](DiscountProfilesByUserTopic)

  //GlobalKTable
  val discountProfilesGTable: GlobalKTable[Profile, Discount] =
    builder.globalTable[Profile, Discount](DiscountsTopic)

  // KStream/KTable transformations: filter, map, flatMap, mapWithValue, flatMapWithValue

  //filtering
  val expensiveOrders: KStream[UserId, Order] =
    usersWithOrders.filter((userId, order) => order.amount > 1000.00)

  // simple maping
  val listOfProducts: KStream[UserId, List[Product]] =
    usersWithOrders.mapValues(_.products)

  // flatMapping
  val flatListOfProducts: KStream[UserId, Product] =
    usersWithOrders.flatMapValues(_.products)

  // joins
  val ordersWithUsersProfiles: KStream[UserId, (Order, Profile)] =
    usersWithOrders.join(userProfilesTable) { (userId, order) =>
      (userId, order)
    }

  val ordersWithDiscount: KStream[UserId, Order] =
    ordersWithUsersProfiles.join(discountProfilesGTable)(
      { case (userId, (order, profile)) =>
        profile
      }, // key of the join - picked from the left stream
      { case ((order, profile), discount) =>
        order.copy(amount = order.amount * (1.0 - discount.amount))
      } // values of the matched records
    )

  // pick another identifiers
  val ordersStream: KStream[OrderId, Order] =
    ordersWithDiscount.selectKey((userId, order) => order.orderId)

  val paymentStream: KStream[OrderId, Payment] =
    builder.stream[OrderId, Payment](PaymentsTopic)

  // find orders with payments
  val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

  val joinOrdersPayment = (order: Order, payment: Payment) =>
    if (payment.status == "PAID") Option(order) else Option.empty[Order]

  val ordersWithPayments = ordersStream
    .join(paymentStream)(joinOrdersPayment, joinWindow)
    .flatMapValues(mayBeOrders => mayBeOrders.toIterable)

  // sink / sink processor / output
  ordersWithPayments.to(PaidOrdersTopic)

  val topology = builder.build()

  // setup the application properties
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
    Serdes.stringSerde.getClass
  )

  val application = new KafkaStreams(topology, props)
  application.start()

}
