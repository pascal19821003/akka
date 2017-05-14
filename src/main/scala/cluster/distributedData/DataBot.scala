package cluster.distributedData

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


/*
An actor that schedules tick messages to itself and for each tick adds or removes elements from ORSet.
It also subscribes to changes of this
 */
object DataBot {
  private case object Tick
}

class DataBot extends Actor with ActorLogging {
  import DataBot._

  val replicator = DistributedData(context.system).replicator
  implicit val node = Cluster(context.system)

//  import context.dispatcher
  val tickTask = context.system.scheduler.schedule(5 seconds, 5 seconds, self, Tick)

  val DataKey = ORSetKey[String]("key")

  replicator ! Subscribe(DataKey, self)

  val Counter1Key = PNCounterKey("counter1")
  val Set1Key = GSetKey[String]("set1")
  val Set2Key = ORSetKey[String]("set2")
  val ActiveFlagKey = FlagKey("active")

  val writeTo3 = WriteTo(n = 3, timeout = 1 second)
  replicator ! Update(Set1Key, GSet.empty[String], writeTo3)(_ + "hello")

  val writeAll = WriteAll(timeout = 5 seconds)
  replicator ! Update(ActiveFlagKey, Flag.empty, writeAll)(_.switchOn)

  replicator ! Get(Counter1Key, ReadLocal)

  val readMajority = ReadMajority(timeout = 5 seconds)
  val writeMajority = WriteMajority(timeout = 3 seconds)
  replicator ! Get(Set2Key, readMajority)

  val readAll = ReadAll(timeout = 5 seconds)
  replicator ! Get(ActiveFlagKey, readAll)

  def receive = {
    case Tick =>
      ???

    case _: UpdateResponse[_] =>

    case c @ Changed(DataKey) =>
      val data = c.get(DataKey)
      log.info("Current elements: {}", data.elements)

    case "increment" =>
      val upd = Update(Counter1Key, PNCounter(), writeTo3, request = Some(sender()))(_ + 1)
      replicator ! upd

    case UpdateSuccess(Counter1Key, Some(replyTo: ActorRef)) =>
      replyTo ! "ack"

    case UpdateTimeout(Counter1Key, Some(replyTo: ActorRef)) =>
      replyTo ! "nack"

    case "get-count" =>
      val readFrom3 = ReadFrom(n = 3, timeout = 1 second)
      replicator ! Get(Set1Key, readFrom3, request = Some(sender()))

    case g @ GetSuccess(Counter1Key, Some(replyTo: ActorRef)) =>
      val value = g.get(Counter1Key).value
      replyTo ! value

    case NotFound(Set1Key, Some(replyTo: ActorRef)) =>
      println("key counter1 does not exist")
      replyTo ! 0L

    case g @ GetSuccess(Set1Key, req) =>
      val elements = g.get(Set1Key).elements

    case GetFailure(Set1Key, Some(replyTo: ActorRef)) =>
      println("read from 3 nodes failed within 1 second")
      replyTo ! -1L
  }

  case class GetCart()
  case class Cart(s: Set[String])
  case class LineItem()
  case class AddItem(ietm: LineItem)
  case class RemoveItem(id: Long)

  def receiveCart: Receive = {
    case GetCart =>
      replicator ! Get(DataKey, readMajority, Some(sender()))

    case g @ GetSuccess(DataKey, Some(replyTo: ActorRef)) =>
      val data = g.get(DataKey)
      val cart = Cart(data.elements)
      replyTo ! cart

    case NotFound(DataKey, Some(replyTo: ActorRef)) =>
      replyTo ! Cart(Set.empty)

    case GetFailure(DataKey, Some(replyTo: ActorRef)) =>
      replicator ! Get(DataKey, ReadLocal, Some(replyTo))
  }

  def updateCart(c: DeltaReplicatedData, item: LineItem) = ???

  def receiveAddItem: Receive = {
    case cmd @ AddItem(item) =>
      val update = Update(DataKey, LWWMap.empty[String, LineItem], writeMajority, Some(cmd)) {
        cart => updateCart(cart, item)
      }
      replicator ! update
  }

  def receiveRemoveItem: Receive = {
    case cmd @ RemoveItem(productId) =>
      replicator ! Get(DataKey, readMajority, Some(cmd))

//    case GetSuccess(DataKey, Some(RemoveItem(productId))) =>
//      replicator ! Update(DataKey, LWWMap(), writeMajority, None) {
//        x => x - productId
//      }

//    case GetFailure(DataKey, Some(RemoveItem(productId))) =>
//      replicator ! Update(DataKey, LWWMap(), writeMajority, None) {
//        _ - productId
//      }

    case NotFound(DataKey, Some(RemoveItem(productId))) =>
      println("nothing to remove")
  }

  replicator ! Delete(Counter1Key, WriteLocal)
  replicator ! Delete(Set2Key, writeMajority)

  override def postStop(): Unit = tickTask.cancel()



}

object CRDTDemo extends App {
  val system = ActorSystem()
  implicit val cluster = Cluster(system)

  val c0 = PNCounter.empty
  val c1 = c0 + 1
  val c2 = c1 + 7
  val c3 = c2 - 2
  println(c3.value)

  val m0 = PNCounterMap.empty[String]
  val m1 = m0.increment("a", 7)
  val m2 = m1.decrement("a", 2)
  val m3 = m2.increment("b", 1)

  println(m3.get("a"))
  m3.entries.foreach { case (k,v) => println(s"${k}->${v}")}

  val s0 = GSet.empty[String]
  val s1 = s0 + "a"
  val s2 = s1 + "b" + "c"
  if(s2.contains("a"))
    println(s2.elements)

  val p0 = ORSet.empty[String]
  val p1 = p0 + "a"
  val p2 = p1 + "b"
  val p3 = p2 - "a"
  println(p3.elements)

  val n0 = ORMultiMap.empty[String, Any]
  val n1 = n0 + ("a" -> Set(1, 2, 3))
  val n2 = n1.addBinding("a", 4)
  val n3 = n2.removeBinding("a", 2)
  val n4 = n3.addBinding("a", 1)
  println(n4.entries)

  val f0 = Flag.empty
  val f1 = f0.switchOn
  println(f1.enabled)

  val r0 = LWWRegister("hello")
  val r1 = r0.withValue("hi")
  println(s"${r1.value} by ${r1.updatedBy} at ${r1.timestamp}")

  case class Record(version: Int, name: String, address: String)
  implicit val recordClock = new LWWRegister.Clock[Record]{
    override def apply(currentTimestamp: Long, value: Record): Long = value.version
  }

  val q0 = LWWRegister(Record(1, "s", "a"))
  val q1 = LWWRegister(Record(2, "s", "b"))
  val q2 = q0.merge(q1)
  println(q2.value)


  system.terminate()
}

object CustomCRDTDemo extends App {

  case class TwoPhaseSet(
    adds: GSet[String] = GSet.empty[String],
    removals: GSet[String] = GSet.empty[String]) extends ReplicatedData {

    type T = TwoPhaseSet

    def add(element: String): T = {
      copy(adds = adds.add(element))
    }

    def remove(element: String): T = {
      copy(removals = removals.add(element))
    }

    def elements = adds.elements diff removals.elements

    override def merge(that: TwoPhaseSet): TwoPhaseSet = {
      copy(adds = this.adds.merge(that.adds),
        removals = this.removals.merge(that.removals))
    }
  }



}

