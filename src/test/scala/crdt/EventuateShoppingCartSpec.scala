package crdt

import java.util.concurrent.Executors._

import org.apache.log4j.Logger
import org.scalacheck.Prop.forAll
import com.rbmhtechnology.eventuate._
import com.twitter.util.CountDownLatch
import mongo.MongoProgram.NamedThreadFactory
import com.rbmhtechnology.eventuate.crdt.ORSet
import org.scalacheck.{ Arbitrary, Gen, Properties }

import scalaz.{ -\/, \/- }
import scalaz.stream.{ Process, async }
import scalaz.stream.async.mutable.{ Signal, Queue }
import scala.collection.concurrent.TrieMap
import scalaz.concurrent.{ Strategy, Task }

object EventuateShoppingCartSpec extends Properties("ShoppingCart") {

  val Size = 50
  val P = scalaz.stream.Process
  val replicasN = Set(1, 2, 3, 4, 5)
  val LoggerI = Logger.getLogger("order")

  val S = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 2,
    new NamedThreadFactory("replicator")))

  /*"VectorTime" should {
    "have detected concurrent versions" in {
      val r0 = VectorTime("a" -> 1l, "b" -> 0l) conc VectorTime("a" -> 0l, "b" -> 1l)
      val r1 = VectorTime("a" -> 1l) conc VectorTime("b" -> 1l)
      r0 === r1 //true
    }
  }*/

  def genBoundedList[T](size: Int, g: Gen[T]): Gen[List[T]] = Gen.listOfN(size, g)

  def genBoundedVector[T](size: Int, g: Gen[T]) = Gen.containerOfN[Vector, T](size, g)

  implicit def DropArbitrary: org.scalacheck.Arbitrary[List[Int]] =
    Arbitrary(genBoundedList(Size, Gen.chooseNum(0, Size - 1)))

  implicit def BuyArbitrary: org.scalacheck.Arbitrary[Vector[String]] =
    Arbitrary(genBoundedVector[String](Size, Gen.uuid.map { _.toString }))

  case class Replica(numR: Int, input: Queue[String], replicationChannel: Signal[ORSet[String]])
                    (implicit collector: TrieMap[Int, Set[String]]) {
    private val ADD = """add-(.+)""".r
    private val DROP = """drop-(.+)""".r

    //store all history for this replica (add and remove)
    @volatile var localTime = VectorTime()

    private def merge(cmd: String, order: ORSet[String]): ORSet[String] = cmd match {
      case ADD(v) ⇒
        localTime = order.versionedEntries.foldLeft(localTime)(_ merge _.updateTimestamp).increase(v)
        order.add(v, localTime)
      case DROP(v) ⇒
        localTime = order.versionedEntries.foldLeft(localTime)(_ merge _.updateTimestamp)
        order.remove(v, order.versionedEntries.map(_.updateTimestamp))
    }

    def start(): Task[Unit] =
      input.dequeue.flatMap { action ⇒
        //LoggerI.info(s"Replica:$numR has received cmd[$action]")
        P.eval(replicationChannel.compareAndSet(c ⇒ Some(merge(action, c.get))))
        /*zip P.eval(replicator.get))
          .map(out ⇒ LoggerI.info(s"Replica:$numR Order:${out._2.value} VT:[${out._2.versionedEntries}] Local-VT:[$localTime]"))*/
      }.onComplete(P.eval(Task.now(collector += numR -> replicationChannel.get.run.value)))
        .run[Task]
  }

  property("Preserve order concurrent operation that are happening") = forAll { (p: Vector[String], cancelled: List[Int]) ⇒
    implicit val collector = new TrieMap[Int, Set[String]]
    val replicator = async.signalOf(ORSet[String]())(S)
    val commands = async.boundedQueue[String](Size / 2)(S)
    val replicas = async.boundedQueue[Int](Size / 2)(S)

    val purchases = p.toSet.&~(cancelled.map(p(_)).toSet).map("product-" + _)

    val latch = new CountDownLatch(replicasN.size)
    replicasN.foreach { replicas.enqueueOne(_).run }
    replicas.close.run

    //We need partial order for add/remove events for the same product-uuid
    //because you can't delete a product that hasn't been added
    //So we just add cancellation in the end
    val ops = P.emitAll(p.map("add-product-" + _) ++ cancelled.map("drop-product-" + p(_)))
      .toSource

    val writer = (ops to commands.enqueue)
      .drain.onComplete(Process.eval_(commands.close))

    writer.merge(
      replicas.dequeue.map(
        Replica(_, commands, replicator).start()
          .runAsync { _ ⇒ latch.countDown() }
      )
    )(S).run.run

    latch.await()
    replicator.close.run

    //check first 2, bare minimum
    collector(replicasN.head) == purchases
    collector(replicasN.tail.head) == purchases
  }
}