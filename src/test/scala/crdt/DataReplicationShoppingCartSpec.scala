package crdt

import java.util.concurrent.Executors._
import org.scalacheck.Prop._
import org.scalacheck.{ Gen, Properties }

import scala.collection.concurrent.TrieMap
import scalaz.concurrent.Strategy
import scalaz.stream.{ Process, async }

class DataReplicationShoppingCartSpec extends Properties("ReplicatedShoppingCart") {
  import Replication._

  property("AkkaDataReplication ORSet") = forAll(
    Gen.containerOfN[Vector, String](Size, for {
      a ← Gen.alphaUpperChar
      b ← Gen.alphaLowerChar
      c ← Gen.alphaUpperChar
    } yield new String(Array(a, b, c))),
    Gen.listOfN(Size, Gen.chooseNum(0, Size - 1))) { (p: Vector[String], cancelled: List[Int]) ⇒
      ShoppingCartLog.info("Products: " + p + " cancelled: " + cancelled)

      val collector = new TrieMap[Int, Set[String]]
      type CRDT[T] = akka.contrib.datareplication.ORSet[T]

      val RCore = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors(), new RThreadFactory("shopping-cart-thread")))

      val commands = async.boundedQueue[String](Size)(R)
      val replicas = async.boundedQueue[Int](Size)(R)

      val purchases = (p.toSet &~ cancelled.map(p(_)).toSet).map("product-" + _)
      ShoppingCartLog.info("Accepted purchases: " + purchases)

      val latch = new java.util.concurrent.CountDownLatch(replicasN.size)
      replicasN.foreach { replicas.enqueueOne(_).run }
      replicas.close.run

      val commandsP = P.emitAll(p.map("add-product-" + _) ++ cancelled.map("drop-product-" + p(_))).toSource

      val commandsWriter = (commandsP to commands.enqueue).drain.onComplete(Process.eval_(commands.close))

      val replicator = replicationSignal[CRDT, String](RCore)

      (commandsWriter merge
        replicas.dequeue.map { Replica[CRDT, String](_, commands, replicator).run(collector).runAsync { _ ⇒ latch.countDown() } })(R)
        .run.run

      latch.await()
      replicator.close.run

      (replicasN.size == replicasN.size) :| "Result size violation" &&
        (collector(replicasN.head) == purchases) :| "Head violation" &&
        (collector(replicasN.tail.head) == purchases) :| "Next violation"
    }
}