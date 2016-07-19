package crdt

import java.util.concurrent.Executors._

import scalaz.concurrent.Strategy

import org.scalacheck.Prop._
import org.scalacheck.Properties

import java.util.concurrent.CountDownLatch
import scalaz.stream.{ Process, async }
import scala.collection.concurrent.TrieMap

/**
 * We are emulating concurrent replicas that receive operations [add|remove]
 * Once replica has received operation we update there state and send command in the replication channel
 * which emulated using [[scalaz.stream.async.mutable.Signal]]
 * We should end up having equal state(same added products) on each replica
 */
class EventuateShoppingCartSpec extends Properties("ShoppingCart") {
  import Replication._

  property("Eventuate ORSet") = forAll { (products: Vector[String], cancelled: List[Int]) ⇒
    ShoppingCartLog.info(s"Products: $products Cancelled: $cancelled")

    val sink = new TrieMap[Int, Set[String]]
    type RType[T] = com.rbmhtechnology.eventuate.crdt.ORSet[T]

    val commands = async.boundedQueue[String](Size)(R)
    val replicas = async.boundedQueue[Int](Size)(R)

    val acceptedPurchases = (products.toSet &~ cancelled.map(products(_)).toSet).map("product-" + _)
    ShoppingCartLog.info("Accepted purchases: " + acceptedPurchases)

    val latch = new CountDownLatch(replicasN.size)
    replicasN.foreach { replicas.enqueueOne(_).run }
    replicas.close.run

    val commandsP = P.emitAll(products.map("add-product-" + _) ++ cancelled.map("drop-product-" + products(_))).toSource

    val commandsWriter = (commandsP to commands.enqueue).drain.onComplete(Process.eval_(commands.close))

    val RCore = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors(), new RThreadFactory("shopping-cart-thread")))
    val replicationChannel = replicationSignal[RType, String](RCore)

    (commandsWriter merge
      replicas.dequeue.map { n ⇒
        (Replica[RType, String](n, commands, replicationChannel) run sink).runAsync { _ ⇒ latch.countDown() }
      }
    )(R).run.run

    latch.await()
    replicationChannel.close.run

    (replicasN.size == replicasN.size) :| "Result size violation" &&
      (sink(replicasN.head) == acceptedPurchases) :| "Head violation" &&
      (sink(replicasN.tail.head) == acceptedPurchases) :| "Next violation"
  }
}