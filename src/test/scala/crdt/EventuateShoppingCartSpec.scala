package crdt

import java.util.concurrent.Executors._

import scalaz.concurrent.Strategy

import org.scalacheck.Prop._
import org.scalacheck.Properties

import com.twitter.util.CountDownLatch
import scalaz.stream.{ Process, async }
import scala.collection.concurrent.TrieMap

/**
 * We emulate concurrently working replicas that receive operations [add|remove]
 * Once replica received operation we update own state and send it in to the replication channel
 * which emulated using [[scalaz.stream.async.mutable.Signal]]
 * We should end up having equal state on each replica
 */
class EventuateShoppingCartSpec extends Properties("ShoppingCart") {
  import Replication._

  property("Eventuate ORSet") = forAll { (products: Vector[String], cancelled: List[Int]) ⇒
    ShoppingCartLog.info("Products: " + products + " cancelled: " + cancelled)

    val sink = new TrieMap[Int, Set[String]]
    type RType[T] = com.rbmhtechnology.eventuate.crdt.ORSet[T]

    val inputBuffer = async.boundedQueue[String](Size)(R)
    val replicas = async.boundedQueue[Int](Size)(R)

    val purchases = products.toSet.&~(cancelled.map(products(_)).toSet).map("product-" + _)
    ShoppingCartLog.info("purchases: " + purchases)

    val latch = new CountDownLatch(replicasN.size)
    replicasN.foreach { replicas.enqueueOne(_).run }
    replicas.close.run

    //We need partial order for add/remove events for the same product-uuid
    //because you can't delete a product that hasn't been added
    //So we just add cancellation in the end
    val commands = P.emitAll(products.map("add-product-" + _) ++ cancelled.map("drop-product-" + products(_))).toSource

    val commandsWriter = (commands to inputBuffer.enqueue).drain.onComplete(Process.eval_(inputBuffer.close))

    val RCore = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors(), new RThreadFactory("shopping-cart-thread")))
    val replicationChannel = replicationChannelFor[RType, String](RCore)

    (commandsWriter merge
      replicas.dequeue.map { n ⇒
        (Replica[RType, String](n, inputBuffer, replicationChannel) run sink).runAsync { _ ⇒ latch.countDown() }
      }
    )(R).run.run

    latch.await()
    replicationChannel.close.run

    (replicasN.size == replicasN.size) :| "Result size violation" &&
      (sink(replicasN.head) == purchases) :| "Head violation" &&
      (sink(replicasN.tail.head) == purchases) :| "Next violation"
  }
}