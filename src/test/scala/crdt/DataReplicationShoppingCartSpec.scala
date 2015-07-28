package crdt

import java.util.concurrent.Executors._
import com.twitter.util.CountDownLatch
import mongo.MongoProgram.NamedThreadFactory

import org.scalacheck.Prop._
import org.scalacheck.Properties

import scala.collection.concurrent.TrieMap
import scalaz.concurrent.Strategy
import scalaz.stream.{ Process, async }

object DataReplicationShoppingCartSpec extends Properties("ReplicatedShoppingCart") {
  import Replication._
  val l = org.apache.log4j.Logger.getLogger("ShoppingCart")

  property("Preserve order concurrent operation that are happening with Akka") = forAll { (p: Vector[String], cancelled: List[Int]) ⇒
    l.info("Wishes: " + p + " cancelled: " + cancelled)

    implicit val collector = new TrieMap[Int, Set[String]]
    type RType[T] = akka.contrib.datareplication.ORSet[T]

    val RCore = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors(),
      new NamedThreadFactory("r-core")))

    val input = async.boundedQueue[String](Size)(R)
    val replicas = async.boundedQueue[Int](Size)(R)

    val purchases = p.toSet.&~(cancelled.map(p(_)).toSet).map("product-" + _)
    l.info("purchases: " + purchases)

    val latch = new CountDownLatch(replicasN.size)
    replicasN.foreach { replicas.enqueueOne(_).run }
    replicas.close.run

    //We need partial order for add/remove events for the same product-uuid
    //because you can't delete a product that hasn't been added
    //So we just add cancellation in the end
    val ops = P.emitAll(p.map("add-product-" + _) ++ cancelled.map("drop-product-" + p(_)))
      .toSource

    val Writer = (ops to input.enqueue).drain.onComplete(Process.eval_(input.close))

    val replicator = replicatorChannelFor[RType, String](RCore)

    Writer.merge(
      replicas.dequeue.map {
        Replica[RType, String](_, input, replicator).task(collector) //run replicas in concurrent manner
          .runAsync { _ ⇒ latch.countDown() }
      }
    )(R).run.run

    latch.await()
    replicator.close.run

    (replicasN.size == replicasN.size) :| "Result size violation" &&
      (collector(replicasN.head) == purchases) :| "Head violation" &&
      (collector(replicasN.tail.head) == purchases) :| "Next violation"
  }
}