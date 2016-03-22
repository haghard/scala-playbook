package crdt

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors._

import org.scalacheck.Prop._
import org.scalacheck.{ Gen, Properties }

import scala.collection.concurrent.TrieMap
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.{ Process, async }

class ScalaCrdtShoppingCartSpec extends Properties("ShoppingCart-ScalaCrdt") {

  import Replication._
  property("ScalaCrdt ORSet") = forAll(
    Gen.containerOfN[Vector, String](Size, for {
      a ← Gen.alphaUpperChar
      b ← Gen.alphaLowerChar
      c ← Gen.alphaUpperChar
    } yield new String(Array(a, b, c))),
    Gen.listOfN(Size, Gen.chooseNum(0, Size - 1))) { (products: Vector[String], cancelled: List[Int]) ⇒

      ShoppingCartLog.info(s"Products: $products cancelled $cancelled")

      val collector = new TrieMap[Int, Set[String]]
      type RType[T] = io.dmitryivanov.crdt.sets.LWWSet[T]

      val Ex = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors(),
        new RThreadFactory("shopping-cart")))

      val operationsQ = async.boundedQueue[String](Size)(R)
      val replicas = async.boundedQueue[Int](Size)(R)

      val purchases = (products.toSet &~ cancelled.map(products(_)).toSet).map("product-" + _)
      ShoppingCartLog.info("accepted purchases: " + purchases)

      val latch = new CountDownLatch(replicasN.size)
      replicasN.foreach {
        replicas.enqueueOne(_).run
      }
      replicas.close.run

      val operations: Process[Task, String] =
        P.emitAll(products.map("add-product-" + _) ++ cancelled.map("drop-product-" + products(_))).toSource

      val operationW =
        (operations to operationsQ.enqueue).drain.onComplete(Process.eval_(operationsQ.close))

      val replicator = replicationSignal[RType, String](Ex)

      (operationW merge
        replicas.dequeue.map {
          Replica[RType, String](_, operationsQ, replicator).run(collector).runAsync { _ ⇒ latch.countDown() }
        })(R)
        .run.run

      latch.await()
      replicator.close.run

      (if (collector.keySet.size > 0) {
        collector.values.head == purchases
      } else true)
    }
}
