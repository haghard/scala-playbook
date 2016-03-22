package crdt

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors._

import crdt.Replication._
import org.specs2.mutable.Specification
import io.dmitryivanov.crdt.sets._

import scala.collection.concurrent.TrieMap
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.{ Process, async }

class ScalaCrdtSpec extends Specification {

  "ORSet" should {
    "Product won't be removed" in {
      def value[T](key: Int, v: T) = ORSet.ElementState(key.toString, v)

      val result = new ORSet[String]()
        .add(value(1, "YlN"))
        .add(value(2, "YlM"))
        .add(value(3, "SVM"))
        //.remove(value(3, "SVM")) will work
        .remove(value(1, "SVM"))
        .lookup

      result.size must beEqualTo(3)
    }

    "Product will be removed" in {
      def value[T](key: Int, v: T) = ORSet.ElementState(key.toString, v)

      val result = new ORSet[String]()
        .add(value(1, "YlN"))
        .add(value(2, "YlM"))
        .add(value(3, "SVM"))
        .remove(value(3, "SVM"))
        .lookup

      result.size must beEqualTo(2)
    }

    "Product will be removed after merge" in {
      //should be remove on replica where it was added

      def value[T](key: Int, v: T) = ORSet.ElementState(key.toString, v)

      val replicaOne = new ORSet[String]().add(value(1, "product-EcM"))
      val replicaTwo = new ORSet[String]().add(value(2, "product-WwQ"))
      val replicaThree = new ORSet[String]().add(value(3, "product-ZxQ"))

      val merged = ((replicaOne merge replicaTwo) merge replicaThree)

      //val result = merged.remove(value(1, "product-ZxQ")).lookup
      val result = merged.remove(value(3, "product-ZxQ")).lookup

      result.size must beEqualTo(2)
      result must contain("product-EcM", "product-WwQ")
    }

    "Concurrent add/remove Add wins because add biases" in {
      //ORSet arbitrary additions and removals from the set
      //Property captures causal information in object itself that guarantees correct convergence
      //Add wins(add biases) because we are allowed to remove things that already added

      def value[T](key: Int, v: T) = ORSet.ElementState(key.toString, v)

      val result = (new ORSet[String]().add(value(1, "product-EcM")) merge new ORSet[String]().remove(value(2, "product-EcM")))
        .lookup

      result.size must beEqualTo(1)
      result must contain("product-EcM")
    }

    "Concurrent add/remove add aka Duplication state" in {
      //Add wins(add biases) because we are allowed to remove things that already added

      def value[T](key: Int, v: T) = ORSet.ElementState(key.toString, v)

      val one = new ORSet[String]()
        .add(value(1, "product-EcM"))
        .remove(value(1, "product-EcM"))

      val two = new ORSet[String]()
        .add(value(2, "product-EcM"))

      val result = (one merge two).lookup

      result.size must beEqualTo(1)
      result must contain("product-EcM")
    }
  }

  //Challenges: Client go offline, duplication of state, reordering of messages
  /**
   * Note that LWWSet relies on synchronized clocks and should only be used when the choice of value is not
   * important for concurrent updates occurring within the clock skew.
   * Instead of using timestamps based on System.currentTimeMillis() time it is possible to use a timestamp value based on something else,
   * for example an increasing version number from a database record that is used for optimistic concurrency control
   * or just use vector clocks.
   */

  //a) merge incoming vc + local one
  //b) increment merged result and write into LWWSet
  //c)
  //This sequence will create

  "LWWSet" should {
    "Concurrent add/remove. Product will be removed" in {

      def value[T](ts: Long, v: String) = LWWSet.ElementState(ts, v)

      val one = new LWWSet[String]().add(value(2l, "prod_a"))
      val two = new LWWSet[String]().remove(value(3l, "prod_a"))

      val merged = (one merge two)
      val result = merged.lookup

      result.size must beEqualTo(0)
    }

    "Concurrent add/remove. Product won't be removed" in {
      //Product will be removed
      def value[T](ts: Long, v: String) = LWWSet.ElementState(ts, v)

      val one = new LWWSet[String]().add(value(3l, "prod_a"))
      val two = new LWWSet[String]().remove(value(3l, "prod_a"))

      val merged = (one merge two)
      val result = merged.lookup

      result.size must beEqualTo(1)
    }

    "add/remove/add LWWSet" in {
      val lww = new LWWSet[String]()

      def value[T](ts: Long, v: String) = LWWSet.ElementState(ts, v)

      val result = lww
        .add(value(1l, "prod_a"))
        .remove(value(2l, "prod_a"))
        .add(value(3l, "prod_a"))
        .add(value(3l, "prod_b"))
        .lookup

      result.size must beEqualTo(2)
      result must contain("prod_a", "prod_b")
    }

    "LWWSet merge 2 sets" in {
      def value[T](ts: Long, v: T) = LWWSet.ElementState(ts, v)

      val one = new LWWSet[String]()
        .add(value(2l, "a"))
        .add(value(3l, "b"))

      val two = new LWWSet[String]()
        //.remove(value(1l, "a"))
        .remove(value(4l, "b"))

      (one merge two).lookup must beEqualTo(Set("a"))
    }
  }

  "LWWSet replicator" should {

    "start shopping with duplicated state" in {
      //we manually create total order with Thread.sleep in replica
      type CRDT[T] = io.dmitryivanov.crdt.sets.LWWSet[T]

      val sink = new TrieMap[Int, Set[String]]
      val latch = new CountDownLatch(replicasN.size)

      val operations = async.boundedQueue[String](Size)(R)
      val replicas = async.boundedQueue[Int](Size)(R)
      val Ex = Strategy.Executor(newFixedThreadPool(2, new RThreadFactory("signal")))

      //Messages can be duplicated
      val products = Vector("NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "HmB", "AqO", "QuL", "HrZ", "ZzF", "YiZ", "OxI", "AaA", "TgV")
      val cancelled = List(0, 1, 2)
      val events = products.map(name ⇒ s"add-product-$name").toList ::: cancelled.map(ind ⇒ s"drop-product-${products(ind)}")

      val purchases = (products.toSet diff cancelled.map(products(_)).toSet).map("product-" + _)
      ShoppingCartLog.info("accepted purchases: " + purchases)

      replicasN.foreach(replicas.enqueueOne(_).run)

      val eventsWriter = (P.emitAll(events).toSource to operations.enqueue)
        .drain
        .onComplete(Process.eval_(replicas.close.flatMap(_ ⇒ operations.close)))

      val replicator = replicationSignal[CRDT, String](Ex)

      (eventsWriter merge replicas.dequeue.map { replicaNum ⇒
        (Replica[CRDT, String](replicaNum, operations, replicator) run sink).runAsync(_ ⇒ latch.countDown())
      })(R).run.run

      latch.await()
      replicator.close.run

      sink(1) === purchases && sink(2) === purchases && sink(3) === purchases && sink(4) === purchases
    }
  }

  "LWWCrdtState replicator" should {

    //we manually create total order by maintaining global counter
    "start shopping with duplicated state" in {
      type CRDT[T] = crdt.Replication.LWWCrdtState[T]

      val sink = new TrieMap[Int, Set[String]]
      val latch = new CountDownLatch(replicasN.size)

      val operations = async.boundedQueue[String](Size)(R)
      val replicas = async.boundedQueue[Int](Size)(R)
      val Ex = Strategy.Executor(newFixedThreadPool(2, new RThreadFactory("signal")))

      //Messages can be duplicated
      val products = Vector("NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "HmB", "AqO", "QuL", "HrZ", "ZzF", "YiZ", "OxI", "AaA", "TgV")
      val cancelled = List(0, 1, 2)
      val events = products.map(name ⇒ s"add-product-$name").toList ::: cancelled.map(ind ⇒ s"drop-product-${products(ind)}")

      val purchases = (products.toSet diff cancelled.map(products(_)).toSet).map("product-" + _)
      ShoppingCartLog.info("accepted purchases: " + purchases)

      replicasN.foreach(replicas.enqueueOne(_).run)

      val eventsWriter = (P.emitAll(events).toSource to operations.enqueue)
        .drain
        .onComplete(Process.eval_(replicas.close.flatMap(_ ⇒ operations.close)))

      val replicator = replicationSignal[CRDT, String](Ex)

      (eventsWriter merge replicas.dequeue.map { replicaNum ⇒
        (Replica[CRDT, String](replicaNum, operations, replicator) run sink).runAsync(_ ⇒ latch.countDown())
      })(R).run.run

      latch.await()
      replicator.close.run

      sink(1) === purchases && sink(2) === purchases && sink(3) === purchases && sink(4) === purchases
    }
  }

  "ConcurrentVersionsState replicator" should {

    "start shopping with duplicated state" in {
      type CRDT[T] = crdt.Replication.ConcurrentVersionsState[T]

      val sink = new TrieMap[Int, Set[OrderEvent]]

      //Messages can be duplicated
      val products = Vector("NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "NjX", "HmB", "AqO", "QuL", "HrZ", "ZzF", "YiZ", "OxI", "AaA", "TgV")
      val cancelled = List(0, 1, 2)

      val Ex = Strategy.Executor(newFixedThreadPool(2, new RThreadFactory("signal")))

      val operations = async.boundedQueue[OrderEvent](Size)(R)
      val replicas = async.boundedQueue[Int](Size)(R)

      val purchases = (products.toSet diff cancelled.map(products(_)).toSet).map(name ⇒ OrderItemAdded("shopping-cart-qwerty", s"product-$name"))
      ShoppingCartLog.info("accepted purchases: " + purchases)

      val latch = new CountDownLatch(replicasN.size)
      replicasN.foreach(replicas.enqueueOne(_).run)

      val events: List[OrderEvent] = products.map(name ⇒ OrderItemAdded("", s"product-$name")).toList :::
        cancelled.map(ind ⇒ OrderItemRemoved("", s"product-${products(ind)}"))

      val eventsWriter = (P.emitAll(events).toSource to operations.enqueue).drain
        .onComplete(Process.eval_(replicas.close.flatMap(_ ⇒ operations.close)))

      val replicator = replicationSignal[CRDT, OrderEvent](Ex)

      (eventsWriter merge replicas.dequeue.map { replicaNum ⇒
        (Replica[CRDT, OrderEvent](replicaNum, operations, replicator) run sink).runAsync(_ ⇒ latch.countDown())
      })(R).run.run

      latch.await()
      replicator.close.run

      sink(1) === purchases && sink(2) === purchases && sink(3) === purchases && sink(4) === purchases
    }
  }
}