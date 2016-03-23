package akka.contrib.datareplication

import _root_.crdt.Replication._
import akka.actor.Address
import akka.cluster.UniqueAddress
import com.rbmhtechnology.eventuate.VectorTime
import org.apache.log4j.Logger

import scalaz.concurrent.{ Strategy, Task }

object Replicas {

  //Cannot be used because: to delete an element we have to delete it on a node where it was added,
  //we can control it this test

  /*
  implicit def scalaCrdtORSet() = new Replica[io.dmitryivanov.crdt.sets.ORSet, String] {
    import io.dmitryivanov.crdt.sets.ORSet.ElementState

    override def converge(op: String, crdt: io.dmitryivanov.crdt.sets.ORSet[String]): io.dmitryivanov.crdt.sets.ORSet[String] =
      op match {
        case ADD(product)  ⇒ crdt.add(ElementState(s"replica-$replicaNum", product))
        case DROP(product) ⇒ crdt.remove(ElementState(s"replica-$replicaNum", product))
      }

    override protected def lookup(crdt: io.dmitryivanov.crdt.sets.ORSet[String]): Set[String] =
      crdt.lookup
  }*/

  implicit def scalaCrdtLWWSet() = new Replica[io.dmitryivanov.crdt.sets.LWWSet, String] {

    import io.dmitryivanov.crdt.sets.LWWSet.ElementState

    private val logger = Logger.getLogger("scala-crdt-replica")

    override def converge(op: String, crdt: io.dmitryivanov.crdt.sets.LWWSet[String]): io.dmitryivanov.crdt.sets.LWWSet[String] =
      op match {
        case ADD(product) ⇒
          Thread.sleep(10) //provide time ordering to track causality and performs drop operations correctly
          val localTime = System.currentTimeMillis()
          logger.info(s"Add: $product on replica $replicaNum at $localTime")
          crdt.add(ElementState(localTime, product))

        case DROP(product) ⇒
          Thread.sleep(10)
          val localTime = System.currentTimeMillis()
          logger.info(s"Remove: $product on replica $replicaNum at $localTime")
          crdt.remove(ElementState(localTime, product))
      }

    override protected def lookup(crdt: io.dmitryivanov.crdt.sets.LWWSet[String]): Set[String] =
      crdt.lookup
  }

  implicit def scalaCrdtLWWState() = new Replica[LWWCrdtState, String] {

    import io.dmitryivanov.crdt.sets.LWWSet.ElementState

    private val logger = Logger.getLogger("scala-crdt-replica")

    override def converge(op: String, crdtState: LWWCrdtState[String]): LWWCrdtState[String] =
      op match {
        case ADD(product) ⇒
          val clock = crdtState.cnt + 1l
          logger.info(s"Add: $product on replica $replicaNum at $clock")
          crdtState.copy(crdtState.crdt.add(ElementState(clock, product)), clock)

        case DROP(product) ⇒
          val clock = crdtState.cnt + 1l
          logger.info(s"Remove: $product on replica $replicaNum at $clock")
          crdtState.copy(crdtState.crdt.remove(ElementState(clock, product)), clock)
      }

    override protected def lookup(crdtState: LWWCrdtState[String]): Set[String] =
      crdtState.crdt.lookup
  }

  //Conflicts is impossible in this test, because we have synch replication here, and state reflects all updates
  implicit def eventuateConcurrentVersions() = new Replica[ConcurrentVersionsState, OrderEvent] {
    private val logger = Logger.getLogger("eventuate-cv-replica")

    override def converge(op: OrderEvent, state: ConcurrentVersionsState[OrderEvent]): ConcurrentVersionsState[OrderEvent] =
      op match {
        case event @ OrderItemAdded(orderId, item) ⇒
          val ts = state.cv.all.head.vectorTimestamp
          val incVt = ts.increment(s"replica-$replicaNum")
          val updated = state.cv.update(event, incVt)
          logger.info(s"Add $item on replica $replicaNum at $incVt. Is it conflict: ${updated.conflict}")
          state.copy(updated)

        case event @ OrderItemRemoved(orderId, item) ⇒
          val ts = state.cv.all.head.vectorTimestamp
          val incVt = ts.increment(s"replica-$replicaNum")
          val updated = state.cv.update(event, ts.increment(s"replica-$replicaNum"))
          logger.info(s"Remove $item on replica $replicaNum at $incVt. Is it conflict: ${updated.conflict}")
          state.copy(updated)
      }

    override protected def lookup(crdtState: ConcurrentVersionsState[OrderEvent]) = {
      val order = crdtState.cv.all.head.value
      order.items.map(OrderItemAdded("shopping-cart-qwerty", _)).toSet
    }
  }

  implicit def eventuateReplica() = new Replica[com.rbmhtechnology.eventuate.crdt.ORSet, String] {
    private val LoggerE = Logger.getLogger("eventuate-replica")
    private var localTime = VectorTime() //tracks all additions and grows infinitely

    override def converge(cmd: String, s: com.rbmhtechnology.eventuate.crdt.ORSet[String]): com.rbmhtechnology.eventuate.crdt.ORSet[String] = cmd match {
      case ADD(product) ⇒
        localTime = s.versionedEntries./:(localTime)(_ merge _.vectorTimestamp).increment(product)
        val oRSet = s.add(product, localTime)
        LoggerE.info(s"Add $product\n on replica $replicaNum\n $localTime\n")
        oRSet
      case DROP(product) ⇒
        localTime = s.versionedEntries./:(localTime)(_ merge _.vectorTimestamp)
        LoggerE.info(s"Drop $product\n on replica $replicaNum\n $localTime\n")
        s.remove(product, s.versionedEntries.map(_.vectorTimestamp))
    }

    override protected def lookup(crdt: com.rbmhtechnology.eventuate.crdt.ORSet[String]): Set[String] = {
      LoggerE.info(s"Local history on $replicaNum: $localTime")
      LoggerE.info(s"History of merge on $replicaNum: ${crdt.versionedEntries}")
      LoggerE.info(s"Purchases on $replicaNum: ${crdt.versionedEntries.map(_.value)}")
      crdt.value
    }
  }

  implicit def akkaReplica() = new Replica[akka.contrib.datareplication.ORSet, String] {
    private val LoggerAkka = Logger.getLogger("akka-replica")
    private val node = UniqueAddress(Address("akka.tcp", "ecom-system", "localhost", 5000 + replicaNum), replicaNum)

    override def converge(cmd: String, s: akka.contrib.datareplication.ORSet[String]): akka.contrib.datareplication.ORSet[String] = {
      cmd match {
        case ADD(product) ⇒
          val vc = s.add(node, product)
          LoggerAkka.info(s"Added $product\n ON $node\n ${vc.elementsMap}\n")
          vc
        case DROP(product) ⇒
          val vc = s.remove(node, product)
          LoggerAkka.info(s"Dropped $product\n ON $node\n  ${vc.elementsMap}\n")
          vc
      }
    }

    override protected def lookup(crdt: akka.contrib.datareplication.ORSet[String]): Set[String] = {
      LoggerAkka.info(s"Purchases history: ${crdt.elementsMap} ")
      crdt.elements
    }
  }

  def akkaAdd(orSet: akka.contrib.datareplication.ORSet[String], node: UniqueAddress, prod: String) = orSet.add(node, prod)

  def akkaRemove(orSet: akka.contrib.datareplication.ORSet[String], node: UniqueAddress, prod: String) = orSet.remove(node, prod)
}