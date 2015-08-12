package akka.contrib.datareplication

import akka.actor.Address
import org.apache.log4j.Logger
import crdt.Replication.Replica
import akka.cluster.UniqueAddress
import com.rbmhtechnology.eventuate.VectorTime

object Replicas {

  implicit def eventuateReplica() = new Replica[com.rbmhtechnology.eventuate.crdt.ORSet, String] {
    private val LoggerE = Logger.getLogger("eventuate-replica")
    @volatile private var localTime = VectorTime() //tracks all additions and grows infinitely

    override def converge(cmd: String, s: com.rbmhtechnology.eventuate.crdt.ORSet[String]): com.rbmhtechnology.eventuate.crdt.ORSet[String] = cmd match {
      case ADD(v) ⇒
        localTime = s.versionedEntries./:(localTime)(_ merge _.updateTimestamp).increase(v)
        val r = s.add(v, localTime)
        LoggerE.info(s"Add $v\n ON $num\n $localTime\n")
        r
      case DROP(v) ⇒
        localTime = s.versionedEntries./:(localTime)(_ merge _.updateTimestamp)
        LoggerE.info(s"Drop $v\n ON $num\n $localTime\n")
        s.remove(v, s.versionedEntries.map(_.updateTimestamp))
    }

    override def elements(set: com.rbmhtechnology.eventuate.crdt.ORSet[String]): Set[String] = {
      LoggerE.info(s"History local on $num: $localTime")
      LoggerE.info(s"History of merge on $num: ${set.versionedEntries}")
      LoggerE.info(s"Purchases history on $num: ${set.versionedEntries.map(_.value)}")
      set.value
    }
  }

  implicit def akkaReplica() = new Replica[akka.contrib.datareplication.ORSet, String] {
    private val LoggerAkka = Logger.getLogger("akka-replica")
    private lazy val node = UniqueAddress(
      Address("akka.tcp", "Ecom-System", "localhost", 5000 + num), num)

    override def converge(cmd: String, s: ORSet[String]): ORSet[String] = {
      cmd match {
        case ADD(v) ⇒
          val vc = s.add(node, v)
          LoggerAkka.info(s"Added $v\n ON $node\n ${vc.elementsMap}\n")
          vc
        case DROP(v) ⇒
          val vc = s.remove(node, v)
          LoggerAkka.info(s"Dropped $v\n ON $node\n  ${vc.elementsMap}\n")
          vc
      }
    }

    override def elements(set: ORSet[String]): Set[String] = {
      LoggerAkka.info(s"Purchases history: ${set.elementsMap} ")
      set.elements
    }
  }
}