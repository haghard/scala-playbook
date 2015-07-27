package akka.contrib.datareplication

import akka.actor.Address
import akka.cluster.UniqueAddress
import com.rbmhtechnology.eventuate.VectorTime
import crdt.Replication.Replica
import org.apache.log4j.Logger

object Replicas {

  implicit def eventuateReplica() = new Replica[com.rbmhtechnology.eventuate.crdt.ORSet, String] {
    private val LoggerE = Logger.getLogger("eventuate-replica")
    @volatile private var localTime = VectorTime()

    override def merge(cmd: String, s: com.rbmhtechnology.eventuate.crdt.ORSet[String]): com.rbmhtechnology.eventuate.crdt.ORSet[String] = cmd match {
      case ADD(v) ⇒
        localTime = s.versionedEntries./:(localTime)(_ merge _.updateTimestamp).increase(v)
        LoggerE.info(s"Add $v\n ON $num\n $localTime\n")
        s.add(v, localTime)
      case DROP(v) ⇒
        localTime = s.versionedEntries./:(localTime)(_ merge _.updateTimestamp)
        LoggerE.info(s"Drop $v\n ON $num\n $localTime\n")
        s.remove(v, s.versionedEntries.map(_.updateTimestamp))
    }

    override def elements(set: com.rbmhtechnology.eventuate.crdt.ORSet[String]): Set[String] = {
      LoggerE.info("History:" + localTime)
      set.value
    }
  }

  implicit def akkaReplica() = new Replica[akka.contrib.datareplication.ORSet, String] {
    private val Logger4j = Logger.getLogger("akka-replica")
    lazy val node = UniqueAddress(
      Address("akka.tcp", "System", "localhost", 5000 + num), num)

    @volatile private var localTime =
      akka.contrib.datareplication.VectorClock.create()

    override def merge(cmd: String, s: ORSet[String]): ORSet[String] = {
      cmd match {
        case ADD(v) ⇒
          val vc = s.add(node, v)
          localTime = vc.vclock
          Logger4j.info(s"Add $v\n ON $node\n $localTime\n")
          vc

        case DROP(v) ⇒
          val vc = s.remove(node, v)
          localTime = vc.vclock
          Logger4j.info(s"Drop $v\n ON $node\n  $localTime\n")
          vc
      }
    }

    override def elements(set: ORSet[String]): Set[String] = {
      Logger4j.info("History:" + localTime)
      set.elements
    }
  }
}