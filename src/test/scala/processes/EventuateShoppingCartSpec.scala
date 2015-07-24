package processes

import java.util.concurrent.Executors._
import com.rbmhtechnology.eventuate.crdt.ORSet
import com.twitter.util.CountDownLatch
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.collection.concurrent.TrieMap
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.async.mutable.Queue
import scalaz.stream.{ Process, async }
import com.rbmhtechnology.eventuate._

class EventuateShoppingCartSpec extends Specification {

  val P = scalaz.stream.Process
  val PLogger = Logger.getLogger("order")

  "VectorTime" should {
    "have detected concurrent versions" in {
      val r0 = VectorTime("a" -> 1l, "b" -> 0l).conc(VectorTime("a" -> 0l, "b" -> 1l))
      val r1 = VectorTime("a" -> 1l).conc(VectorTime("b" -> 1l))

      r0 === r1 //true
    }
  }
  //Make in property based
  "Calculate aggregated order on N replicas" should {
    "have converged deterministically" in {
      val E = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("replicator")))
      val replicator = async.signalOf(ORSet[String]())(E)
      val commands = async.boundedQueue[String](50)(E)
      val replicas = async.boundedQueue[Int](10)(E)
      val S = Strategy.Executor(newFixedThreadPool(2, new NamedThreadFactory("replica")))

      val replicasN = Set(1, 2, 3)
      val latch = new CountDownLatch(replicasN.size)
      replicasN.foreach { replicas.enqueueOne(_).run }
      replicas.close.run

      val results = new TrieMap[Int, Set[String]]

      //First N command shouldn't be concurrent, where N == replica number
      val ops: Process[Task, String] = P.emitAll(Seq(
        "add-prodA", "add-prodB", "add-prodC",
        "remove-prodB", "remove-prodC", "add-prodE",
        "add-prodF", "add-prodI", "remove-prodE", "remove-prodE",
        "remove-prodA"))

      val opsWriter = (ops to commands.enqueue).drain
        .onComplete(Process.eval_(commands.close))

      def replica(numR: Int, q: Queue[String]): Task[Unit] = {
        //Store history to be able to observe all history of actions on replica
        @volatile var vt = VectorTime()

        val ADD = """add-(\w+)""".r
        val REMOVE = """remove-(\w+)""".r

        def merge(cmd: String, order: ORSet[String]): ORSet[String] = cmd match {
          case ADD(v) ⇒
            //detect conflicts
            /*order.versionedEntries.find { _.updateTimestamp conc vt }.fold(PLogger.info("No conflicts detected")) { r ⇒
              PLogger.info(s"Conflict detected $vt - ${r.updateTimestamp}")
            }*/
            vt = order.versionedEntries.foldLeft(vt)(_ merge _.updateTimestamp).increase(v)
            order.add(v, vt)
          case REMOVE(v) ⇒
            vt = order.versionedEntries.foldLeft(vt)(_ merge _.updateTimestamp)
            order.remove(v, order.versionedEntries.map(_.updateTimestamp))
        }

        q.dequeue.flatMap { action ⇒
          PLogger.info(s"R №$numR has received cmd[$action] VT: $vt")
          //linearizable write read
          (P.eval(replicator.compareAndSet(c ⇒ Some(merge(action, c.get))))
            zip P.eval(replicator.get))
            .map { out ⇒
              PLogger.info(s"R №$numR Order:${out._2.value} VT:[${out._2.versionedEntries}] Local-VT:[$vt]")
            }
        }.onComplete(P.eval_ {
          Task.delay {
            results += (numR -> replicator.get.run.value)
            latch.countDown()
          }
        }).run[Task]
      }

      opsWriter.merge(
        replicas.dequeue.map(replica(_, commands).runAsync(_ ⇒ ()))
      )(S).run.run

      latch.await()
      replicator.close.run
      results.keySet.size === replicasN.size
      results.keySet === replicasN

      results(replicasN.head) === Set("prodI", "prodF")
      results(replicasN.tail.head) === Set("prodI", "prodF")
      results(replicasN.tail.tail.head) === Set("prodI", "prodF")
    }
  }
}