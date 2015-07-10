package processes

import java.util.concurrent.Executors._

import com.rbmhtechnology.eventuate.crdt.ORSet
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.async.mutable.Queue
import scalaz.stream.{ Process, async }
import com.rbmhtechnology.eventuate._

class EventuateSpec extends Specification {

  val P = scalaz.stream.Process
  val PLogger = Logger.getLogger("order")

  "item" should {
    "add remove" in {
      //starting point for 2 replica: val initState = VectorTime("a" -> 0l, "b" -> 0l)
      (VectorTime("a" -> 1l, "b" -> 0l) conc VectorTime("a" -> 0l, "b" -> 1l)) === true
    }
  }

  "Calculate aggregate order on 2 replica" should {
    "have converged deterministically" in {
      val E = Strategy.Executor(newFixedThreadPool(3, new NamedThreadFactory("replicator")))
      val replicatedOrder = async.signalOf(ORSet[String]())(E)
      val commands = async.boundedQueue[String](50)(E)
      val replicas = async.boundedQueue[Int](10)(E)
      implicit val S = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("replica")))

      val ops: Process[Task, String] = P.emitAll(Seq(
        "add-prodA",
        "add-prodB",
        "remove-prodB",
        "add-prodC",
        "remove-prodC",
        "add-prodE",
        "add-prodF"))

      val opsWriter: Process[Task, Unit] =
        (ops to commands.enqueue)
          .drain.onComplete(Process.eval_ {
            PLogger.info("All ops was scheduled")
            commands.close
          })

      def replicaN(numR: Int, q: Queue[String]): Process[Task, (Unit, ORSet[String])] = {
        var vTime = VectorTime()
        val clock = VectorClock("Order")

        val ADD = """add-(\w+)""".r
        val REMOVE = """remove-(\w+)""".r

        def update(cmd: String, order: ORSet[String]): ORSet[String] = cmd match {
          case ADD(v) ⇒
            vTime = vTime.increase(v)
            order.add(v, vTime)
          case REMOVE(v) ⇒
            order.remove(v, order.versionedEntries.map(_.updateTimestamp))
        }

        (q.dequeue.map { cmd ⇒
          PLogger.info(s"Replica №$numR has received cmd: $cmd VectorTime: $vTime")
          replicatedOrder.compareAndSet { c ⇒
            Some(update(cmd, c.get))
          }.run

          ()
        } zip
          replicatedOrder.discrete
          .map { r ⇒
            PLogger.info(s"Replica №$numR Current order:${r.value}")
            r
          }
        ).onComplete(P.eval_ {
            println(s"Final replica №$numR state:" + replicatedOrder.get.run.value)
            replicatedOrder.close
          })
      }

      replicas.enqueueOne(1).run
      replicas.enqueueOne(2).run
      replicas.close.run

      (opsWriter merge replicas.dequeue.map { id ⇒
        Task.fork(replicaN(id, commands).run[Task]).runAsync(_ ⇒ ())
      }).runLog.run

      //expected Set(prodA, prodE, prodF)
      1 === 1
    }
  }
}