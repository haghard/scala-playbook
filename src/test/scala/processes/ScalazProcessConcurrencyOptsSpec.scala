package processes

import java.util.concurrent.Executors._
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import scala.collection.IndexedSeq
import scala.concurrent.SyncVar
import scalaz.stream.Process._
import scalaz.{ \/-, -\/, \/ }

import scalaz.stream._
import scalaz.concurrent.{ Strategy, Task }

class ScalazProcessConcurrencyOptsSpec extends Specification {
  val P = scalaz.stream.Process

  private val logger = Logger.getLogger("proc-binding")

  "Binding to asynchronous sources" should {
    "non-deterministic interleave of both streams through merge/either" in {
      implicit val strategy =
        Strategy.Executor(newFixedThreadPool(2, new NamedThreadFactory("io-worker")))

      def ioTask(latency: Int): Task[Int] = Task.async(blockingIO(latency))

      def blockingIO(n: Int): (Throwable \/ Int ⇒ Unit) ⇒ Unit =
        callback ⇒ try {
          val result = (math.random * 100).toInt
          logger.info(s"Start: $result")
          Thread.sleep(n) // simulate blocking call
          logger.info(s"Done: $result")
          callback(\/-(result))
        } catch {
          case t: Throwable ⇒ callback(-\/(t))
        }

      def externalSource(sleepTime: Int) = Process.repeatEval(ioTask(sleepTime))

      val l = externalSource(800)
      val r = externalSource(900)

      (l merge r)
        .map(_.toString).take(10)
        .to(io.stdOutLines)
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.run

      (l either r)
        .map(_.toString).take(10)
        .to(io.stdOutLines)
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.run

      true should be equalTo true
    }
  }

  "Binding to asynchronous sources" should {
    "Merges non-deterministically processes with mergeN" in {
      val ioPool = newFixedThreadPool(4, new NamedThreadFactory("io-worker"))

      val pool = newFixedThreadPool(1, new NamedThreadFactory("Gather"))
      val consumerStrategy = Strategy.Executor(pool)

      val range = 0 until 50
      val sum = range.foldLeft(0)(_ + _)
      val sync = new SyncVar[Throwable \/ IndexedSeq[Int]]

      def resource(url: Int) =
        P.eval(Task {
          Thread.sleep(50) // simulate blocking call
          logger.info(s"${Thread.currentThread.getName} - Get $url")
          url
        }(ioPool))

      val source = emitAll(range) |> process1.lift(resource)

      merge.mergeN(source)(consumerStrategy).fold(0) { (a, b) ⇒
        val r = a + b
        logger.info(s"${Thread.currentThread.getName} - current sum: $r")
        r
      }.runLog
        .runAsync(sync.put)

      sync.get should be equalTo (\/-(IndexedSeq(sum)))
    }
  }
}