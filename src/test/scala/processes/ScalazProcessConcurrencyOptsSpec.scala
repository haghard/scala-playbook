package processes

import java.util.concurrent.Executors._
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import streams.ReadBatchData
import scala.collection.IndexedSeq
import scala.concurrent.SyncVar
import scalaz.stream.Process._
import scalaz.{ \/-, -\/, \/ }

import scalaz.stream._
import scalaz.concurrent.{ Strategy, Task }

class ScalazProcessConcurrencyOptsSpec extends Specification {
  val P = scalaz.stream.Process

  private val logger = Logger.getLogger("proc-binding")
  /*
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

      merge.mergeN(0)(source)(consumerStrategy).fold(0) { (a, b) ⇒
        val r = a + b
        logger.info(s"${Thread.currentThread.getName} - current sum: $r")
        r
      }.runLog
        .runAsync(sync.put)

      sync.get should be equalTo (\/-(IndexedSeq(sum)))
    }
  }
*/

  "asda" should {
    "as" in {
      import scalaz.syntax.equal._
      val range = 1 until 50
      import scalaz._
      import scalaz.std.anyVal._
      import scalaz.std.list._
      import scalaz.std.list.listSyntax._
      import scalaz.std.option._
      import scalaz.std.vector._
      import scalaz.std.string._
      import scalaz.syntax.equal._
      import scalaz.syntax.foldable._

      //implicit val M = scalaz.Equal[Int]
      //Process(0, 1, 2, 3, 4).splitOn(2).toList

      implicit val M = Monoid[String]

      var i = 0

      Process("1", "2", "3", "4", "5", "6", "7", "8").splitWith {
        in ⇒
          {
            i += 1
            i % 3 == 0
          }
      }.filter(_.size == 1)
        .flatMap { tag ⇒
          P.eval(Task.delay(println(tag)))
        }.runLog.run

      /*((P.emitAll(range) |> process1.sliding(5)).flatMap { batch =>
        P.eval(Task.delay(println(batch)))
      }).runLog.run*/

      1 should be equalTo 1
    }
  }
}