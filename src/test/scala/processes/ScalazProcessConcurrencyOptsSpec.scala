package processes

import java.util.concurrent.Executors._
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import scala.collection.IndexedSeq
import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.stream.Process._
import scalaz.{ \/-, -\/, \/ }

import scalaz.stream._
import scalaz.concurrent.{ Strategy, Task }

class ScalazProcessConcurrencyOptsSpec extends Specification {
  val P = scalaz.stream.Process

  val logger = Logger.getLogger("proc-binding")

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

      //arbitrary nondeterminism
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
      val range = 0 until 50
      val ioExecutor = newFixedThreadPool(4, new NamedThreadFactory("io-executor"))
      val fanOut = Strategy.Executor(newFixedThreadPool(1, new NamedThreadFactory("fan-out")))

      val sum = range.foldLeft(0)(_ + _)
      val sync = new SyncVar[Throwable \/ IndexedSeq[Int]]

      def resource(url: Int): Process[Task, Int] = P.eval(Task {
        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate blocking call
        logger.info(s"${Thread.currentThread.getName} - Get $url")
        url
      }(ioExecutor))

      val source: Process[Task, Process[Task, Int]] =
        emitAll(range) |> process1.lift(resource)

      merge.mergeN(0)(source)(fanOut).fold(0) { (a, b) ⇒
        val r = a + b
        logger.info(s"${Thread.currentThread.getName} - current sum: $r")
        r
      }.runLog
        .runAsync(sync.put)

      sync.get should be equalTo (\/-(IndexedSeq(sum)))
    }
  }

  "Parallel map-reduce through queue" should {
    "word count with monoid" in {
      import scalaz._
      import Scalaz._
      val PLogger = Logger.getLogger("map-reduce")

      //identity op(a, zero) == a
      //associativity op(op(a,b),c) == op(a, op(b,c))

      val mSize = Runtime.getRuntime.availableProcessors / 2
      val q = async.boundedQueue[String](mSize * mSize)
      implicit val S =
        Strategy.Executor(newFixedThreadPool(mSize, new NamedThreadFactory("map-reduce")))

      val inWords: Process[Task, String] =
        P.emitAll(Seq("A A", "A", "B", "C", "B", "C", "C", "B", "B", "C", "B", "C", "C A", "B", "B", "C", "B", "C", "C", "B", "B A"))

      val qWriter: Process[Task, Unit] =
        (inWords to q.enqueue)
          .drain.onComplete(Process.eval_ { PLogger.info("All input was scheduled. Close queue"); q.close })

      val mappers: Process[Task, Process[Task, Map[String, Int]]] =
        P.range(0, mSize) map { i ⇒
          PLogger.info(s"Start mapper process №$i")
          q.dequeue.map { line ⇒
            val m = line.split(" ").toList.foldMap(i ⇒ Map(i -> 1))
            PLogger.info(s"Mapper input: $line  out: $m")
            m
          }
        }

      val out: IndexedSeq[Map[String, Int]] =
        (qWriter.drain merge scalaz.stream.merge.mergeN(mSize)(mappers)(S).foldMonoid).runLog.run

      out.size === 1
      out(0)("B") === 10
      out(0)("C") === 9
      out(0)("A") === 5
    }
  }
}