package processes

import java.util.concurrent.CountDownLatch
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

      val sum = range.sum
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

      sync.get should be equalTo \/-(IndexedSeq(sum))
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

  /*
   * The resulting streams can be pulled independently on different rates,
   * though they will propagate back pressure if one of them is running too far ahead of the other
   */
  def broadcast2[T](source: Process[Task, T], limit: Int = 10)(implicit S: scalaz.concurrent.Strategy): Process[Task, (Process[Task, T], Process[Task, T])] = {
    val left = async.boundedQueue[T](limit)
    val right = async.boundedQueue[T](limit)
    val qWriter = (source observe left.enqueue observe right.enqueue).drain
      .onComplete(Process.eval_ { logger.info("All input was scheduled."); left.close.flatMap(_ ⇒ right.close) })
    (qWriter.drain merge P.emit((left.dequeue, right.dequeue)))
  }

  "Broadcast single process in 2 output processes where one of them degrade slowly" should {
    "have gapped at most of queue size and slow down the whole flow" in {
      implicit val E = newFixedThreadPool(4, new NamedThreadFactory("broadcast"))
      implicit val S = Strategy.Executor(E)

      val digits = P.emitAll(1 to 35)
      val latch = new CountDownLatch(1)
      var latency = 0
      val p = for {
        both ← broadcast2(digits)

        right = both._1.zip(P.repeatEval(Task {
          val delayPerMsg = 500
          Thread.sleep(delayPerMsg)
          delayPerMsg
        })).map(r ⇒ logger.info(s"${r._2} fetch left ${r._1}"))

        left = both._2.zip(P.repeatEval(Task {
          val init = 500
          latency += 30
          val delay = init + latency
          Thread.sleep(delay)
          delay
        })).map(r ⇒ logger.info(s"${r._2} fetch right ${r._1}"))

        _ ← right merge left
      } yield ()

      p.onComplete(P.eval(Task.delay { logger.info("Consumer are done"); latch.countDown() })).run.runAsync(_ ⇒ ())
      latch.await
      1 === 1
    }
  }
}