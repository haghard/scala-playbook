package processes

import java.util.concurrent.Executors

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.{ ForkJoinPool, ThreadLocalRandom }

import scalaz.stream._
import scala.concurrent.duration._
import scalaz.concurrent.Task
import java.util.concurrent.TimeoutException

class ScalazProcessSpec extends Specification {
  private val logger = Logger.getLogger("processes")

  implicit val executor = new ForkJoinPool(2)
  implicit val ex = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Schedulers"))

  "Process atLeastEvery without fail the whole process" should {
    "run" in {
      val P = scalaz.stream.Process
      val s = new SyncVar[Boolean]

      def atLeastEvery[A](rate: Process[Task, Duration], default: A)(p: Process[Task, A]): Process[Task, A] = {
        for {
          w ← rate either p
          res ← w.fold({ d ⇒ P.emit(default) }, { v: A ⇒ P.emit(v) })
        } yield (res)
      }

      val io = for {
        p ← P.emitAll(0 until 10)
        i ← P.eval(Task {
          val l = ThreadLocalRandom.current().nextInt(1, 4) * 1000
          Thread.sleep(l)
          s"$p - $l"
        }(executor))
      } yield i

      val p = atLeastEvery(time.awakeEvery(2200 milli), "Timeout 2200 mills")(io)
      p.take(10)
        .map { r ⇒ logger.info("result - " + r); r }
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.runAsync(_ ⇒ s.put(true))

      s.get should be equalTo true
    }
  }

  "Process atLeastEvery with fail on first timeout" should {
    "run" in {
      val P = scalaz.stream.Process
      val s = new SyncVar[Boolean]

      def atLeastEvery[A](rate: Process[Task, Duration], default: A)(p: Process[Task, A]): Process[Task, A] =
        for {
          w ← (rate either p) |> process1.sliding(2)
          r ← if (w.forall(_.isLeft)) P.fail(new TimeoutException(default.toString))
          else P.emitAll(w.headOption.toSeq.filter(_.isRight).map(_.getOrElse(default)))
        } yield (r)

      val io = for {
        p ← P.emitAll(0 until 10)
        i ← P.eval(Task {
          val l = ThreadLocalRandom.current().nextInt(1, 4) * 1000
          Thread.sleep(l)
          s"$p - $l"
        }(executor))
      } yield i

      val p = atLeastEvery(time.awakeEvery(2200 milli), "Timeout fail")(io)
      p.take(10)
        .map { r ⇒ logger.info("In time task latency: " + r); r }
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.runAsync(_ ⇒ s.put(true))

      s.get should be equalTo true
    }
  }
}