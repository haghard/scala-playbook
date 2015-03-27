package processes

import java.util.concurrent.Executors._
import java.util.concurrent.TimeoutException

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent.SyncVar
import scala.concurrent.duration.Duration
import scala.concurrent.forkjoin.{ ForkJoinPool, ThreadLocalRandom }
import scalaz.concurrent.Task
import scalaz.stream.{ Process, process1, time }
import scala.concurrent.duration._

class AtLeastEverySpec extends Specification {
  private val logger = Logger.getLogger("atLeastEvery")

  val P = scalaz.stream.Process

  implicit val executor = new ForkJoinPool(2)
  implicit val ex = newScheduledThreadPool(1, new NamedThreadFactory("Schedulers"))

  "Process atLeastEvery" should {
    "will survive the timeout" in {

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
        .run.run

      true should be equalTo true
    }
  }

  "Process atLeastEvery" should {
    "will throw TimeoutException once we break latency limit" in {

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
        .run.run /*Async(_ ⇒ s.put(true))*/

      true should be equalTo true
    }
  }
}
