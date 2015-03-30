package processes

import java.util.concurrent.Executors._
import java.util.concurrent.TimeoutException

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent.duration.Duration
import scala.concurrent.forkjoin.{ ForkJoinPool, ThreadLocalRandom }
import scalaz.concurrent.Task
import scalaz.stream.{ Process, process1, time }
import scala.concurrent.duration._

class AtLeastEverySpec extends Specification {
  private val logger = Logger.getLogger("atLeastEvery")

  val P = scalaz.stream.Process

  val executor = new ForkJoinPool(2)
  implicit val ex = newScheduledThreadPool(1, new NamedThreadFactory("Schedulers"))

  "Process atLeastEvery" should {
    "if we exceed latency for whole provess we will throw TimeoutException/emit default" in {
      val n = 15
      def atLeastEvery[A](rate: Process[Task, Duration], default: A)(p: Process[Task, A]): Process[Task, A] = {
        for {
          w ← rate either p
          r ← w.fold({ d ⇒ /*P.emit(default)*/ P.fail(new TimeoutException(default.toString)) }, { v: A ⇒ P.emit(v) })
        } yield r
      }

      val io = (for {
        p ← P.emitAll(1 until n)
        i ← P.eval(Task {
          val l = ThreadLocalRandom.current().nextInt(1, 4) * 100
          logger.info("№" + p + " - generated latency: " + l)
          Thread.sleep(l)
          s"$p - $l"
        }(executor))
      } yield i)

      val p = atLeastEvery(time.awakeEvery(2000 milli), "Timeout the whole process in  3000 mills")(io)
      p.take(n)
        .map { r ⇒ logger.info("result - " + r); r }
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.run
      true should be equalTo true
    }
  }

  "Process atLeastEvery" should {
    "if we exceed latency we will throw TimeoutException/emit default" in {
      val n = 10
      def atLeastEvery[A](rate: Process[Task, Duration], default: A)(p: Process[Task, A]): Process[Task, A] =
        for {
          w ← (rate either p) |> process1.sliding(2)
          r ← if (w.forall(_.isLeft)) P.emit(default) //P.fail(new TimeoutException(default.toString))
          else P.emitAll(w.headOption.toSeq.filter(_.isRight).map(_.getOrElse(default)))
          l = logger.info(w)
        } yield (r)

      val io = for {
        p ← P.emitAll(1 until n)
        i ← P.eval(Task {
          val l = ThreadLocalRandom.current().nextInt(1, 4) * 1000
          logger.info("№" + p + " - generated latency: " + l)
          Thread.sleep(l)
          s"$p - $l"
        }(executor))
      } yield i

      val p = atLeastEvery(time.awakeEvery(2200 milli), "Task timeout !!!")(io)
      p.take(n)
        .map { r ⇒ logger.info("result - " + r); r }
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.run /*Async(_ ⇒ s.put(true))*/

      true should be equalTo true
    }
  }
}
