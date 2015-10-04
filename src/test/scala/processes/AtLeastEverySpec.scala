package processes

import org.apache.log4j.Logger
import java.util.concurrent.Executors._
import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory

import scala.concurrent.duration.Duration
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.{ Process, process1, time }
import scala.concurrent.forkjoin.{ ForkJoinPool, ThreadLocalRandom }
import scala.concurrent.duration._

class AtLeastEverySpec extends Specification {
  val P = scalaz.stream.Process
  val executor = new ForkJoinPool(2)
  private val logger = Logger.getLogger("atLeastEvery")

  val Scheduler = newScheduledThreadPool(1, new NamedThreadFactory("Scheduler"))
  val I = Strategy.Executor(newScheduledThreadPool(2, new NamedThreadFactory("infrastructure")))

  "Process atLeastEvery" should {
    "if we exceed latency we will throw TimeoutException or Emit default" in {
      val n = 30

      def atLeastEvery[A](rate: Process[Task, Duration], default: A)(source: Process[Task, A]): Process[Task, A] =
        for {
          w ← (rate either source)(I) |> process1.sliding(2)
          r ← if (w.forall(_.isLeft)) {
            P.emit(s"Exceed latency $default".asInstanceOf[A])
            //P.fail(new TimeoutException(default.toString))
          } else P.emitAll(w.headOption.toSeq.filter(_.isRight).map(_.getOrElse(default)))
        } yield (r)

      def l() = ThreadLocalRandom.current().nextInt(3, 8) * 100

      val Source = for {
        p ← P.emitAll(Seq.range(1, n))
        i ← P.eval(Task {
          val latency = l()
          Thread.sleep(latency)
          logger.info(s"№ $p generated latency: $latency")
          s"$p - $latency"
        }(executor))
      } yield i

      val p = atLeastEvery(time.awakeEvery(500 milli)(I, Scheduler), "Task timeout !!!")(Source)
      p.take(n)
        .map { r ⇒ logger.info(s"result - $r"); r }
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.run

      1 === 1
    }
  }
}