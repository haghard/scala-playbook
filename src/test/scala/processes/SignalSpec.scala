package processes

import java.util.concurrent.Executors._
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicLong
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory
import scalaz.stream.Process._
import scalaz.Nondeterminism
import scala.concurrent.SyncVar
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream._

class SignalSpec extends Specification {
  val P = scalaz.stream.Process

  def LoggerOut[T, E](logger: Logger) = sink.lift[Task, Int] { (s: Int) ⇒
    Task.now(logger.info(s"latest value $s"))
  }

  "N writer and 2 readers through signal" should {
    "accumulate mutable state and read latest known value" in {
      val ND = Nondeterminism[Task]
      val syncResult = new SyncVar[Int]
      implicit val writers = new ForkJoinPool(5)

      val reader = Strategy.Executor(newFixedThreadPool(2, new NamedThreadFactory("reader")))
      val signal = async.signalOf(0)(reader)

      val counter0 = new AtomicLong()
      signal.discrete
        .dropWhile { _ ⇒ counter0.incrementAndGet() % 50000 != 0 }
        .to(LoggerOut(Logger.getLogger("reader-0")))
        .onComplete(Process.eval_(Task.delay(println("signal 0 done"))))
        .run.runAsync(_ ⇒ ())

      val counter1 = new AtomicLong()
      signal.discrete
        .dropWhile { _ ⇒ counter1.incrementAndGet() % 50000 != 0 }
        .to(LoggerOut(Logger.getLogger("reader-1")))
        .onComplete(Process.eval_(Task.delay(println("signal 1 done"))))
        .run.runAsync(_ ⇒ ())

      val f: (Unit, Unit, Unit, Unit, Unit) ⇒ Unit = (a, b, c, d, e) ⇒ ()

      def writer(id: Int) = P.emitAll(0 to 10000) map { i ⇒
        signal.compareAndSet(v ⇒ Some(v.getOrElse(0) + i)).run
        ()
      }

      val start = System.nanoTime()
      ND.nmap5(
        Task.fork(writer(1).run[Task]),
        Task.fork(writer(2).run[Task]),
        Task.fork(writer(3).run[Task]),
        Task.fork(writer(4).run[Task]),
        Task.fork(writer(5).run[Task])
      )(f)
        .runAsync { _ ⇒
          val result = signal.get.run
          println(s"""Duration: ${System.nanoTime() - start} : $result""")
          syncResult.put(result)
          signal.close.run
        }

      syncResult.get === 50005000 * 5
    }
  }
}