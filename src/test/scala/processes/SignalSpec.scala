package processes

import java.util.concurrent.Executors._

import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory
import scalaz.{ \/-, \/ }
import scala.concurrent.SyncVar
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.{ process1, Process, async, sink }

class SignalSpec extends Specification {
  val P = scalaz.stream.Process

  private val logger = Logger.getLogger("proc-signal")

  def LoggerOut[T, E] = sink.lift[Task, Int]((s: Int) ⇒
    Task.delay(logger.info(s"${Thread.currentThread().getName} - intermediate value $s")))

  "3 writer in 1 signal" should {
    "change mutable state" in {
      val F = scalaz.Nondeterminism[Task]
      val sync = new SyncVar[Throwable \/ Unit]
      val syncResult = new SyncVar[Int]

      implicit val worker = newFixedThreadPool(3, new NamedThreadFactory("worker"))
      val signal = async.signalOf(0)(Strategy.Executor(newFixedThreadPool(1, new NamedThreadFactory("signal"))))

      (signal.discrete observe LoggerOut)
        .onComplete(Process.eval(Task.delay(println("signal done"))))
        .run.runAsync(sync.put)

      val f: (Unit, Unit, Unit) ⇒ Unit =
        (a, b, c) ⇒
          ()

      def proc = P.emitAll(0 to 10) |> process1.lift { i ⇒
        Thread.sleep(500)
        logger.info(s"${Thread.currentThread().getName} - calculate value $i")
        signal.compareAndSet(arg ⇒ Some(arg.getOrElse(0) + i)).run
        ()
      }

      F.nmap3(Task.fork(proc.run[Task]), Task.fork(proc.run[Task]), Task.fork(proc.run[Task]))(f)
        .runAsync { _ ⇒
          syncResult.put(signal.get.run)
          signal.close.run
        }

      sync.get should be equalTo \/-(())
      syncResult.get should be equalTo 165
    }
  }
}