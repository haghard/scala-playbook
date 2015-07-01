package processes

import java.util.concurrent.Executors._

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger

import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task
import scalaz.stream.Process._
import org.specs2.mutable.Specification
import scalaz.stream._
import scalaz.concurrent.Strategy

class BroadcastSpec extends Specification {

  val logger = Logger.getLogger("broadcast")

  /**
   *
   *
   */
  def broadcast[A](p: Process[Task, A], subs: Process[Task, String], bound: Int = 10): Process[Task, Unit] = Process.suspend {
    val P = newFixedThreadPool(2, new NamedThreadFactory("broadcast-pub")) // publish, and fork new consumers
    val I = Strategy.Executor(P)
    val S = newFixedThreadPool(1, new NamedThreadFactory("signal"))
    val SIG = Strategy.Executor(S)
    val Q = newFixedThreadPool(3, new NamedThreadFactory("broadcast-sub")) // could be extended depends on consumer's size
    val C = Strategy.Executor(Q)

    //Behaves like a AtomicReference for Set[Queue[A]]
    val signal = async.signalOf(Set[async.mutable.Queue[A]]())(SIG)

    def publish(a: A): Task[Unit] = for {
      qsList ← signal.discrete.filter(s ⇒ s.nonEmpty).take(1).runLog //get
      qs = qsList.flatMap { _.toList }

      _ = logger.info(s"Publish $a for ${qs.size}")
      _ ← Task.gatherUnordered(qs.toList.map(_ enqueueOne a))
    } yield ()

    def unsubscribe(q: async.mutable.Queue[A]): Task[Unit] = for {
      qs ← signal.get
      updatedQs = qs - q

      result ← signal compareAndSet {
        case Some(`qs`) ⇒ Some(updatedQs)
        case opt        ⇒ opt
      }

      _ ← if (result.contains(updatedQs))
        q.close
      else
        unsubscribe(q)
    } yield ()

    def subscribe(name: String): Task[Process[Task, A]] = for {
      qs ← signal.get
      q = async.boundedQueue[A](bound)(C)
      updatedQs = qs + q

      result ← signal.compareAndSet {
        case Some(`qs`) ⇒ Some(updatedQs)
        case opt        ⇒ opt
      }

      p = q.dequeue.onComplete(Process eval_ unsubscribe(q))

      back ← if (result.contains(updatedQs)) Task.delay { logger.info(s"New subscription from $name"); p }
      else subscribe(name)
    } yield back

    val publisher = (p to sink.lift(publish)).drain

    val source = subs.map { n ⇒
      Task.fork {
        subscribe(n).flatMap { p ⇒
          p.map(s"${Thread.currentThread().getName} - $n got " + _)
            .to(scalaz.stream.io.stdOutLines).run
        }
      }(P).runAsync(_ ⇒ ())
    }

    (publisher merge source)(I)
      .onComplete(Process.eval_(signal.set(Set()).map(_ ⇒ logger.info("Done"))))
  }

  "broadcast" should {
    "run for n" in {
      val names: Process[Task, String] = Process.emitAll(Seq("Doctor Who", "Sherlock", "Ironman", "Superman"))
      val subs = (names zip Process.repeatEval(Task.delay(Thread.sleep(4000)))).map(_._1)

      def naturals: Process[Task, Long] = {
        def go(i: Long): Process[Task, Long] =
          Process.await(Task.delay(i)) { i ⇒ Thread.sleep(500); Process.emit(i) ++ go(i + 1l) }
        go(1l)
      }

      broadcast(naturals.take(30), subs).runLog.run
      1 === 1
    }
  }
}