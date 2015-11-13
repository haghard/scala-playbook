package processes

import java.util.concurrent.Executors._
import java.util.concurrent.ForkJoinPool

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger

import scalaz.concurrent.Task
import scalaz.stream.Process._
import org.specs2.mutable.Specification
import scalaz.stream._
import scalaz.concurrent.Strategy

class BroadcastSpec extends Specification {
  val P = Process
  val logger = Logger.getLogger("broadcast")

  /**
   * Dynamic broadcast
   *
   */
  def broadcast[A](sourceP: Process[Task, A], subs: Process[Task, String],
                   actions: Map[String, (A, String) ⇒ String],
                   bound: Int = 10): Process[Task, Unit] = Process.suspend {
    import scalaz.stream.io
    val Pub = newFixedThreadPool(2, new NamedThreadFactory("publisher")) // publish and fork new consumers
    val I = Strategy.Executor(Pub)
    val S = newFixedThreadPool(1, new NamedThreadFactory("signal"))
    val SIG = Strategy.Executor(S)

    val Q = new ForkJoinPool(2) // could be extended depends on consumer's size
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

      p = q.dequeue.onComplete(P eval_ unsubscribe(q))

      back ← if (result contains updatedQs) Task.delay { logger.info(s"New subscription from $name"); p }
      else subscribe(name)
    } yield back

    def publisher = (sourceP to sink.lift(publish)).drain

    def subscriber = subs.map { n ⇒
      Task.fork {
        for {
          s ← subscribe(n)
          _ ← (s.map(elem ⇒ actions(n)(elem, n)) to io.stdOutLines).run[Task]
        } yield ()
      }(Pub).runAsync(_ ⇒ ())
    }

    (publisher merge subscriber)(I)
      .onComplete(P.eval_(signal.set(Set()).map(_ ⇒ logger.info("Broadcast flow has been completed"))))
  }

  "Dynamic broadcast" should {
    "run with multiple subscribers" in {
      val subscriberNames: Process[Task, String] = P.emitAll(Seq("Doctor Who", "Sherlock", "Ironman", "Superman"))
      val subs = (subscriberNames zip P.repeatEval(Task.delay(Thread.sleep(4000)))).map(_._1)

      def naturals: Process[Task, Long] = {
        def go(i: Long): Process[Task, Long] =
          P.await(Task.delay(i)) { i ⇒ Thread.sleep(500); P.emit(i) ++ go(i + 1l) }
        go(1l)
      }

      def print = { (elem: Long, name: String) ⇒ s"$name in [${Thread.currentThread.getName}] has got $elem" }
      val actions = Map("Doctor Who" -> print, "Sherlock" -> print, "Ironman" -> print, "Superman" -> print)

      broadcast((naturals take 30), subs, actions).runLog.run
      1 === 1
    }
  }
}