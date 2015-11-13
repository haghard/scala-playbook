package streams.api

import org.apache.log4j.Logger
import java.util.concurrent.Executors._
import java.util.concurrent.ExecutorService
import scalaz.concurrent.Task
import mongo.MongoProgram.NamedThreadFactory
import scalaz.stream.{ sink, Process, Cause, async }
import org.reactivestreams.{ Subscription, Subscriber, Publisher }
import scalaz.concurrent.Strategy

object ScalazPublisher {
  def apply[T](source: Process[Task, T])(implicit ex: ExecutorService) =
    new ScalazPublisher[T](source)

  def bounded[T](s: Process[Task, T], bound: Long = Long.MaxValue)(implicit ex: ExecutorService) =
    new ScalazPublisher[T](s take bound.toInt)

  trait FailedPublisher[T] extends Publisher[T] {
    abstract override def subscribe(subscriber: Subscriber[_ >: T]): Unit = {
      super.subscribe(subscriber)
      subscriber.onError(new RuntimeException("Can't subscribe subscriber"))
    }
  }

  def failedOnSubscribe[T](source: Process[Task, T])(implicit ex: ExecutorService) =
    new ScalazPublisher[T](source) with FailedPublisher[T]
}

class ScalazPublisher[T] private (source: Process[Task, T])(implicit ex: ExecutorService) extends Publisher[T] {
  private val P = Process
  private val logger = Logger.getLogger("publisher")
  private val executor = newFixedThreadPool(2, new NamedThreadFactory("infrastructure"))
  private val Ex = Strategy.Executor(executor)
  private val subscriptions = async.signalOf(Set[async.mutable.Queue[T]]())(Ex)
  private val requests = async.boundedQueue[Subscriber[_ >: T]](1 << 4)(Ex)

  private def publish(a: T): Task[Unit] = for {
    qsList ← subscriptions.discrete.filter(s ⇒ s.nonEmpty).take(1).runLog
    qs = qsList.flatMap { _.toList }
    _ ← Task.gatherUnordered(qs.toList.map { q ⇒ q.enqueueOne(a) })
  } yield ()

  private def register(subscriber: Subscriber[_ >: T]): Task[Process[Task, T]] = for {
    qs ← subscriptions.get
    q = async.boundedQueue[T](1 << 5)(Strategy.Executor(ex))
    updatedQs = qs + q

    result ← subscriptions.compareAndSet {
      case Some(`qs`) ⇒ Some(updatedQs)
      case opt        ⇒ opt
    }

    p = q.dequeue.onComplete(P eval_ unsubscribe(q))

    back ← if (result contains updatedQs) Task.delay { logger.info("new registration"); p }
    else register(subscriber)
  } yield back

  private def unsubscribe(q: async.mutable.Queue[T]): Task[Unit] = for {
    qs ← subscriptions.get
    updatedQs = qs - q

    result ← subscriptions compareAndSet {
      case Some(`qs`) ⇒ Some(updatedQs)
      case opt        ⇒ opt
    }

    _ ← if (result.contains(updatedQs))
      q.close.map(_ ⇒ logger.info(s"unsubscribe"))
    else
      unsubscribe(q)
  } yield ()

  private val finalizer: Process[Task, Unit] =
    P.eval_ {
      for {
        qsList ← subscriptions.discrete.take(1).runLog
        qs = qsList.flatMap {
          _.toList
        }
        _ = logger.info(s"writer has done for qs: ${qs.size}")
        _ ← Task.gatherUnordered(qs.map(_.close))
      } yield ()
    }

  private def publisher = (source to sink.lift(publish)).drain.onComplete(finalizer)

  private def flow = (publisher merge (requests.dequeue.map { s ⇒
    if (s eq null) s.onError(new NullPointerException("Empty subscriber"))
    val subscription = createSubscription(s, register(s).run)
    s.onSubscribe(subscription)
  }))(Ex)

  flow.run.runAsync(_ ⇒ ())

  override def subscribe(subscriber: Subscriber[_ >: T]): Unit =
    Task.fork(requests.enqueueOne(subscriber))(executor)
      .runAsync(_ ⇒ ())

  private def createSubscription(s: Subscriber[_ >: T], p: Process[Task, T]) = new Subscription {
    private lazy val source = streams.io.chunkR(p)
    private val requests = async.signalOf(-1)(Strategy.Executor(ex))

    private val finalizer: Cause ⇒ Process[Task, Unit] = {
      case cause @ Cause.End ⇒
        s.onComplete()
        requests.close.run
        Process.Halt(cause)
      case cause @ Cause.Kill ⇒
        s.onComplete()
        requests.close.run
        Process.Halt(cause)
      case cause @ Cause.Error(_) ⇒
        if (cause.rsn.getMessage == streams.io.exitMessage) s.onComplete()
        else s.onError(cause.rsn)
        requests.close.run
        Process.halt
    }

    (for {
      size ← requests.discrete.filter(_ > -1)
      _ ← (source chunk size) map { batch ⇒
        batch foreach (s.onNext(_))
        if (size > batch.size) { //controversial behavior
          logger.info(s"requested $size but available only ${batch.size}")
          throw new Exception(streams.io.exitMessage)
        }
      }
    } yield ())
      .onHalt(finalizer)
      .run[Task]
      .runAsync(_ ⇒ logger.info(s"Subscriber $s is gone"))

    override def cancel(): Unit =
      requests.close.runAsync(_ ⇒ ())

    override def request(n: Long): Unit = {
      if (n < 1)
        s.onError(new IllegalArgumentException(
          "Violate the Reactive Streams rule 3.9 by requesting a non-positive number of elements."))

      Task.fork(requests.set(n.toInt))(ex).runAsync(_ ⇒ logger.info(s"request $n"))
    }
  }
}