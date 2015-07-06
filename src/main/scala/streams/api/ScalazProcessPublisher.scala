package streams.api

import org.apache.log4j.Logger
import java.util.concurrent.Executors._
import java.util.concurrent.ExecutorService
import scalaz.concurrent.{ Task, Strategy }
import mongo.MongoProgram.NamedThreadFactory
import scalaz.stream.{ sink, Process, Cause, async }
import org.reactivestreams.{ Subscription, Subscriber, Publisher }

object ScalazProcessPublisher {
  def apply[T](source: Process[Task, T])(implicit ex: ExecutorService) =
    new ScalazProcessPublisher[T](source)

  /**
   * For Tck spec
   * @param bound
   * @param ex
   * @tparam T
   * @return
   */
  def bounded[T](s: Process[Task, T], bound: Long = Long.MaxValue)(implicit ex: ExecutorService) =
    new ScalazProcessPublisher[T](s.take(bound.toInt))

  trait FailedPublisher[T] extends Publisher[T] {
    abstract override def subscribe(subscriber: Subscriber[_ >: T]): Unit = {
      super.subscribe(subscriber)
      subscriber.onError(new RuntimeException("Can't subscribe subscriber"))
    }
  }

  def failedOnSubscribe[T](source: Process[Task, T])(implicit ex: ExecutorService) =
    new ScalazProcessPublisher[T](source) with FailedPublisher[T]
}

trait WritablePublisher[T] extends Publisher[T] {
  def source: Process[Task, T]
  def flow: Process[Task, Unit]
}

class ScalazProcessPublisher[T] private (val source: Process[Task, T])(implicit ex: ExecutorService) extends WritablePublisher[T] {
  private val P = Process
  private val logger = Logger.getLogger("publisher")

  private val infrastructureE = newFixedThreadPool(4, new NamedThreadFactory("publisher-infrastructure"))
  private val infrastructureI = Strategy.Executor(infrastructureE)

  private val signalQ = async.signalOf(Set[async.mutable.Queue[T]]())(infrastructureI)
  private val subscriptions = async.boundedQueue[Subscriber[_ >: T]](10)(infrastructureI)

  def publish(a: T): Task[Unit] = for {
    qsList ← signalQ.discrete.filter(s ⇒ s.nonEmpty).take(1).runLog
    qs = qsList.flatMap { _.toList }

    _ ← Task.gatherUnordered(qs.toList.map { q ⇒ q.enqueueOne(a) })
  } yield ()

  private def subscribe0(subscriber: Subscriber[_ >: T]): Task[Process[Task, T]] = for {
    qs ← signalQ.get
    q = async.boundedQueue[T](32)(infrastructureI)
    updatedQs = qs + q

    result ← signalQ.compareAndSet {
      case Some(`qs`) ⇒ Some(updatedQs)
      case opt        ⇒ opt
    }

    p = q.dequeue.onComplete(P eval_ unsubscribe(q))

    back ← if (result.contains(updatedQs)) Task.delay { logger.info(s"new subscription"); p }
    else subscribe0(subscriber)
  } yield back

  private def unsubscribe(q: async.mutable.Queue[T]): Task[Unit] = for {
    qs ← signalQ.get
    updatedQs = qs - q

    result ← signalQ compareAndSet {
      case Some(`qs`) ⇒ Some(updatedQs)
      case opt        ⇒ opt
    }

    _ ← if (result.contains(updatedQs))
      q.close.map(_ ⇒ logger.info(s"unsubscribe from queue"))
    else
      unsubscribe(q)
  } yield ()

  val finalizer: Process[Task, Unit] =
    P.eval(
      for {
        qsList ← signalQ.discrete.take(1).runLog
        qs = qsList.flatMap { _.toList }
        _ = logger.info(s"writer done for qs: ${qs.size}")
        _ ← Task.gatherUnordered(qs.map { _.close })
      } yield ()
    )

  val writer = (source to sink.lift(publish)).drain.onComplete(finalizer)

  override val flow = writer.merge(subscriptions.dequeue.map { s ⇒
    if (s == null) s.onError(new NullPointerException("Null subscriber"))
    val subscription = secret(s, subscribe0(s).run)
    s.onSubscribe(subscription)
  })(infrastructureI)

  flow.run.runAsync(_ ⇒ ())

  /**
   *
   * @param subscriber
   */
  override def subscribe(subscriber: Subscriber[_ >: T]): Unit = {
    Task.fork(subscriptions.enqueueOne(subscriber))(infrastructureE)
      .runAsync(_ ⇒ ())
  }

  private def secret(s: Subscriber[_ >: T], p: Process[Task, T]) = new Subscription {
    private lazy val CSource = streams.io.chunkR(p)
    private val signalR = async.signalOf(-1)(Strategy.Executor(ex))

    private val halter: Cause ⇒ Process[Task, Unit] = {
      case cause @ Cause.End ⇒
        s.onComplete()
        signalR.close.run
        Process.Halt(cause)
      case cause @ Cause.Kill ⇒
        s.onComplete()
        signalR.close.run
        Process.Halt(cause)
      case cause @ Cause.Error(_) ⇒
        if (cause.rsn.getMessage == streams.io.gracefulExitMessage) s.onComplete()
        else s.onError(cause.rsn)
        signalR.close.run
        Process.halt
    }

    (for {
      size ← signalR.discrete.filter(_ > -1)
      _ ← (CSource chunk size) map { seq ⇒
        seq foreach { i ⇒
          s.onNext(i)
          //Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
        }
        if (size > seq.size) {
          logger.info(s"Requested $size - Received ${seq.size} ")
          throw new Exception(streams.io.gracefulExitMessage)
        }
      }
    } yield ())
      .onHalt(halter)
      .run[Task]
      .runAsync(_ ⇒ logger.info(s"Subscriber $s is gone"))

    override def cancel(): Unit = {
      signalR.close.runAsync(_ ⇒ ())
    }

    override def request(n: Long): Unit = {
      if (n < 1) {
        s.onError(new IllegalArgumentException(
          "Violate the Reactive Streams rule 3.9 by requesting a non-positive number of elements."))
      }
      Task.fork {
        signalR.set(n.toInt)
      }(ex).runAsync(_ ⇒ logger.info(s"request $n"))
    }
  }
}