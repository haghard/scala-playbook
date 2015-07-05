package streams.api

import org.apache.log4j.Logger
import java.util.concurrent.Executors._
import mongo.MongoProgram.NamedThreadFactory
import java.util.concurrent.ForkJoinPool
import org.reactivestreams.{ Subscription, Subscriber, Publisher }
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.{ sink, Process, Cause, async }

object ScalazProcessPublisher {
  def apply[T](source: Process[Task, T], bound: Long = Long.MaxValue) =
    new ScalazProcessPublisher[T](source, bound)
}

class ScalazProcessPublisher[T] private (source: Process[Task, T], bound: Long) extends Publisher[T] {
  private val P = Process
  private val logger = Logger.getLogger("publisher")

  private val ex = newFixedThreadPool(4, new NamedThreadFactory("publisher-infrastructure"))
  private val I = Strategy.Executor(ex)

  private val signalQ = async.signalOf(Set[async.mutable.Queue[T]]())(I)
  private val subscriptions = async.boundedQueue[Subscriber[_ >: T]](10)(I)

  private val S = new ForkJoinPool(Runtime.getRuntime.availableProcessors() * 2)

  private def publish(a: T): Task[Unit] = for {
    qsList ← signalQ.discrete.filter(s ⇒ s.nonEmpty).take(1).runLog
    qs = qsList.flatMap { _.toList }

    _ ← Task.gatherUnordered(qs.toList.map { q ⇒ q.enqueueOne(a) })
  } yield ()

  private def subscribe0(subscriber: Subscriber[_ >: T]): Task[Process[Task, T]] = for {
    qs ← signalQ.get
    q = async.boundedQueue[T](16)(I)
    updatedQs = qs + q

    result ← signalQ.compareAndSet {
      case Some(`qs`) ⇒ Some(updatedQs)
      case opt        ⇒ opt
    }

    p = q.dequeue.onComplete(P eval_ unsubscribe(q))

    back ← if (result.contains(updatedQs)) Task.delay { logger.info(s"New subscription"); p }
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
      q.close.map(_ ⇒ logger.info(s"unsubscribe from queue $q"))
    else
      unsubscribe(q)
  } yield ()

  val writer = (source.take(bound.toInt) to sink.lift(publish)).drain
    .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
    .onComplete(P.eval {
      for {
        qsList ← signalQ.discrete.take(1).runLog
        qs = qsList.flatMap { _.toList }
        _ = logger.info(s"writer done for qs: ${qs.size}")
        _ ← Task.gatherUnordered(qs.map { _.close })
      } yield ()
    })

  writer.merge(subscriptions.dequeue.map { s ⇒
    val subscription = secret(s, subscribe0(s).run)
    s.onSubscribe(subscription)
  })(I)
    .run
    .runAsync(_ ⇒ ())

  /**
   *
   * @param subscriber
   */
  override def subscribe(subscriber: Subscriber[_ >: T]): Unit = {
    if (subscriber == null)
      subscriber.onError(new NullPointerException("Null subscriber"))

    Task.fork(subscriptions.enqueueOne(subscriber))(ex)
      .runAsync(_ ⇒ ())
  }

  private def secret(s: Subscriber[_ >: T], p: Process[Task, T]) = new Subscription {
    private lazy val chunkedSource = streams.io.chunkR(p)
    private val signalR = async.signalOf(-1)(Strategy.Executor(S))

    private val halter: Cause ⇒ Process[Task, Unit] = {
      case cause @ Cause.End ⇒
        s.onComplete()
        signalR.close.run
        Process.Halt(cause)
      case cause @ Cause.Kill ⇒
        s.onComplete()
        signalR.close.run
        Process.Halt(cause)
      case cause @ Cause.Error(ex) ⇒
        if (ex.getMessage == "IOS") {
          s.onComplete()
          signalR.close.run
          Process.halt
        } else {
          s.onError(ex)
          signalR.close.run
          Process.halt
        }
    }

    (for {
      size ← signalR.discrete.filter(_ > -1)
      _ ← (chunkedSource chunk size) map { seq ⇒
        seq foreach { i ⇒
          s.onNext(i)
          //Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
        }

        if (size > seq.size) {
          logger.info(s"Requested $size - Received ${seq.size} ")
          throw new Exception("IOS")
        }
      }
    } yield ())
      .onHalt(halter).run[Task]
      .runAsync(_ ⇒ logger.info(s"Subscriber $s exit"))

    override def cancel(): Unit = {
      signalR.close.runAsync(_ ⇒ ())
    }

    override def request(n: Long): Unit = {
      if (n < 1) {
        s.onError(
          throw new IllegalArgumentException("Violate the Reactive Streams rule 3.9 by requesting a non-positive number of elements."))
      }
      Task.fork {
        signalR.set(n.toInt)
      }(S).runAsync(_ ⇒ logger.info(s"request $n"))
    }
  }
}