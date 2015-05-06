package streams.api

import java.util.concurrent.ExecutorService
import org.apache.log4j.Logger
import org.reactivestreams.{ Subscription, Subscriber, Publisher }
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.{ Process, Cause, async }

object ProcessPublisher {
  def apply[T](source: Process[Task, T])(implicit ex: ExecutorService) =
    new ProcessPublisher[T](source)
}

class ProcessPublisher[T] private (source: Process[Task, T])(implicit ex: ExecutorService) extends Publisher[T] {
  private val logger = Logger.getLogger("process-pub")

  val P = Process
  private val signal = async.signalOf(0)(Strategy.Executor(ex))
  private val signalP = signal.discrete

  private var subscriber: Option[Subscriber[_ >: T]] = None

  private val halter: Cause ⇒ Process[Task, Unit] = {
    case cause @ Cause.End ⇒
      subscriber.fold(())(_.onComplete())
      signal.close.run
      Process.Halt(cause)
    case cause @ Cause.Kill ⇒
      subscriber.fold(())(_.onComplete())
      signal.close.run
      Process.Halt(cause)
    case cause @ Cause.Error(ex) ⇒
      if (ex.getMessage == "IOF") {
        subscriber.fold(())(_.onComplete())
        signal.close.run
      } else {
        subscriber.fold(())(_.onError(ex))
        signal.close.run
      }
      Process.Halt(cause)
  }

  private val secret = new Subscription {
    override def cancel(): Unit = {
      signal.close.runAsync(_ ⇒ ())
    }

    override def request(l: Long): Unit = {
      require(l > 0, s" $subscriber violated the Reactive Streams rule 3.9 by requesting a non-positive number of elements.")
      signal.set(l.toInt)
        .runAsync(_ ⇒ logger.info(s"request: $l"))
    }
  }

  val chunkedSource = streams.io.chunkR(source)
  (for {
    reqSize ← signalP.filter(_ > 0)
    batch ← chunkedSource chunk reqSize
    r ← P.emitAll(batch).flatMap(i ⇒ P.eval(Task.delay { subscriber.fold(())(_.onNext(i)) }))
  } yield {
    if (batch.size != reqSize.toInt) throw new Exception("IOF")
  }).onHalt(halter).run[Task].runAsync(_ ⇒ ())

  /**
   *
   * @param sub
   */
  override def subscribe(sub: Subscriber[_ >: T]): Unit = {
    subscriber.fold { subscriber = Option(sub); sub.onSubscribe(secret) } { r ⇒
      throw new IllegalStateException("Only one subscription is available")
    }
  }
}