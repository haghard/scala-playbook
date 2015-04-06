package streams.api

import java.util.concurrent.ExecutorService
import org.apache.log4j.Logger
import org.reactivestreams.{ Subscription, Subscriber, Publisher }
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.{ Process, Cause, async }

object ProcessPublisher {
  def apply[T](source: Process[Task, T], batchSize: Int)(implicit ex: ExecutorService) =
    new ProcessPublisher[T](source, batchSize)
}

class ProcessPublisher[T] private (source: Process[Task, T], batchSize: Int)(implicit ex: ExecutorService) extends Publisher[T] {
  require(batchSize > 0, "batchSize must be greater than zero!, was: " + batchSize)

  val logger = Logger.getLogger("process-pub")

  val P = Process
  private val signal = async.signalOf(0l)(Strategy.Executor(ex))
  private val signalP = signal.discrete

  private var subscriber: Option[Subscriber[_ >: T]] = None

  private val halter: Cause ⇒ Process[Task, Unit] = {
    case cause @ Cause.End ⇒
      logger.info("Stream is exhausted")
      subscriber.fold(())(_.onComplete())
      signal.close.run
      Process.Halt(cause)
    case cause @ Cause.Kill ⇒
      subscriber.fold(())(_.onComplete())
      signal.close.run
      Process.Halt(cause)
    case cause @ Cause.Error(ex) ⇒
      subscriber.fold(())(_.onComplete())
      signal.close.run
      subscriber.fold(())(_.onError(ex))
      Process.Halt(cause)
  }

  private val secret = new Subscription {
    override def cancel(): Unit = {
      signal.close.runAsync(_ ⇒ ())
    }

    override def request(l: Long): Unit = {
      require(l > 0, s" $subscriber violated the Reactive Streams rule 3.9 by requesting a non-positive number of elements.")
      signal.set(l)
        .runAsync(_ ⇒ logger.info(s"${Thread.currentThread().getName} request: $l"))
    }
  }

  /**
   * It doesn't support dynamic batchSize resizing
   */
  (for {
    batch ← signalP zip source.chunk(batchSize)
    i ← P.emitAll(batch._2)
    r ← P.eval(Task.delay { subscriber.fold(())(_.onNext(i.asInstanceOf[T])) }) /*++ P.eval(Task.delay(Thread.sleep(50)))*/
  } yield r).onHalt(halter).run.runAsync(_ ⇒ ())

  /**
   *
   * @param sub
   */
  override def subscribe(sub: Subscriber[_ >: T]): Unit = {
    subscriber = Option(sub)
    sub.onSubscribe(secret)
  }
}