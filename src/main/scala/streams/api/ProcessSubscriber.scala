package streams.api

import org.apache.log4j.Logger
import org.reactivestreams.{ Subscription, Subscriber }

import scala.concurrent.SyncVar

class ProcessSubscriber[T](batchSize: Int, sync: SyncVar[Boolean]) extends Subscriber[T] {
  val logger = Logger.getLogger("process-sub")

  private var bs = batchSize
  protected var subscription: Option[Subscription] = None

  override def onNext(t: T): Unit = {
    logger.info(s"${Thread.currentThread().getName} onNext: $t")
    bs -= 1
    if (bs == 0) {
      bs = batchSize
      subscription.fold(())(_.request(bs))
    }
  }

  override def onError(throwable: Throwable): Unit = {
    logger.info(s"Error ${throwable.getMessage}")
    subscription = None
  }

  override def onSubscribe(sub: Subscription): Unit = {
    subscription = Some(sub)
    sub.request(batchSize)
  }

  override def onComplete(): Unit = {
    logger.info("ProcessSubscriber onComplete")
    subscription = None
    sync.put(true)
  }
}