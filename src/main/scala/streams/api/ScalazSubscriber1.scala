package streams.api

import org.apache.log4j.Logger
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import org.reactivestreams.{ Subscription, Subscriber }

/**
 * SyncProcessSubscriber1 is an implementation of Reactive Streams `Subscriber`,
 * it runs synchronously (on the Publisher's thread) and invokes a user-defined method to process each element.
 */
class ScalazSubscriber1[T](val latch: CountDownLatch, batchSize: Int) extends Subscriber[T] {
  private val logger = Logger.getLogger("subscriber")

  private val i = new AtomicInteger(0)
  protected val bs = new AtomicInteger(batchSize)
  protected var subscription: Option[Subscription] = None

  override def onNext(t: T): Unit = {
    logger.info(s"onNext: $t")
    if (i.getAndIncrement() == 100) {
      subscription.fold(())(_.cancel())
      latch.countDown()
    }

    bs.decrementAndGet()
    if (bs.get() == 0) {
      bs.set(updateBufferSize())
      subscription.fold(())(_.request(bs.get()))
    }
  }

  protected def updateBufferSize() = batchSize

  override def onError(throwable: Throwable): Unit = {
    logger.info(s"Error ${throwable.getMessage}")
    subscription = None
  }

  override def onSubscribe(sub: Subscription): Unit = {
    if (sub == null) throw null

    if (subscription.isDefined) {
      sub.cancel()
    } else {
      subscription = Some(sub)
      sub request bs.get()
    }
  }

  override def onComplete(): Unit = {
    logger.info("onComplete")
    subscription = None
  }
}