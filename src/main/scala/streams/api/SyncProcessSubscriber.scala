package streams.api

import org.apache.log4j.Logger
import scala.concurrent.SyncVar
import org.reactivestreams.{ Subscription, Subscriber }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }

/**
 * SyncProcessSubscriber is an implementation of Reactive Streams `Subscriber`,
 * it runs synchronously (on the Publisher's thread) and invokes a user-defined method to process each element.
 */
class SyncProcessSubscriber[T](batchSize: Int, val sync: SyncVar[Long], error: AtomicReference[Throwable]) extends Subscriber[T] {
  private val logger = Logger.getLogger("process-sub")
  private val acc = new AtomicInteger(0)
  protected val bs = new AtomicInteger(batchSize)
  protected var subscription: Option[Subscription] = None

  override def onNext(t: T): Unit = {
    logger.info(s"onNext: $t")

    acc.getAndIncrement()
    bs.decrementAndGet()
    if (bs.get() == 0) {
      bs.set(updateBufferSize())
      subscription.fold(())(_.request(bs.get()))
    }
  }

  protected def updateBufferSize() = batchSize

  override def onError(throwable: Throwable): Unit = {
    if (throwable == null) throw null

    logger.info(s"Error ${throwable.getMessage}")
    subscription = None
    error.set(throwable)
    sync.put(acc.get())
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
    sync.put(acc.get())
  }
}