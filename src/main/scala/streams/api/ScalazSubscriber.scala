package streams.api

import org.apache.log4j.Logger
import scala.concurrent.SyncVar
import org.reactivestreams.{ Subscription, Subscriber }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }

/**
 * SyncProcessSubscriber is an implementation of Reactive Streams `Subscriber`,
 * it runs synchronously (on the Publisher's thread) and invokes a user-defined method to process each element.
 */
class ScalazSubscriber[T](requestN: Int, val sync: SyncVar[Long], error: AtomicReference[Throwable]) extends Subscriber[T] {
  private val logger = Logger.getLogger("subscriber")
  protected val bs = new AtomicInteger(requestN)
  protected var subscription: Option[Subscription] = None

  //for testing only
  protected val acc = new AtomicInteger(0)

  override def onNext(element: T): Unit = {
    logger.info(s"onNext $element")
    acc.incrementAndGet()
    if (bs.decrementAndGet() == 0) {
      bs.set(requestN)
      subscription.fold(())(_.request(bs.get()))
    }
  }

  protected def resume() = requestN

  override def onError(throwable: Throwable): Unit = {
    if (throwable == null) throw null

    logger.info(s"Error ${throwable.getMessage}")
    subscription = None
    error.set(throwable)
    sync.put(bs.get())
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