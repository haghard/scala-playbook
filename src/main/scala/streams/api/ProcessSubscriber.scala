package streams.api

import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }

import org.apache.log4j.Logger
import org.reactivestreams.{ Subscription, Subscriber }

import scala.concurrent.SyncVar

class ProcessSubscriber[T](batchSize: Int, val sync: SyncVar[Long], error: AtomicReference[Throwable]) extends Subscriber[T] {
  val logger = Logger.getLogger("process-sub")
  private val acc = new AtomicInteger(0)
  protected var bs = new AtomicInteger(batchSize)
  protected var subscription: Option[Subscription] = None

  override def onNext(t: T): Unit = {
    logger.info(s"onNext: $t ${##}")
    acc.getAndIncrement()
    bs.decrementAndGet()
    if (bs.get() == 0) {
      bs.set(updateBufferSize)
      subscription.fold(())(_.request(bs.get()))
    }
  }

  protected def updateBufferSize = batchSize

  override def onError(throwable: Throwable): Unit = {
    logger.info(s"Error ${throwable.getMessage}")
    subscription = None
    error.set(throwable)
    sync.put(acc.get())
  }

  override def onSubscribe(sub: Subscription): Unit = {
    subscription = Some(sub)
    sub request bs.get()
  }

  override def onComplete(): Unit = {
    logger.info("ProcessSubscriber onComplete")
    subscription = None
    sync.put(acc.get())
  }
}