package streams

import akka.actor.{ Props, ActorLogging }
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor._
import scala.collection.mutable

object ProcessorSink {
  def props[I] = Props[ProcessorSink[I]]
}

/**
 * This actor reflects back-pressure semantic provided by RequestStrategy
 * back to underlying pull based scalaz process
 * Gives scalaz process acknowledge with Unit
 *
 * @param I
 */
class ProcessorSink[I] extends ActorSubscriber with ActorPublisher[I] with ActorLogging {
  private var pubSubDistance = 0l
  private var bufferSize = 0l

  private val buffer = mutable.Queue[I]() //controlled size
  private var lastReq: Option[WriteRequest[I]] = None

  override val requestStrategy = new WatermarkRequestStrategy(15, 10)

  override def receive = subscriberOps orElse publisherOps
  import scalaz.syntax.either._

  val subscriberOps: Receive = {
    case OnNext(element: I) ⇒
      pubSubDistance -= 1
      log.info("Delivered sub element: - {} Demand: {}", element, pubSubDistance)
      Thread.sleep(100)

    case OnComplete ⇒
      onComplete()
      log.info("OnComplete")
      context.stop(self)

    case OnError(ex) ⇒
      onError(ex)
      log.info("OnError {}", ex)
      context.stop(self)
  }

  val publisherOps: Receive = {
    case r: WriteRequest[I] ⇒
      if (bufferSize < pubSubDistance) {
        r.cb(().right) //do acking
        buffer.enqueue(r.i)
        log.info("Buffer element: {} Demand: {}", r.i, pubSubDistance)
        bufferSize += 1
      } else {
        lastReq = Option(r)
        while (bufferSize > 0) {
          onNext(buffer.dequeue())
          bufferSize -= 1
        }
      }
      Thread.sleep(100)

    case Request(n) if (isActive && totalDemand > 0) ⇒
      pubSubDistance += n
      log.info("Request: {} - {}", n, pubSubDistance)

      //notify outSide producer
      for {
        r ← lastReq
      } {
        r.cb(().right)
        lastReq = None
      }

    case Cancel ⇒
      cancel()
      log.info("Cancel")
  }
}