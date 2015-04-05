package streams

import akka.actor.{ Props, ActorLogging }
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor._
import scala.annotation.tailrec
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
  private var pubSubGap = 0l
  private var bufferSize = 0l

  private val buffer = mutable.Queue[I]() //controlled size
  private var lastReq: Option[WriteRequest[I]] = None

  override val requestStrategy = new WatermarkRequestStrategy(15, 10)

  override def receive = subscriberOps orElse publisherOps
  import scalaz.syntax.either._

  val subscriberOps: Receive = {
    case OnNext(element: I) ⇒
      pubSubGap -= 1
      log.info("Delivered sub element: - {} Demand: {}", element, pubSubGap)
    //Thread.sleep(50) // for test purpose

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
    case r: WriteRequest[I] @unchecked ⇒
      if (bufferSize < pubSubGap) {
        r.cb(().right) //do acking
        buffer.enqueue(r.i)
        log.info("Buffer element: {} Demand: {}", r.i, pubSubGap)
        bufferSize += 1
      } else {
        lastReq = Option(r)
        deliverBatch
      }
    //Thread.sleep(50) // for test purpose

    case Request(n) if (isActive && totalDemand > 0) ⇒
      pubSubGap += n
      log.info("Request: {} - {}", n, pubSubGap)

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

  final def deliverBatch(): Unit = {
    @tailrec
    def loop(bs: Long): Long = {
      if (bs > 0) {
        onNext(buffer.dequeue())
        loop(bs - 1)
      } else bs
    }

    bufferSize = loop(bufferSize)
  }
}