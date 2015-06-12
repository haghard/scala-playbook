package streams

import akka.actor.{ Props, ActorLogging }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }

import scalaz.\/-

object BoundedPublisher {
  def props[T] = Props[BoundedPublisher[T]]
}

class BoundedPublisher[T] extends ActorPublisher[T] with ActorLogging {
  private var pubSubGap = 0l
  private var lastReq: Option[WriteRequest[T]] = None

  override def receive: Receive = {
    case r: WriteRequest[T] ⇒
      if (pubSubGap > 0) {
        pubSubGap -= 1
        onNext(r.i)
        r.cb(\/-(r.i))
      } else {
        lastReq = Some(r)
      }

    case Request(n) if (isActive && totalDemand > 0) ⇒
      log.info("Request: {}", n)
      pubSubGap += n

      for {
        r ← lastReq
      } {
        onNext(r.i)
        pubSubGap -= 1
        r.cb(\/-(r.i))
        lastReq = None
      }

    case Cancel ⇒
  }
}
