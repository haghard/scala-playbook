package streams

import akka.actor.{ Props, ActorLogging }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }

object OneByOneSourcePublisher {
  def props[T] = Props[OneByOneSourcePublisher[T]]
}

class OneByOneSourcePublisher[T] extends ActorPublisher[T] with ActorLogging {
  import scalaz.syntax.either._
  private var lastReq: Option[WriteRequest[T]] = None

  override def receive: Receive = {
    case r: WriteRequest[T] ⇒
      lastReq = Option(r)
      onNext(r.i)

    case Request(n) if (isActive && totalDemand > 0) ⇒
      log.info("Request {}", n)
      for {
        r ← lastReq
      } {
        r.cb(().right)
        lastReq = None
      }
    case Cancel ⇒
      log.info("Cancel")
      context.stop(self)
  }
}
