package streams

import akka.actor.{ Props, ActorLogging }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor.{ OneByOneRequestStrategy, ActorSubscriber }
import scalaz.\/-

object OneByOneSinkSubscriber {
  def props[T] = Props[OneByOneSinkSubscriber[T]]
}

class OneByOneSinkSubscriber[T] extends ActorSubscriber with ActorLogging {

  private var lastVal: Option[T] = None

  override val requestStrategy = OneByOneRequestStrategy

  override def receive: Receive = {
    case r: ReadElement[T] ⇒
      log.info("ReadData")
      for {
        v ← lastVal
      } {
        r.cb(\/-(v))
        lastVal = None
      }

    case OnNext(element: T) ⇒
      log.info("OnNext")
      lastVal = Some(element)

    case OnComplete ⇒
      log.info("OnComplete")
      context.stop(self)

    case OnError(ex) ⇒
      log.info("OnError {}", ex)
      context.stop(self)
  }
}