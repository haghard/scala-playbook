package streams

import akka.actor.{ ActorLogging, Props }
import akka.stream.actor.ActorSubscriberMessage.{ OnComplete, OnError, OnNext }
import akka.stream.actor.{ ActorSubscriber, OneByOneRequestStrategy }

import scala.collection._
import scalaz.\/-

object Reader {
  def props[T] = Props(new Reader[T])
}

class Reader[T] extends ActorSubscriber with ActorLogging {
  private val buffer = mutable.Queue[T]()
  private val bufferReq = mutable.Queue[ReadElement[T]]()

  override protected def requestStrategy = OneByOneRequestStrategy

  override def receive: Receive = {
    case OnNext(element: T) ⇒
      //Thread.sleep(50) // for test
      if (bufferReq.isEmpty) {
        buffer.enqueue(element)
      } else {
        val r = bufferReq.dequeue()
        r.cb(\/-(element))
      }

    case OnComplete ⇒ context.become(finish)

    case OnError(ex) ⇒
      log.info("Error {}", ex)
      context.stop(self)

    case r: ReadElement[T] ⇒
      if (buffer.isEmpty) {
        bufferReq.enqueue(r)
      } else {
        r.cb(\/-(buffer.dequeue()))
      }
  }

  def finish: Receive = {
    case r: ReadElement[T] ⇒
      if (buffer.isEmpty) {
        context.stop(self)
      } else r.cb(\/-(buffer.dequeue()))
  }
}
