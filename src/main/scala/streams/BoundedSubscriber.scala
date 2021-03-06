package streams

import scalaz.\/-
import akka.stream.actor.ActorSubscriber
import akka.actor.{ ActorLogging, Props }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }

object BoundedSubscriber {
  def props[T](batchSize: Int) = Props(new BoundedSubscriber[T](batchSize))
}

/*
  The provided WatermarkRequestStrategy is a good strategy if the actor performs work itself.

  The provided MaxInFlightRequestStrategy is useful if messages are queued internally or delegated
    to other actors.

  You can also implement a custom RequestStrategy or call request manually together with
  ZeroRequestStrategy or some other strategy. In that case you must also call request when the
  actor is started or when it is ready, otherwise it will not receive any elements.
*/
class BoundedSubscriber[T](batchSize: Int) extends ActorSubscriber with ActorLogging {
  private var bufferSize = 0
  private var buffer = Vector[T]()

  override protected def requestStrategy = akka.stream.actor.ZeroRequestStrategy

  /*new MaxInFlightRequestStrategy(batchSize) {
      override def inFlightInternally: Int = bs
    }*/

  override def preStart = request(batchSize)

  override def receive: Receive = {
    case r: ReadBatchData[T] ⇒
      log.info("Flush batch size {}", bufferSize)
      r.cb(\/-(buffer))
      buffer = Vector.empty[T]
      bufferSize = 0
      request(batchSize)

    case OnNext(element: T) ⇒
      log.info("out: {}", element)
      buffer = buffer :+ element
      bufferSize += 1
      if (bufferSize == 0) request(batchSize)

    case OnComplete ⇒
      log.info("OnComplete with {} undelivered size ", buffer.size)

    case OnError(ex) ⇒
      log.info("OnError {}", ex)
  }
}