package streams

import scalaz.\/-
import akka.actor.{ ActorLogging, Props }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor.{ MaxInFlightRequestStrategy, ActorSubscriber }

object SinkBatchedSubscriber {
  case class UndeliveredSize(n: Int)
  def props[T](batchSize: Int) = Props(new SinkBatchedSubscriber[T](batchSize))
}

class SinkBatchedSubscriber[T](batchSize: Int) extends ActorSubscriber with ActorLogging {
  private var bs = 0
  private var v = Vector[T]()

  override protected def requestStrategy = new MaxInFlightRequestStrategy(batchSize + 1) {
    override def inFlightInternally: Int = bs
  }

  override def receive: Receive = {
    case r: ReadBatchData[T] ⇒
      r.cb(\/-(v))
      v = Vector.empty[T]
      bs = 0
      log.info("Flush batch")

    case OnNext(element: T) ⇒
      log.info("OnNext {}", element)
      v = v :+ element
      bs += 1

    case OnComplete ⇒
      log.info("OnComplete with {} undelivered size ", v.size)

    case OnError(ex) ⇒
      log.info("OnError {}", ex)
  }
}