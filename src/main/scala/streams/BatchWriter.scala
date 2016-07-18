package streams

import akka.actor.{ ActorLogging, Props }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import streams.BatchWriter.WriterDone

import scala.annotation.tailrec
import scala.collection.mutable
import scalaz.\/-

object BatchWriter {
  case object WriterDone
  def props[T] = Props[BatchWriter[T]]
}

/**
 * We start buffering incoming data `WriteRequest` until last request size.
 * After we exceed last request size we will send it to the subscriber (batching),
 * so we are limited by last request size
 * @tparam T
 */
class BatchWriter[T] extends ActorPublisher[T] with ActorLogging {
  private var pubSubGap = 0l
  private var bufferSize = 0l
  private var active = false
  private val buffer = mutable.Queue[T]()
  private var lastReq: Option[WriteRequest[T]] = None

  override def receive: Receive = {
    case WriterDone ⇒
      while (bufferSize > 0) {
        onNext(buffer.dequeue)
        bufferSize -= 1
      }
      if (buffer.size == 0) {
        onComplete()
        context.stop(self)
      } else {
        log.info("Smth gonna went wrong {}", buffer.size)
      }

    case r: WriteRequest[T] ⇒
      if (active) {
        if (bufferSize < pubSubGap) {
          r.cb(\/-(r.i))
          log.info("enqueue: {}", r.i)
          buffer.enqueue(r.i)
          bufferSize += 1
        } else {
          lastReq = Option(r)
          deliverBatch()
        }
      } else {
        lastReq = Option(r)
        buffer.enqueue(r.i)
      }

    case Request(n) if (isActive && totalDemand > 0) ⇒
      log.info("request: {}", n)
      pubSubGap += n

      //notify outSide producer
      if (active) {
        for { r ← lastReq } { r.cb(\/-(r.i)); lastReq = None }
      } else {
        for { r ← lastReq } {
          r.cb(\/-(r.i))
          lastReq = None
          onNext(buffer.dequeue())
          pubSubGap -= 1
        }
        active = true
      }

    case Cancel ⇒
      context.stop(self)
  }

  final def deliverBatch(): Unit = {
    @tailrec def loop(bs: Long, gap: Long): (Long, Long) =
      if (bs > 0) {
        onNext(buffer.dequeue())
        loop(bs - 1, gap - 1)
      } else (bs, gap)

    val (b, g) = loop(bufferSize, pubSubGap)
    bufferSize = b
    pubSubGap = g
  }
}
