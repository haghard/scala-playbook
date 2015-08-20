package streams

import java.net.{ InetAddress, InetSocketAddress }
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel

import akka.actor.{Actor, Props, ActorSystem}
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.actor.ActorSubscriberMessage.{ OnComplete, OnNext }
import akka.stream.actor.{ActorPublisher, OneByOneRequestStrategy, ActorSubscriber}
import akka.stream.{ OverflowStrategy, ActorMaterializer }
import akka.stream.scaladsl._
import scala.concurrent.duration._
import scala.language.postfixOps

import scala.concurrent.duration.FiniteDuration

package object backpressure {

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()
  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.171"), 8125)
  case class Tick()

  /**
   * 1. Fast publisher, Faster consumer
   * - publisher with a map to send, and a throttler (e.g 50 msg/s)
   * - Result: publisher and consumer rates should be equal.
   */
  def scenario1: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder ⇒

      import FlowGraph.Implicits._
      // get the elements for this flow.
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 20000, "fastProducer")
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingActor], "fastSink", statsD))

      // connect source to sink
      source ~> fastSink
    }
  }

  /**
   * 2. Fast publisher, fast consumer in the beginning get slower, no buffer
   * - same publisher as step 1. (e.g 50msg/s)
   * - consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher and consumer will start at same rate. Publish rate will go down
   * together with publisher rate.
   * @return
   */
  def scenario2: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder ⇒

      import FlowGraph.Implicits._

      // get the elements for this flow.
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 10000, "fastProducer")
      val slowingSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowingDownSink", statsD, 10l))

      // connect source to sink
      source ~> slowingSink
    }
  }

  /**
   * 3. Fast publisher, fast consumer in the beginning get slower, with drop buffer
   * - same publisher as step 1. (e.g 50msg/s)
   * - consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher stays at the same rate, consumer starts dropping messages
   */
  def scenario3: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder ⇒

      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 30 milliseconds, 6000, "fastProducer")
      val slowingSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowingDownSinkWithBuffer", statsD, 20l))

      // now get the buffer, with 100 messages, which overflow
      // strategy that starts dropping messages when it is getting
      // too far behind.
      val buffer = Flow[Int].buffer(1000, OverflowStrategy.dropHead)
      //val buffer = Flow[Int].buffer(1000, OverflowStrategy.backpressure)

      // connect source to sink with additional step
      source ~> buffer ~> slowingSink
    }
  }

  /**
   * 4. Fast publisher, 2 fast consumers, one consumer which gets slower
   * - Result: publisher rate and all consumer rates go down at the same time
   */
  def scenario4: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder ⇒

      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 9000, "fastProducer")

      // and the sinks
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingActor], "broadcast_fastsink", statsD, 0l))
      val slowingDownSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "broadcast_slowsink", statsD, 20l))

      // and the broadcast
      val broadcast = builder.add(Broadcast[Int](2))

      // use a broadcast to split the stream
      source ~> broadcast ~> fastSink
      broadcast ~> slowingDownSink
    }
  }

  /**
   * 5. Fast publisher, 2 fast consumers, one consumer which gets slower but has buffer with drop
   * - Result: publisher rate and fast consumer rates stay the same. Slow consumer goes down.
   */
  def scenario5: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder ⇒

      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 9000, "fastProducer")

      // and the sinks
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingActor], "fastSink", 0l))
      val slowingDownSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowSink", 30l))

      val buffer = Flow[Int].buffer(1000, OverflowStrategy.dropTail)
      //val buffer = Flow[Int].buffer(3500, OverflowStrategy.backpressure)

      // and the broadcast
      val broadcast = builder.add(Broadcast[Int](2))

      // connect source to sink with additional step
      source ~> broadcast ~> fastSink
      broadcast ~> buffer ~> slowingDownSink
    }
  }

  /**
   * 6. Fast publisher (100msg/s), 2 consumer which total 70msg/s, one gets slower with balancer
   * - Result: slowly more will be processed by fast one. When fast one can't keep up, publisher
   * will slow down*
   */
  def scenario6: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder ⇒

      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 10 milliseconds, 500000, "fastProducer")

      // and the sin
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingActor], "fastSinkWithBalancer", statsD, 12l))
      val slowingDownSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowingDownWithBalancer", statsD, 14l, 1l))
      val balancer = builder.add(Balance[Int](2))

      // connect source to sink with additional step
      source ~> balancer ~> fastSink
      balancer ~> slowingDownSink
    }
  }

  /**
   * Merge[In] fan-in operation – (N inputs, 1 output)
   * Several sources with different rates fan-in in single merge junction followed by sink
   * -Result: Sink rate = sum(sources)
   * @return
   */
  def scenario7: RunnableGraph[Unit] = {

    val queryStreams = Source() { implicit b ⇒
      import FlowGraph.Implicits._
      val streams = List("okcStream","houStream","miaStream", "sasStream")
      val latencies = List(20l, 30l, 40l, 45l).iterator

      val merge = b.add(Merge[Int](streams.size))
      streams.foreach { name ⇒
        Source.actorPublisher(Props(classOf[TopicReader], name, statsD, latencies.next())) ~> merge
      }
      merge.out
    }

    val fastSink = Sink.actorSubscriber(Props(classOf[DelayingActor], "multiStreamSink", statsD, 0l))

    FlowGraph.closed() { implicit builder ⇒
      import FlowGraph.Implicits._
      //val merge = builder.add(Merge[Int](4))

      //val okcSource = throttledSource(statsD, 1 second, 20 milliseconds, 10000, "okcSource")
      //val houSource = throttledSource(statsD, 1 second, 30 milliseconds, 10000, "houSource")
      //val miaSource = throttledSource(statsD, 1 second, 40 milliseconds, 10000, "miaSource")
      //val sasSource = throttledSource(statsD, 1 second, 45 milliseconds, 10000, "sasSource")

      //or
      //Source.actorPublisher(Props(classOf[TopicReader], "okcSource", statsD, 20l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "houSource", statsD, 30l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "miaSource", statsD, 40l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "sasSource", statsD, 45l)) ~> merge
      //merge ~> fastSink

      /*
      okcSource ~> merge
      houSource ~> merge
      miaSource ~> merge
      sasSource ~> merge
                   merge ~> fastSink
      */
      queryStreams ~> fastSink
    }
  }

  /**
   * Create a source which is throttled to a number of message per second.
   */
  def throttledSource(statsD: InetSocketAddress, delay: FiniteDuration, interval: FiniteDuration, numberOfMessages: Int, name: String): Source[Int, Unit] = {
    Source[Int]() { implicit b ⇒
      import FlowGraph.Implicits._
      val sendBuffer = ByteBuffer.allocate(1024)
      val channel = DatagramChannel.open()

      // two source
      val tickSource = Source(delay, interval, Tick())
      val rangeSource = Source(1 to numberOfMessages)

      def send(message: String) = {
        sendBuffer.put(message.getBytes("utf-8"))
        sendBuffer.flip()
        channel.send(sendBuffer, statsD)
        sendBuffer.limit(sendBuffer.capacity())
        sendBuffer.rewind()
      }

      val sendMap = b.add(Flow[Int] map { x ⇒ send(s"$name:1|c"); x })

      // we use zip to throttle the stream
      val zip = b.add(Zip[Tick, Int]())
      val unzip = b.add(Flow[(Tick, Int)].map(_._2))

      // setup the message flow
      tickSource ~> zip.in0
      rangeSource ~> zip.in1
      zip.out ~> unzip ~> sendMap

      sendMap.outlet
    }
  }
}

trait StatsD {
  val Encoding = "utf-8"

  val sendBuffer = ByteBuffer.allocate(1024)
  val channel = DatagramChannel.open()

  def statsD: InetSocketAddress

  def notifyStatsD(message: String) = {
    sendBuffer.put(message.getBytes("utf-8"))
    sendBuffer.flip()
    channel.send(sendBuffer, statsD)
    sendBuffer.limit(sendBuffer.capacity())
    sendBuffer.rewind()
  }
}


/**
 * Same as throttledSource but using ActorPublisher
 * @param name
 * @param statsD
 * @param delay
 */
class TopicReader(name: String, val statsD: InetSocketAddress, delay: Long) extends ActorPublisher[Int] with StatsD {
  val Limit = 10000
  var progress = 0
  val observeGap = 1000

  override def receive: Actor.Receive = {
    case Request(n) => if (isActive && totalDemand > 0) {
      var n0 = n

      if(progress >= Limit)
        self ! Cancel

      while (n0 > 0) {
        if(progress % observeGap == 0)
          println(s"$name: $progress")

        progress += 1
        onNext(progress)
        Thread.sleep(delay)
        notifyStatsD(s"$name:1|c")
        n0 -= 1
      }
    }

    case Cancel =>
      println(name + " is canceled")
      context.system.stop(self)
  }
}


class DelayingActor(name: String, val statsD: InetSocketAddress, delay: Long) extends ActorSubscriber
    with StatsD {

  override protected val requestStrategy = OneByOneRequestStrategy

  def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0)
  }

  // we collect some metrics during processing so we can count the rate
  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      Thread.sleep(delay)
      notifyStatsD(s"$name:1|c")

    case OnComplete ⇒
      println(s"Complete DelayingActor")
      context.system.stop(self)
  }
}

class DegradingActor(val name: String, val statsD: InetSocketAddress, delayPerMsg: Long, initialDelay: Long) extends ActorSubscriber
    with StatsD {

  override protected val requestStrategy = OneByOneRequestStrategy

  // default delay is 0
  var delay = 0l

  def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0, 0)
  }

  def this(name: String, statsD: InetSocketAddress, delayPerMsg: Long) {
    this(name, statsD, delayPerMsg, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      delay += delayPerMsg
      Thread.sleep(initialDelay + (delay / 1000), delay % 1000 toInt)
      notifyStatsD(s"$name:1|c")

    case OnComplete ⇒
      println(s"Complete DegradingActor")
      context.system.stop(self)
  }
}

