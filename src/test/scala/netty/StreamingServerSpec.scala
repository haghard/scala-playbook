package netty

import java.net.InetSocketAddress
import java.util.Date
import java.util.concurrent.Executors._
import java.util.concurrent.TimeUnit

import mongo.MongoProgram.NamedThreadFactory
import org.specs2.mutable.Specification
import scodec.bits.ByteVector

import scala.concurrent.duration.FiniteDuration
import scalaz.Nondeterminism
import scalaz.concurrent.{ Strategy, Task }
import scalaz.netty.Netty
import scalaz.stream._

class StreamingServerSpec extends Specification with ScalazNettyConfig {
  val n = 15
  override val address = new InetSocketAddress("localhost", 9092)

  val E = newFixedThreadPool(5, new NamedThreadFactory("netty-worker2"))
  val S = Strategy.Executor(E)

  "Streaming with period the same content" should {
    "run" in {

      val topic = async.topic[ByteVector]()(S)
      val period = new FiniteDuration(1, TimeUnit.SECONDS)

      (time.awakeEvery(period).zip(P.range(0, n)))
        .map(x ⇒ topic.publishOne(ByteVector(new Date().toString.getBytes)).run)
        .runLog.runAsync(_ ⇒ ())

      val cleanup = Task.delay {
        logger.info("StreamingTimeServer complete")
        topic.close.run
      }

      val errorHandler: Cause ⇒ Process[Task, Exchange[ByteVector, ByteVector]] = {
        case cause @ Cause.End ⇒
          Process.Halt(Cause.End)
        case cause @ Cause.Kill ⇒
          Process.Halt(Cause.End)
        case cause @ Cause.Error(ex) ⇒
          ex match {
            case e: java.nio.channels.ClosedChannelException ⇒
              logger.debug("Client disconnected")
              //just end process without errors
              Process.Halt(Cause.End)
            case other ⇒
              logger.debug(s"Server Exception $other")
              Process.Halt(cause)
          }
      }

      val clientsCommands: Sink[Task, ByteVector] = sink.lift[Task, ByteVector] { bts: ByteVector ⇒
        Task.delay {
          val cmd = bts.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r)
          if (cmd == "stop") throw new Exception("stop-server")
          else println(cmd)
        }
      }

      //Server for 2 parallel at most clients
      scalaz.stream.merge.mergeN(2)(Netty.server(address)(E).map { v ⇒
        val addr = v._1
        val exchange = v._2
        (for {
          _ ← Process.eval(Task.delay(logger.info(s"Accepted connection from $addr")))

          in = exchange.read to clientsCommands
          out = topic.subscribe to exchange.write
          _ ← in merge out
        } yield ()) onHalt errorHandler
      })(S).onComplete(P.eval(cleanup)).runLog.runAsync(_ ⇒ ())

      def client(id: Long, n0: Int) = Netty connect address flatMap { exchange ⇒
        (for {
          data ← exchange.read.take(n0).map(_.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r))
          _ ← Process.eval(Task.delay(logger.info(s"client $id receive: $data")))
        } yield (data))
      }

      val ND = Nondeterminism[Task]

      val m = n - 5
      val (l, r) = ND.both(client(1, n).runLog[Task, Any], client(2, m).runLog[Task, Any]).run

      Netty.connect(address).flatMap { ex ⇒ P.emit(ByteVector("stop".getBytes(enc))) to ex.write }.runLog.run

      l.size must be equalTo n
      r.size must be equalTo m
    }
  }
}