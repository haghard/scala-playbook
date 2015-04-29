package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import mongo.MongoProgram.NamedThreadFactory
import org.specs2.mutable.Specification
import scodec.bits.ByteVector

import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.stream.Process
import scalaz.concurrent.Task
import scalaz.netty.Netty
import scalaz.stream._

class ServerWithExternalEffect extends Specification with ScalazNettyConfig {

  val address = new InetSocketAddress("localhost", 9093)

  "Batching client and external effects server" should {
    "run" in {
      val n = 15
      val bs = n / 3

      val io = Executors.newFixedThreadPool(2, new NamedThreadFactory("remote-io-worker"))

      def server = P.repeatEval(Task now { bts: ByteVector ⇒
        //in separate pool we can do external effects
        Task {
          logger.info("external call")
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))

          if (bts.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r) == "stop")
            throw new Exception("Stop command received") //not a best way, but ...

          P.emit(greeting ++ bts)
        }(io)
      })

      val EchoGreetingServer = scalaz.stream.merge.mergeN(1)(Netty server address map { v ⇒
        val addr = v._1
        val exchange = v._2
        for {
          _ ← Process.eval(Task.delay(logger.info(s"Accepted connection from $addr")))
          out ← exchange.read through server //every message from single client will be handled sequentially
          _ ← out to exchange.write
        } yield ()
      })

      EchoGreetingServer.runLog.runAsync(_ ⇒ ())

      def batchClient(message: String) = Netty connect address flatMap { exchange ⇒
        val source: Process[Task, ByteVector] = P.emitAll(Seq.fill(n)(ByteVector(message.getBytes(enc))))
        (for {
          batch ← source.chunk(bs)
          _ ← P.emitAll(batch) |> process1.lift { b ⇒ logger.info(s"client write"); b } to exchange.write
          data ← exchange.read.take(1).map(_.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r))
          _ ← Process.eval(Task.delay(logger.info(s"client receive: $data")))
        } yield (data)) ++ (P.emit(ByteVector("stop".getBytes(enc))) to exchange.write)
      }

      val r = batchClient("Bob").runLog.run

      r.size must be equalTo n + 1
    }
  }
}