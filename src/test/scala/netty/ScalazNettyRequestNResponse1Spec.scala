package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors._

import mongo.MongoProgram.NamedThreadFactory
import org.specs2.mutable.Specification
import scodec.bits.ByteVector

import scala.collection.mutable.Buffer
import scalaz.Monoid
import scalaz.concurrent.{Strategy, Task}
import scalaz.netty.Netty
import scalaz.stream._


/*
It works too, but kind of too static....
def zipWithN[I, I2]: Tee[I, I2, I2] = {
  for {
    _ ← awaitL[I]
    _ ← awaitL[I]
    _ ← awaitL[I]
    _ ← awaitL[I]
    _ ← awaitL[I]
    r0 ← awaitR[I2]
    r ← emit(r0)
  } yield r
} repeat
*/

class ScalazNettyRequestNResponse1Spec extends Specification with ScalazNettyConfig {

  override val address = new InetSocketAddress("localhost", 9095)

  "Request(N) aggregated response server/client" should {
    "run" in {
      val batchSize = 10
      val iterationN = 5
      val S = newFixedThreadPool(2, new NamedThreadFactory("netty-worker2"))
      val C = newFixedThreadPool(1, new NamedThreadFactory("netty-client"))

      def reduceServer(batch: Vector[ByteVector]) = {
        logger.info(s"[server] receive batch")
        if (batch(0).decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r) == PoisonPill) {
          logger.info(s"kill message received")
          throw new Exception("Stop command received")
        }
        batch.reduce(_ ++ _)
      }

      val EchoGreetingServer = merge.mergeN(2)(Netty.server(address)(S) map { v ⇒
        for {
          _ ← Process.eval(Task.delay(logger.info(s"Connection had accepted from ${v._1}")))
          Exchange(src, sink) = v._2
          _ ← (src |> process1.chunk(batchSize)).map(reduceServer) to (sink)
        } yield ()
      })(Strategy.Executor(S))

      EchoGreetingServer.run.runAsync(_ ⇒ ())

      val bWye = wye.dynamic(
        (x: (Unit, Long)) ⇒ if (x._2 % batchSize == 0) { wye.Request.R } else { wye.Request.L },
        (y: Unit) ⇒ wye.Request.L
      )

      def client(mes: String, buf: Buffer[String]) = {
        val clientStream: Process[Task, ByteVector] =
          (P.emitAll(Seq.fill(batchSize * iterationN)(mes) ++ Seq.fill(batchSize)(PoisonPill)) |> enc0.encoder).map(_.toByteVector)

        for {
          exchange ← Netty.connect(address)(C)
          Exchange(src, sink) = transcode(exchange)

          out = clientStream |> process1.lift { b ⇒ logger.info(s"send $mes"); b } to sink
          in = src observe (LoggerS) to io.fillBuffer(buf)

          _ ← ((out zip nats).wye(in)(bWye)(Strategy.Executor(C)))
            .take((batchSize + 1) * iterationN + batchSize)// for correct exit
        } yield ()
      }

      val buf = Buffer.empty[String]
      client("Bob", buf).runLog[Task, Unit].run
      buf.size must be equalTo iterationN
    }
  }
}
