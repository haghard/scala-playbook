package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors._

import mongo.MongoProgram.NamedThreadFactory
import org.specs2.mutable.Specification
import scodec.bits.ByteVector

import scala.collection.mutable.Buffer
import scalaz.concurrent.{ Strategy, Task }
import scalaz.netty.Netty
import scalaz.stream.Process._
import scalaz.stream._
import process1._

class ScalazNettyRequestNResponse1Spec extends Specification with ScalazNettyConfig {

  override val address = new InetSocketAddress("localhost", 9095)

  "Request(N) aggregated response server/client" should {
    "run with tee" in {
      val batchSize = 5
      val iterationN = 10
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
          _ ← src chunk batchSize map reduceServer to sink
        } yield ()
      })(Strategy.Executor(S))

      EchoGreetingServer.run.runAsync(_ ⇒ ())

      /*val bWye = wye.dynamic(
        (x: (Unit, Long)) ⇒ if (x._2 % batchSize == 0) { wye.Request.R } else { wye.Request.L },
        (y: Unit) ⇒ wye.Request.L
      )*/

      /*
      * It is tee reading `n` elements from the left, then one element from right
      */
      def zipN[I, I2](n: Int) = {
        def go(n: Int, limit: Int): Tee[I, I2, Any] = {
          if (n > 0) awaitL[I] ++ go(n - 1, limit)
          else awaitR[I2] ++ go(limit, limit)
        }
        go(n, n)
      }

      def client(mes: String, buf: Buffer[String]) = {
        val n = 10 * batchSize
        val poison = (P.emitAll(Seq.fill(batchSize)(PoisonPill)) |> enc0.encoder) map (_.toByteVector)

        for {
          exchange ← Netty.connect(address)(C)
          Exchange(src, sink) = transcode(exchange)

          out = ((clientStream(mes) take n) ++ poison) |> lift { b ⇒ logger.info(s"send $mes"); b } to sink
          in = src observe (LoggerS) to io.fillBuffer(buf)

          _ ← (out tee in)(zipN(batchSize))
            //_ ← ((out zip nats).wye(in)(bWye)(Strategy.Executor(C)))
            .take((batchSize + 1) * iterationN + batchSize) // for correct client exit
        } yield ()
      }

      val buf = Buffer.empty[String]
      client("Bob", buf).runLog[Task, Unit].run
      buf.size must be equalTo iterationN
    }
  }
}
