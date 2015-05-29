package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors._

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
      val iterationN = 11
      val S = newFixedThreadPool(2, namedThreadFactory("netty-worker2"))
      val C = newFixedThreadPool(1, namedThreadFactory("netty-client"))

      def reduceServer(batch: Vector[ByteVector]) = {
        logger.info(s"[server] receive batch")

        logger.info("Server receive: " +
          batch.map(_.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r)).mkString(", "))

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

      def next[I, I2](l: I, r: I2, n: Int, limit: Int): Tee[I, I2, Any] =
        if (n > 0) nextL(l, n - 1, limit) else nextR(r, limit, limit)

      def nextR[I, I2](r: I2, n: Int, limit: Int): Tee[I, I2, Any] = tee.receiveROr[I, I2, Any](emit(r))(next(_, r, n, limit))
      def nextL[I, I2](l: I, n: Int, limit: Int): Tee[I, I2, Any] = tee.receiveLOr[I, I2, Any](emit(l))(next(_, l, n, limit))

      def initT[I, I2](n: Int, limit: Int): Tee[I, I2, Any] = tee.receiveL[I, I2, Any] { l: I ⇒ emit[I](l); nextL[I, I2](l, n - 1, limit) }

      def init[I, I2](n: Int, limit: Int): Tee[I, I2, Any] = awaitL[I].flatMap { nextL(_, n - 1, n) }

      def zipN2[I, I2](n: Int): Tee[I, I2, Any] =
        init[I, I2](n, n)

      /*
      * It is tee reading `n` elements from the left, then one element from right
      */
      def zipN[I, I2](n: Int): Tee[I, I2, Any] = {
        def go(n: Int, limit: Int): Tee[I, I2, Any] = {
          if (n > 0) awaitL[I] ++ go(n - 1, limit)
          else awaitR[I2] ++ go(limit, limit)
        }
        go(n, n)
      }

      def client(target: String, buf: Buffer[String]) = {
        val n = iterationN * batchSize
        val poison = (P.emitAll(Seq.fill(batchSize)(PoisonPill)) |> encUtf.encoder) map (_.toByteVector)

        for {
          exchange ← Netty.connect(address)(C)
          Exchange(src, sink) = transcodeUtf(exchange)

          out = (requestSrc(target) take n) ++ poison |> lift { b ⇒ logger.info(s"send for $target"); b } to sink
          in = src observe (LoggerS) to io.fillBuffer(buf)

          _ ← (out tee in)(zipN2(batchSize)) //map(_ => 1)
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
