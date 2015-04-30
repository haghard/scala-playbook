package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors._
import java.util.concurrent.{ Executors, ThreadFactory }
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable._
import scodec.bits.ByteVector
import scalaz._
import scalaz.stream.Process._
import scalaz.stream._
import scalaz.syntax.monad._
import scodec.Codec
import scodec.codecs.implicits._
import scalaz.concurrent.{ Strategy, Task }
import scalaz.netty._
import collection.mutable.Buffer
import scalaz.stream.merge
import scalaz.stream.io

/*
 * doobie uses shapless-2.0.0, scalaz-netty through scodec-core uses shapless-2.1.0
 *
 */

trait ScalazNettyConfig {
  val enc = java.nio.charset.Charset.forName("UTF-8")
  val greeting = ByteVector("Hello ".getBytes(enc))

  implicit val scheduler = {
    Executors.newScheduledThreadPool(2, new ThreadFactory {
      def newThread(r: Runnable) = {
        val t = Executors.defaultThreadFactory.newThread(r)
        t.setDaemon(true)
        t.setName("scheduled-task-thread")
        t
      }
    })
  }

  val P = Process
  val logger = Logger.getLogger("netty-server")

  def LoggerS: Sink[Task, String] = sink.lift[Task, String] { line ⇒
    Task.delay(logger.info(line))
  }

  val codec: Codec[String] = scodec.codecs.utf8
  val enc0 = scodec.stream.encode.many(codec)
  val dec0 = scodec.stream.decode.many(codec)

  def transcode(ex: Exchange[ByteVector, ByteVector]) = {
    val Exchange(src, sink) = ex
    val src2 = src.map(_.toBitVector).flatMap(b ⇒ dec0.decode(b))
    Exchange(src2, sink)
  }

  def address: InetSocketAddress

  val PoisonPill = "Poison"

  def nats = {
    def go(i: Long): Process[Task, Long] =
      Process.await(Task.delay(i))(i ⇒ Process.emit(i) ++ go(i + 1))
    go(1l)
  }
}

class ScalazNettyRequestResponseSpec extends Specification with ScalazNettyConfig {

  override val address = new InetSocketAddress("localhost", 9091)

  "Request-response server with 2 clients" should {
    "run echo" in {
      val n = 5

      val S = newFixedThreadPool(4, new NamedThreadFactory("netty-worker2"))
      val C = newFixedThreadPool(2, new NamedThreadFactory("netty-client"))

      def serverHandler(cmd: String) = {
        logger.info(s"[server] receive $cmd")
        if (cmd == PoisonPill) throw new Exception("Stop command received")

        greeting ++ ByteVector(cmd.getBytes(enc))
      }

      val EchoGreetingServer = merge.mergeN(2)(Netty.server(address)(S) map { v ⇒
        for {
          _ ← Process.eval(Task.delay(logger.info(s"Connection had accepted from ${v._1}")))
          Exchange(src, sink) = transcode(v._2)
          _ ← src map serverHandler to sink
        } yield ()
      })(Strategy.Executor(S))

      EchoGreetingServer.run.runAsync(_ ⇒ ())

      def client(mes: String, buf: Buffer[String]) = {
        val clientStream: Process[Task, ByteVector] =
          (P.emitAll(Seq.fill(n)(mes) :+ PoisonPill) |> enc0.encoder).map(_.toByteVector)

        for {
          exchange ← Netty.connect(address)(C)
          Exchange(src, sink) = transcode(exchange)

          out = clientStream |> process1.lift { b ⇒ logger.info(s"send $mes"); b } to sink
          in = src observe (LoggerS) to io.fillBuffer(buf)

          /*
           * Request response mode with `zip`.
           * Order significant for  req-resp flow
           * Don't wait last response because it kills server
           */
          _ ← (out zip in.take(n))

          //request non deterministic req/resp flow  with `merge`
          //_ ← (out merge in)(Strategy.Executor(C))
        } yield ()
      }

      val ND = Nondeterminism[Task]
      val bufBob = Buffer.empty[String]
      val bufAlice = Buffer.empty[String]

      val r = ND.both(client("echo Bob", bufBob).runLog[Task, Any], client("echo Alice", bufAlice).runLog[Task, Any]).run
      bufBob.size must be equalTo n
      bufAlice.size must be equalTo n
    }
  }
}