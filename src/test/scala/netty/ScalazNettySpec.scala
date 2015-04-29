package netty

import java.net.InetSocketAddress
import java.util.concurrent.{ Executors, ThreadFactory, TimeUnit }
import org.apache.log4j.Logger
import org.specs2.mutable._
import scodec.bits.ByteVector
import scalaz._
import scalaz.stream.Process._
import scalaz.stream._
import scalaz.syntax.monad._

import scala.concurrent.duration._
import scalaz.concurrent.{ Strategy, Task }
import scalaz.netty._

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
}

class ScalazNettySpec extends Specification with ScalazNettyConfig {

  val message = "Alice"
  val address = new InetSocketAddress("localhost", 9091)

  "Server for multi clients" should {
    "single req response echo server" in {
      import scalaz.stream.merge
      val n = 6
      def serverLogic(bytes: ByteVector) = {
        if (bytes.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r) == "stop")
          throw new Exception("Stop command received") //not a best way, but ...

        val mes = bytes.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r)
        logger.info(s"[server] receive $mes length:${mes.length}")
        greeting ++ bytes
      }

      val EchoGreetingServer = merge.mergeN(Netty.server(address).map { v ⇒
        val addr = v._1
        val exchange = v._2
        for {
          _ ← Process.eval(Task.delay(logger.info(s"Accepted connection from $addr")))
          batch ← exchange.read.map(serverLogic) to exchange.write
        } yield ()
      })

      EchoGreetingServer.run.runAsync(_ ⇒ ())

      def client(message: String) = Netty connect (address) flatMap { exchange ⇒
        val source: Process[Task, ByteVector] = P.emitAll(Seq.fill(n)(ByteVector(message.getBytes(enc))))
        (for {
          batch ← source to exchange.write
          data ← exchange.read.take(1).map(_.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r)) // take(1) is necessary for wait response
          _ ← Process.eval(Task.delay(logger.info(s"Server response: $data")))
        } yield (data)) ++ (P.emit(ByteVector("stop".getBytes(enc))) to exchange.write)
      }

      val ND = Nondeterminism[Task]
      val r = ND.both(client("Bob").runLog[Task, Any], client("Alice").runLog[Task, Any]).run
      r._1.size + r._2.size must be equalTo (n * 2) + 1 * 2 //2 is the errors
    }
  }
}