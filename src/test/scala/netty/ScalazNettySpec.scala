/*

// doobie uses shapless-2.0.0, scalaz-netty throuth scodec-core uses shapless-2.1.0

package netty

import java.net.InetSocketAddress
import java.util.concurrent.{ ExecutorService, Executors, ThreadFactory, TimeUnit }

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable._
import scodec.bits.ByteVector
import scalaz.{ -\/, \/-, syntax, Nondeterminism }
import syntax.monad._

import scala.concurrent.duration._
import scalaz.concurrent.{ Strategy, Task }
import scalaz.netty._
import scalaz.stream.{ Exchange, Process, time }

class ScalazNettySpec extends Specification {

  import collection.mutable.Buffer

  private val logger = Logger.getLogger("netty-server")

  org.specs2.main.ArgumentsShortcuts.sequential

  val P = Process

  val enc = java.nio.charset.Charset.forName("UTF-8")
  val greeting = ByteVector("Hello ".getBytes(enc))

  val address = new InetSocketAddress("localhost", 9091)
  val message = "Alice"

  val scheduler = {
    Executors.newScheduledThreadPool(2, new ThreadFactory {
      def newThread(r: Runnable) = {
        val t = Executors.defaultThreadFactory.newThread(r)
        t.setDaemon(true)
        t.setName("scheduled-task-thread")
        t
      }
    })
  }

  val delay = time.sleep(new FiniteDuration(1, TimeUnit.SECONDS))(Strategy.DefaultStrategy, scheduler)

  "Server for multi clients" should {
    "single req response echo server" in {

      val EchoGreetingServer = scalaz.stream.merge.mergeN(Netty.server(address).map {
        case (addr, incoming) ⇒ {
          for {
            exchange ← incoming
            _ ← Process.eval(Task.delay(logger.info(s"Accepted connection from $addr")))
            _ ← exchange.read.map(m ⇒ greeting ++ m) to exchange.write
          } yield ()
        }
      })

      EchoGreetingServer.run.runAsync(_ ⇒ ())

      def client(message: String) = Netty connect address flatMap { exchange ⇒
        for {
          _ ← P.emit(ByteVector(message.getBytes(enc))) to exchange.write
          data ← (exchange.read take 1).map(_.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r))
          _ ← Process.eval(Task.delay(logger.info(s"Server response: $data")))
        } yield (data)
      }

      val ND = Nondeterminism[Task]

      val r = ND.nmap2(client("Bob").runLog[Task, String], client("Alice").runLog[Task, String])(_(0) + _(0)).run
      r must contain("Bob")
      r must contain("Alice")
    }
  }
}*/
