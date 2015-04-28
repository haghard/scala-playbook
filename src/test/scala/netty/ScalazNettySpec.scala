package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors._
import java.util.concurrent.{ ThreadLocalRandom, Executors, ThreadFactory, TimeUnit }

import mongo.MongoProgram.NamedThreadFactory
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

/* doobie uses shapless-2.0.0, scalaz-netty through scodec-core uses shapless-2.1.0 */
class ScalazNettySpec extends Specification {

  private val logger = Logger.getLogger("netty-server")

  org.specs2.main.ArgumentsShortcuts.sequential

  val P = Process
  val delay = time.sleep(new FiniteDuration(1, TimeUnit.SECONDS))(Strategy.DefaultStrategy, scheduler)

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

  "Server for multi clients" should {
    "single req response echo server" in {
      import scalaz.stream.merge

      def serverLogic(bytes: ByteVector) = {
        val mes = bytes.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r)
        logger.info(s"[server] receive $mes length:${mes.length}")
        greeting ++ bytes
      }

      val EchoGreetingServer = merge.mergeN(Netty.server(address).map {
        case (addr, connection) ⇒ {
          for {
            exchange ← connection
            _ ← Process.eval(Task.delay(logger.info(s"Accepted connection from $addr")))
            batch ← exchange.read.map(serverLogic) to exchange.write
          } yield ()
        }
      })

      EchoGreetingServer.run.runAsync(_ ⇒ ())

      def client(message: String) = Netty connect (address) flatMap { exchange ⇒
        val source: Process[Task, ByteVector] = P.emitAll(Seq.fill(6)(ByteVector(message.getBytes(enc))))
        for {
          batch ← source to exchange.write
          data ← exchange.read.take(1).map(_.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r)) // take(1) is necessary for wait response
          _ ← Process.eval(Task.delay(logger.info(s"Server response: $data")))
        } yield (data)
      }

      val ND = Nondeterminism[Task]
      val r = ND.nmap2(client("Bob").runLog[Task, String], client("Alice").runLog[Task, String])(_(0) + _(0)).run
      r must contain("Bob")
      r must contain("Alice")
    }
  }

  "Batch client sequentially Server" should {
    "client say [Bob], server respond with [Hello Bob] " in {
      val n = 15
      val bs = n / 3

      val io = newFixedThreadPool(2, new NamedThreadFactory("remote-io-worker"))

      def server = P.repeatEval(Task now { bts: ByteVector ⇒
        //in separate pool
        Task {
          logger.info("external blocking call")
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
          P.emit(greeting ++ bts)
        }(io)
      })

      val EchoGreetingServer = scalaz.stream.merge.mergeN(1)(Netty.server(address).map {
        case (addr, connection) ⇒ {
          for {
            Exchange(src, sink) ← connection
            out ← src through server //every message from single client will be handled sequentially
            _ ← out to sink
          } yield ()
        }
      })

      EchoGreetingServer.runLog.runAsync(_ ⇒ ())

      def batchClient(message: String) = Netty connect address flatMap { exchange ⇒
        val source: Process[Task, ByteVector] = P.emitAll(Seq.fill(n)(ByteVector(message.getBytes(enc))))
        for {
          batch ← source.chunk(bs)
          _ ← P.emitAll(batch) |> process1.lift { b ⇒ logger.info(s"client write"); b } to exchange.write
          data ← exchange.read.take(1).map(_.decodeUtf8.fold(ex ⇒ ex.getMessage, r ⇒ r))
          _ ← Process.eval(Task.delay(logger.info(s"client receive: $data")))
        } yield (data)
      }

      val r = (batchClient("Bob").runLog[Task, String].run)
      r.size must be equalTo n
    }
  }
}
