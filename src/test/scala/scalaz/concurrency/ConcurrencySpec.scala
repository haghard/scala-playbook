package scalaz.concurrency

import java.util.concurrent.Executors._
import java.util.concurrent.atomic.AtomicInteger

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scalaz.concurrent.MVar._
import scalaz.concurrent.{ MVar, Strategy }
import scalaz.effect.IO
import scalaz.effect.IO._

class ConcurrencySpec extends Specification {

  val logger = Logger.getLogger("concurrent")
  val pool = newFixedThreadPool(2, new NamedThreadFactory("helper"))
  implicit val P = Strategy.Executor(pool)

  case class Foo(name: String)
  def forkIO(f: ⇒ IO[Unit])(implicit s: Strategy): IO[Unit] = IO { s(f.unsafePerformIO); () }

  "MVar" should {
    "have deterministic sequential take/put behaviour" in {
      def run = for {
        in ← newEmptyMVar[Foo]
        _ ← forkIO {
          for {
            _ ← in.put(Foo("aliceV1"))
            _ ← in.put(Foo("aliceV2"))
            u = logger.info(s"perform writes")
          } yield ()
        }

        a ← in.take
        b ← in.take
        u = logger.info(s"perform take")
      } yield (a, b)
      run.unsafePerformIO must_== (Foo("aliceV1"), Foo("aliceV2"))
    }
  }

  "MVar swap references" in {
    def io(bob: Foo, alice: Foo) = for {
      bRef ← newEmptyMVar[Foo]
      aRef ← newEmptyMVar[Foo]

      _ ← forkIO {
        for {
          _ ← aRef.put(bob)
          _ ← bRef.put(alice)
          u = logger.info(s"do write")
        } yield ()
      }

      newBob ← bRef.take
      newAlice ← aRef.take
      _ ← putStrLn(s"after bob's become: $newBob")
      _ ← putStrLn(s"after alice's become: $newAlice")
    } yield (newBob, newAlice)

    val bob = Foo("bob")
    val alice = Foo("alice")

    val r = io(bob, alice).unsafePerformIO() // must_== (alice, bob)

    //check for reference equality
    (bob eq r._2) === true
    (alice eq r._1) === true

    //check for object  equality
    (bob == r._2) === true
    (alice == r._1) === true
  }

  def pingpong() {
    def pong(c: MVar[String], p: MVar[String]) =
      for {
        _ ← c.take flatMap (s ⇒ IO(logger.info(s"Read c $s. Wait for new c")))
        _ ← p.put("pong")
        _ ← c.take flatMap (s ⇒ IO(logger.info(s"Read c $s, Wait for new c")))
        _ ← p.put("pong")
      } yield ()

    def ping =
      for {
        c ← newMVar("ping")
        p ← newEmptyMVar[String]
        _ ← forkIO(pong(c, p))
        _ ← p.take flatMap (s ⇒ IO(logger.info(s"Read p $s. Wait for new p")))
        _ ← c.put("ping")
        _ ← p.take flatMap (s ⇒ IO(logger.info(s"Read p $s. Wait for new p")))
      } yield ()
    ping.unsafePerformIO
  }

  "MVar pingpong" in {
    pingpong()
    1 === 1
  }

  "Observe Vars changes" should {
    "have observed only after pair update" in {
      import com.twitter.util.Var
      import com.twitter.util.Updatable
      val N = 100
      val a, b = Var(0)

      class Counter(n: Int, limit: Int, u: Updatable[Int], sleep: Long) extends Thread {
        override def run() {
          var i = 1
          while (i < limit) {
            u.update(i)
            logger.info(s"$n update local var $i")
            i += 1
            Thread.sleep(sleep)
          }
        }
      }

      val c = a.flatMap(_ ⇒ b)
      val acc = new AtomicInteger(-1)
      c.observe { i ⇒
        logger.info(s"update notification $i")
        assert(i === acc.get() + 1)
        acc.set(i)
      }

      val ac = new Counter(0, N, a, 50)
      val bc = new Counter(1, N, b, 70)

      ac.start()
      bc.start()
      ac.join()
      bc.join()

      acc.get === N - 1
    }
  }
}