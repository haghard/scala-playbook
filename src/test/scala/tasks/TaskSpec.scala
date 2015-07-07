package tasks

import java.util.concurrent.Executors
import java.util.concurrent.Executors._
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.ForkJoinPool
import scalaz.{ \/, Nondeterminism, \/- }
import scalaz.concurrent.Task

class TaskSpec extends Specification {

  private val logger = Logger.getLogger("tasks")

  implicit val executor = new ForkJoinPool(3)

  "Run fib" should {
    "in same thread" in {
      def fib(n: Int): Task[Int] = n match {
        case 0 | 1 ⇒ Task now 1
        case n ⇒
          for {
            x ← fib(n - 1)
            y ← fib(n - 2)
          } yield (x + y)
      }
      //run/attemptRun is like get on Future
      fib(15).attemptRun should be equalTo \/-(987)
    }
  }

  "Run fib with explicitly asking for concurrency" should {
    "run in pool" in {
      def fib(n: Int): Task[Int] = n match {
        case 0 | 1 ⇒ Task now 1
        case n ⇒ {
          val ND = Nondeterminism[Task]
          for {
            r ← ND.mapBoth(Task.fork(fib(n - 1)), Task.fork(fib(n - 2)))(_ + _)
          } yield { logger.info(r); r }
        }
      }
      fib(15).attemptRun should be equalTo \/-(987)
    }
  }

  "Run factorial" should {
    "run single thread" in {
      object Big { def unapply(n: BigInt) = Some(n.toInt) }

      def factorial(n: BigInt): Task[BigInt] = n match {
        case Big(1) ⇒ Task now 1
        case n      ⇒ for { r ← factorial(n - 1) } yield { n * r }
      }

      factorial(30).attemptRun should be equalTo \/-(BigInt("265252859812191058636308480000000"))
    }
  }

  "Apply a function to n results" should {
    "nondeterminstically ordering their effects" in {
      import scalaz.concurrent.Task
      import scalaz.Nondeterminism

      val a = Task.fork { Thread.sleep(1000); Task.now("a") }
      val b = Task.fork { Thread.sleep(800); Task.now("b") }
      val c = Task.fork { Thread.sleep(600); Task.now("c") }
      val d = Task.fork { Thread.sleep(200); Task.now("d") }

      val r = Nondeterminism[Task].nmap4(a, b, c, d)(List(_, _, _, _))
      val list = r.run

      list.size should be equalTo 4
    }
  }

  "Aggregate results" should {
    "with monoid" in {
      import scalaz._
      import Scalaz._
      val ND = scalaz.Nondeterminism[Task]
      implicit val worker = newFixedThreadPool(3, new NamedThreadFactory("worker"))

      def fib(n: Int): Task[Int] = n match {
        case 0 | 1 ⇒ Task now 1
        case n ⇒
          for {
            r ← ND.mapBoth(Task.fork(fib(n - 1)), Task.fork(fib(n - 2)))(_ + _)
          } yield { logger.info(r); r }
      }

      //Sum support commutative laws
      val m = scalaz.Monoid[Int]
      val task = ND.aggregateCommutative(Seq(fib(11), fib(12), fib(13), fib(14)))(m)
      val task1 = ND.aggregate(Seq(fib(11), fib(12), fib(13), fib(14)))(m)

      val r0 = task.attemptRun
      r0 should be equalTo task1.attemptRun
      r0 should be equalTo \/-(1364)
    }
  }

  "Kleisli for pleasant choose the pool" should {
    "run" in {
      import common.KleisliSupport._
      //This is how we can get a computation to run on precisely the pool,
      //without having to constantly pass around the explicit reference to the target pool

      val executorIO = Executors.newFixedThreadPool(2, new NamedThreadFactory("io-worker"))
      val executorCPU = Executors.newFixedThreadPool(2, new NamedThreadFactory("cpu-worker"))

      val flow = for {
        executor ← reader //delegate
        pair ← Nondeterminism[Task].both(
          Task {
            logger.info("X start")
            Thread.sleep(2000)
            logger.info("X stop")
            Thread.currentThread().getName + "-x"
          }(executor),
          Task {
            logger.info("Y start")
            Thread.sleep(1000)
            logger.info("Y stop")
            Thread.currentThread().getName + "-y"
          }(executor)
        ).kleisliR //kleisli

        (x, y) = pair
      } yield { s"$x - $y" }

      val result0 = flow run executorIO
      val result1 = flow run executorCPU

      val r0 = new SyncVar[Throwable \/ String]
      val r1 = new SyncVar[Throwable \/ String]

      result0.runAsync(r0.put)
      result1.runAsync(r1.put)

      r0.get should be equalTo \/-("io-worker-thread-1-x - io-worker-thread-2-y")
      r1.get should be equalTo \/-("cpu-worker-thread-1-x - cpu-worker-thread-2-y")
    }
  }

  /*
  "sdfsdf" should {
    "run" in {
      import scalaz.EitherT._

      val service = (x: String) ⇒
        Task(\/.fromTryCatchThrowable[Int, NumberFormatException](x.toInt))

      val result: Task[NumberFormatException \/ Int] = (for {
        a ← service("1") |> eitherT
        b ← service("2") |> eitherT
      } yield a + b).run

      trait Convertor[A] {
        def toInt(v: A): Int
      }

      def service[T <: { def toInt(l: String): Int }] = scalaz.Reader /*[T, Task]*/ { (c: T) ⇒
        (x: String) ⇒
          Task(\/.fromTryCatchThrowable[Int, NumberFormatException](c toInt x))
      }

      service[Convertor[Int]].run

      1 === 1
    }
  }*/
}