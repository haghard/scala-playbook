package tasks

import java.util.concurrent.{ ExecutorService, Executors }

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.ForkJoinPool
import scalaz.{ \/, Nondeterminism, \/- }
import scalaz.concurrent.Task

class ExplicitParallelismSpec extends Specification {

  private val logger = Logger.getLogger("tasks")

  implicit val executor = //new ForkJoinPool(3)
    Executors.newFixedThreadPool(5, new NamedThreadFactory("ext-worker"))

  "Run fib without concurrency" should {
    "run in same thread" in {
      def fib(n: Int): Task[Int] = n match {
        case 0 | 1 ⇒ Task now 1
        case n ⇒
          for {
            x ← fib(n - 1)
            y ← fib(n - 2)
          } yield {
            logger.info(s"${Thread.currentThread.getName}")
            x + y
          }
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

  "Run fact" should {
    "run single thread" in {
      object Big { def unapply(n: BigInt) = Some(n.toInt) }

      def fact(n: BigInt): Task[BigInt] = n match {
        case Big(1) ⇒ Task now 1
        case n      ⇒ for { r ← fact(n - 1) } yield { n * r }
      }

      fact(30).attemptRun should be equalTo \/-(BigInt("265252859812191058636308480000000"))
    }
  }

  "Apply a function to n results" should {
    "nondeterminstically ordering their effects" in {
      import scalaz.concurrent.Task
      import scalaz.Nondeterminism

      val a = Task.fork { Thread.sleep(1000); Task.now("a") }
      val b = Task.fork { Thread.sleep(1000); Task.now("b") }
      val c = Task.fork { Thread.sleep(1000); Task.now("c") }
      val d = Task.fork { Thread.sleep(1000); Task.now("d") }

      val r = Nondeterminism[Task].nmap4(a, b, c, d)(List(_, _, _, _))
      val list = r.run

      list.size should be equalTo 4
    }
  }

  "Kleisli for pleasant choose the pool" should {
    "run" in {
      //This is how we can then get a computation to run on precisely the pool, 
      //without having to constantly pass around the explicit reference to the desired pool
      import scalaz.Kleisli
      type Delegated[A] = Kleisli[Task, ExecutorService, A]
      implicit def delegateTaskToPool[A](a: Task[A]): Delegated[A] = Kleisli { x ⇒ a }
      def delegate: Delegated[ExecutorService] = Kleisli { e ⇒ Task.now(e) }

      val executorIO = Executors.newFixedThreadPool(5, new NamedThreadFactory("io-worker"))
      val executorCPU = Executors.newFixedThreadPool(2, new NamedThreadFactory("cpu-worker"))

      val flow = for {
        p ← delegate
        pair ← Nondeterminism[Task].both(
          Task {
            println(Thread.currentThread().getName + "X start")
            Thread.sleep(2000)
            println(Thread.currentThread().getName + "X stop")
            Thread.currentThread().getName + "-x"
          }(p),
          Task {
            println(Thread.currentThread().getName + "Y start")
            Thread.sleep(1000)
            println(Thread.currentThread().getName + "Y stop")
            Thread.currentThread().getName + "-y"
          }(p)
        )
        (x, y) = pair
      } yield { s"$x - $y" }

      val result0 = flow.run(executorIO)
      val result1 = flow.run(executorCPU)

      val r0 = new SyncVar[Throwable \/ String]
      val r1 = new SyncVar[Throwable \/ String]

      result0.runAsync(r0.put(_))
      result1.runAsync(r1.put(_))

      r0.get should be equalTo \/-("io-worker-thread-1-x - io-worker-thread-2-y")
      r1.get should be equalTo \/-("cpu-worker-thread-1-x - cpu-worker-thread-2-y")
    }
  }
}