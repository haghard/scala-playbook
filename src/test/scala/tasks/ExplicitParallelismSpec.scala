package tasks

import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent.forkjoin.ForkJoinPool
import scalaz.{ Nondeterminism, \/- }
import scalaz.concurrent.Task

class ExplicitParallelismSpec extends Specification {

  private val logger = Logger.getLogger("tasks")

  implicit val executor = new ForkJoinPool(3)
  //Executors.newFixedThreadPool(5, new NamedThreadFactory("ext-worker"))

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
}