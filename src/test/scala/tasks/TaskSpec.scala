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
          } yield x + y
      }
      //run/attemptRun is like get on Future
      fib(15).attemptRun should be equalTo \/-(987)
    }
  }

  "Run fib with explicitly asking for concurrency" should {
    "run in pool" in {
      def fib(n: Int): Task[Int] = n match {
        case 0 | 1 ⇒ Task now 1
        case n ⇒
          val ND = Nondeterminism[Task]
          for {
            r ← ND.mapBoth(Task.fork(fib(n - 1)), Task.fork(fib(n - 2)))(_ + _)
          } yield { logger.info(r); r }
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
        ).kleisliR

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

  import scalaz._
  import Scalaz._

  val serviceR = (x: String) ⇒
    Task(\/.fromTryCatchThrowable[Int, NumberFormatException](x.toInt))

  def serviceV(x: String): ValidationNel[String, Int] = try {
    val r = x.toInt
    if (r > 5) r.success
    else s"$r should be > 5".failureNel
  } catch {
    case e: Exception ⇒ e.getMessage.failureNel
  }

  "Task error handling with EitherT" should {
    "run failed with first error" in {
      import scalaz.EitherT._

      val flow: Task[NumberFormatException \/ Int] =
        (for {
          a ← serviceR("a") |> eitherT
          b ← serviceR("b") |> eitherT
        } yield (a + b)).run

      val r = flow.run
      logger.info(r)
      //new NumberFormatException("For input string: "a""))
      r.isLeft === true
    }
  }

  "Error accumulation with ValidationNel" should {
    "run succeed" in {
      val flowV = (serviceV("a") |@| serviceV("5")) {
        case (a, b) ⇒ s"serviceA result:$a  serviceV result:$b"
      }

      flowV.isFailure === false
      flowV.shows must_=== "Failure([\"For input string: \"a\"\",\"5 should be > 5\"])"
    }
  }

  "Task error handling with EitherT" should {
    "run succeed" in {
      import scalaz.EitherT._

      val flow: Task[NumberFormatException \/ Int] =
        (for {
          a ← serviceR("1") |> eitherT
          b ← serviceR("2") |> eitherT
        } yield (a + b)).run

      flow.run should be equalTo \/-(3)
    }
  }

  "Task error handling with EitherT" should {
    "run failed with first error" in {
      import scalaz.EitherT._

      val flow: Task[NumberFormatException \/ Int] =
        (for {
          a ← serviceR("a") |> eitherT
          b ← serviceR("b") |> eitherT
        } yield (a + b)).run

      val r = flow.run
      logger.info(r)
      //new NumberFormatException("For input string: "a""))
      r.isLeft === true
    }
  }

  trait Op[A] {
    def sum(f: A, s: A): A
  }

  object Op {

    implicit object StrCont extends Op[String] {
      override def sum(f: String, s: String): String = (f.toInt + s.toInt).toString
    }

    implicit object DoubleCont extends Op[Double] {
      override def sum(f: Double, s: Double): Double = f + s
    }

    implicit object IntCont extends Op[Int] {
      override def sum(f: Int, s: Int): Int = {
        if (f > 5) throw new Exception(s" $f should be < 5 ")
        if (s > 5) throw new Exception(s" $s should be < 5 ")
        f + s
      }
    }

  }

  def sReader[T: Op] = scalaz.Reader { (left: T) ⇒
    (right: T) ⇒ {
      val c = implicitly[Op[T]]
      Task(\/.fromTryCatchThrowable[T, Exception](c.sum(left, right))).map {
        case \/-(r)  ⇒ \/-(r)
        case -\/(ex) ⇒ -\/(ex.getMessage)
      }
    }
  }

  "Task error handling with Reader" should {
    "run succeed" in {
      sReader[Int].run(1)(2).run should be equalTo \/-(3)
    }
  }

  "Task error handling with Reader" should {
    "run failed with first error" in {
      val r = sReader[String].run("asw")("2").run
      logger.info(r)
      //-\/(java.lang.NumberFormatException: For input string: "asw")
      r.isLeft === true
    }
  }

  "Task error handling with 2 independent Readers" should {
    "composition" in {

      val x = sReader[Int].run(1)
      val y = sReader[Int].run(2)

      val f = for {
        a ← x(3)
        b ← y(6)
      } yield a |+| b

      val result = f.run
      result should be equalTo -\/(" 6 should be < 5 ")
    }
  }

  "Task error handling with 2 dependent Readers" should {
    "composition" in {

      val flow = sReader[Int] >==> {
        v ⇒
          {
            for {
              x ← v(2)
              y ← x match {
                case \/-(r)  ⇒ sReader[Int].apply(r)(6)
                case -\/(ex) ⇒ Task.now[String \/ Int](-\/(ex))
              }
            } yield y
          }
      }

      val r = flow.run(1).run
      r should be equalTo -\/(" 6 should be < 5 ")
    }
  }
}