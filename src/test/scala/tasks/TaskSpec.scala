package tasks

import java.util.concurrent.Executors
import java.util.concurrent.Executors._
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.scalatest.FlatSpec
import scala.concurrent.SyncVar
import scala.concurrent.forkjoin.ForkJoinPool
import scalaz.{ \/, Nondeterminism, \/- }
import scalaz.concurrent.Task

class TaskSpec extends FlatSpec with org.scalatest.Matchers {

  private val logger = Logger.getLogger("tasks")

  implicit val executor = new ForkJoinPool(3)

  def fib(n: Int): Int = if(n <= 1) n else fib(n-1) + fib(n-2)

  def fib2(n: Int): BigInt = {
    @scala.annotation.tailrec
    def loop(n: Int, a: BigInt, b: BigInt): BigInt =
      if(n == 0) a else loop(n-1, b, a + b)

    loop(n, 0, 1)
  }

  "Mutual tail recursion" should "run" in {

    def odd(n: Int): Task[Boolean] =
      for {
        r <- Task.delay(n == 0)
        x <- if(r) Task.now(false) else even(n-1)
      } yield x

    def even(n: Int): Task[Boolean] =
      for {
        r <- Task.delay(n == 0)
        x <- if(r) Task.now(true) else odd(n-1)
      } yield x

    even(100000).attemptRun should === (\/-(true))
  }

  "Run fib" should "in same thread" in {

    def fib(n: Int): Task[Int] = n match {
      case 0 | 1 ⇒ Task now 1
      case n ⇒
        for {
          x ← fib(n - 1)
          y ← fib(n - 2)
        } yield x + y
    }

    //run/attemptRun is like get on Future
    fib(15).attemptRun should ===(\/-(987))

    //the same
    Task.delay(fib2(16)).attemptRun should ===(\/-(987))
  }

  "Run fib with explicitly asking for concurrency" should "run in pool" in {
    def fib(n: Int): Task[Int] = n match {
      case 0 | 1 ⇒ Task now 1
      case n ⇒
        val ND = Nondeterminism[Task]
        for {
          r ← ND.mapBoth(Task.fork(fib(n - 1)), Task.fork(fib(n - 2)))(_ + _)
        } yield { logger.info(r); r }
    }
    fib(15).attemptRun should ===(\/-(987))
  }

  "Run factorial" should "run single thread" in {
    object Big { def unapply(n: BigInt) = Some(n.toInt) }

    def factorial(n: BigInt): Task[BigInt] = n match {
      case Big(1) ⇒ Task now 1
      case n      ⇒ for { r ← factorial(n - 1) } yield { n * r }
    }

    factorial(30).attemptRun should ===(\/-(BigInt("265252859812191058636308480000000")))
  }

  "Apply a function to n results" should "nondeterminstically ordering their effects" in {
    import scalaz.concurrent.Task
    import scalaz.Nondeterminism

    val a = Task.fork { Thread.sleep(1000); Task.now("a") }
    val b = Task.fork { Thread.sleep(800); Task.now("b") }
    val c = Task.fork { Thread.sleep(600); Task.now("c") }
    val d = Task.fork { Thread.sleep(200); Task.now("d") }

    val r = Nondeterminism[Task].nmap4(a, b, c, d)(List(_, _, _, _))
    val list = r.run

    list.size should equal(4)
  }

  "Aggregate results" should "with monoid" in {
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
    val r1 = task1.attemptRun
    r0 should ===(r1)
    r0 should ===(\/-(1364))
  }

  "Kleisli for pleasant choose the pool" should "run" in {
    import common.KleisliSupport._
    //This is how we can get a computation to run on precisely the pool,
    //without having to constantly pass around the explicit reference to the target pool
    val r0 = new SyncVar[Throwable \/ String]
    val r1 = new SyncVar[Throwable \/ String]

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

    result0.runAsync(r0.put)
    result1.runAsync(r1.put)

    r0.get should ===(\/-("io-worker-thread-1-x - io-worker-thread-2-y"))
    r1.get should ===(\/-("cpu-worker-thread-1-x - cpu-worker-thread-2-y"))
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

  "Task error handling with EitherT" should "run failed with first error" in {
    import scalaz.EitherT._

    val flow: Task[NumberFormatException \/ Int] =
      (for {
        a ← serviceR("a") |> eitherT
        b ← serviceR("b") |> eitherT
      } yield (a + b)).run

    val r = flow.run
    logger.info(r)
    //new NumberFormatException("For input string: "a""))
    r.isLeft.shouldEqual(true)
  }

  "Error accumulation with ValidationNel" should "run succeed" in {
    val flowV = (serviceV("a") |@| serviceV("5")) {
      case (a, b) ⇒ s"serviceA result:$a  serviceV result:$b"
    }

    flowV.isFailure shouldEqual true
    flowV.shows shouldEqual "Failure([\"For input string: \"a\"\",\"5 should be > 5\"])"
  }

  "Task error handling with EitherT succeed" should "run" in {
    import scalaz.EitherT._

    val flow: Task[NumberFormatException \/ Int] =
      (for {
        a ← serviceR("1") |> eitherT
        b ← serviceR("2") |> eitherT
      } yield (a + b)).run

    flow.run should ===(\/-(3))
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
      val op = implicitly[Op[T]]
      Task(\/.fromTryCatchThrowable[T, Exception](op.sum(left, right))).map {
        case \/-(r)  ⇒ \/-(r)
        case -\/(ex) ⇒ -\/(ex.getMessage)
      }
    }
  }

  "Task error handling with Reader" should "run succeed" in {
    sReader[Int].run(1)(2).run should ===(\/-(3))
  }

  "Task error handling with Reader" should "run failed with first error" in {
    val r = sReader[String].run("asw")("2").run
    logger.info(r)
    //-\/(java.lang.NumberFormatException: For input string: "asw")
    r.isLeft shouldEqual true
  }

  "Task error handling with 2 independent Readers" should "composition" in {

    val x = sReader[Int].run(1)
    val y = sReader[Int].run(2)

    val f = for {
      a ← x(3)
      b ← y(6)
    } yield a |+| b

    val result = f.run
    result should ===(-\/(" 6 should be < 5 "))

  }

  "Task error handling with 2 dependent Readers" should "composition" in {
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
    r should ===(-\/(" 6 should be < 5 "))
  }

  "Task with RuntimeException" should "throw" in {
    val thrown = the[RuntimeException] thrownBy {
      Task.delay { throw new RuntimeException("expected") } run
    }

    thrown.getMessage shouldEqual "expected"
  }

  "Task with Error" should "throw" in {
    val thrown = the[Error] thrownBy {
      Task.delay { throw new OutOfMemoryError("oem") } run
    }

    thrown.getMessage shouldEqual "oem"
  }

  "Ackermann with Task and limited stack" should "run" in {
    //http://blog.higher-order.com/blog/2015/06/18/easy-performance-wins-with-scalaz/
    import scalaz.concurrent.Task, Task._

    def ackermann(m: Int, n: Int): Task[Int] = {
      (m, n) match {
        case (0, _) ⇒ now(n + 1)
        case (m, 0) ⇒ suspend(ackermann(m - 1, 1))
        case (m, n) ⇒
          suspend(ackermann(m, n - 1)).flatMap { x ⇒
            suspend(ackermann(m - 1, x))
          }
      }
    }

    /**
     * In ackermann we’re making too many jumps back to the trampoline with suspend.
     * We don’t actually need to suspend and return control to the trampoline at each step.
     * We only need to do it enough times to avoid overflowing the stack.
     * We can then keep track of how many recursive calls we’ve made,
     * and jump on the trampoline only when we need to. In this case 256
     */
    val maxStack = 256
    def ackermannO(m: Int, n: Int): Task[Int] = {
      def step(m: Int, n: Int, stack: Int): Task[Int] =
        if (stack >= maxStack) suspend(ackermannO(m, n))
        else go(m, n, stack + 1)

      def go(m: Int, n: Int, stack: Int): Task[Int] =
        (m, n) match {
          case (0, _) ⇒ now(n + 1)
          case (m, 0) ⇒ step(m - 1, 1, stack)
          case (m, n) ⇒ for {
            internalRec ← step(m, n - 1, stack)
            result ← step(m - 1, internalRec, stack)
          } yield result
        }

      go(m, n, 0)
    }
    1 should ===(1)
  }
}