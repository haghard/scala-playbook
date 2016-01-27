package scalaz

import java.util.concurrent.Executors._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ThreadFactory, CountDownLatch, TimeUnit }

import scalaz.Scalaz._
import scalaz.stream.io
import scala.reflect.ClassTag
import org.apache.log4j.Logger
import scalaz.stream.Process._
import scala.language.higherKinds
import scala.collection.mutable.Buffer
import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory
import scala.concurrent.duration.FiniteDuration
import rx.lang.scala.schedulers.ExecutionContextScheduler
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.{ ExecutionContext, Future }
import scalaz.concurrent.{ Strategy, Task }
import java.util.concurrent.atomic.{ AtomicReference ⇒ JavaAtomicReference }

class FunctionCompositionWithEffectsSpec extends Specification {
  val P = scalaz.stream.Process
  val logger = Logger.getLogger("flow")

  final class AtomicRegister[A](init: A) extends JavaAtomicReference[A](init) {
    @annotation.tailrec def attempt(f: A ⇒ A): A = {
      val current = get
      val updated = f(current)
      if (compareAndSet(current, updated)) updated else attempt(f)
    }

    def transact(f: A ⇒ A): Unit = {
      attempt(f)
      ()
    }

    def transactAndGet(f: A ⇒ A): A = attempt(f)
  }

  def namedTF(name: String) = new ThreadFactory {
    val num = new AtomicInteger(1)
    def newThread(runnable: Runnable) = new Thread(runnable, s"$name - ${num.incrementAndGet}")
  }

  //Domain
  case class User(id: Long, name: String, ids: List[Int] = Nil)
  case class Address(street: String = "Baker Street 221B")

  //_ ← logF(s"[$cName] fetched address $address for user $user")

  //Modes from Rapture !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  /**
   * Acts like
   * {{{
   *  getUserF(id) flatMap { user ⇒
   *    getUserAddressF(user) flatMap { a ⇒
   *      logF(s"[${t.runtimeClass.getName}] fetch address $a for user $user").map(_ ⇒ a)
   *   }
   * }
   * }}}
   *
   * @return M[Address]
   */
  def flow[M[_]: Monad](getUserF: Long ⇒ M[User],
                        getUserAddressF: User ⇒ M[Address],
                        logF: String ⇒ M[Unit])(id: Long)(implicit t: ClassTag[M[_]]): M[Address] = {
    val cName = t.runtimeClass.getName
    for {
      _ ← logF(s"1.[$cName] Searching user for id $id")
      user ← getUserF(id)
      _ ← logF(s"2.[$cName] Searching address for user $user")
      address ← getUserAddressF(user)
    } yield address
  }

  /**
   *  Kleisli composition
   */
  def kleisliFlow[M[_]: Monad](getUserF: Long ⇒ M[User],
                               getUserAddressF: User ⇒ M[Address],
                               logF: String ⇒ M[Unit])(implicit t: ClassTag[M[_]]): Kleisli[M, Long, Address] = {
    import scalaz.Kleisli._
    val cName = t.runtimeClass.getName
    kleisli(getUserF.compose { id: Long ⇒ logger.info(s"1.[$cName] Searching user for id $id"); id }) >=>
      kleisli(getUserAddressF.compose { u: User ⇒ logger.info(s"2.[$cName] Searching address for user $u"); u })
  }

  "Id monad effect" should {
    val getUserF: Long ⇒ Id[User] = id ⇒ User(id, "Sherlock")
    val getAddressF: User ⇒ Id[Address] = user ⇒ Address()
    val logF: String ⇒ Id[Unit] = r ⇒ logger.info(r)

    "have run with flow" in {
      flow[Id](getUserF, getAddressF, logF)(99l) should be equalTo Address()
    }

    "have run with kleisliFlow" in {
      kleisliFlow[Id](getUserF, getAddressF, logF).run(99l) should be equalTo Address()
    }
  }

  "Absent/Present value effect with Option" should {
    val getUserF: Long ⇒ Option[User] = id ⇒ Option(User(id, "Sherlock"))
    val getAddressF: User ⇒ Option[Address] = user ⇒ Some(Address())
    val logF: String ⇒ Option[Unit] = r ⇒ Option(logger.info(r))

    "have run with flow" in {
      flow[Option](getUserF, getAddressF, logF)(99l) should be equalTo Some(Address())
    }

    "have run with kleisliFlow" in {
      kleisliFlow[Option](getUserF, getAddressF, logF).run(99l) should be equalTo Some(Address())
    }
  }

  "Latency effect with Future" should {
    import scala.concurrent.Await
    val sleep = new FiniteDuration(1, TimeUnit.SECONDS)
    val getUserF: Long ⇒ Future[User] = id ⇒ Future(User(id, "Sherlock"))
    val getAddressF: User ⇒ Future[Address] = id ⇒ Future(Address())
    val logF: String ⇒ Future[Unit] = r ⇒ Future(logger.info(r))

    "have run with flow" in {
      val f = flow[Future](getUserF, getAddressF, logF)(99l)
      Await.result(f, sleep) should be equalTo Address()
    }

    "have run with kleisliFlow" in {
      val f = kleisliFlow[Future](getUserF, getAddressF, logF).run(99l)
      Await.result(f, sleep) should be equalTo Address()
    }
  }

  "Concurrency effect with Task" should {
    implicit val pool = java.util.concurrent.Executors.newFixedThreadPool(1)
    val getUserF: Long ⇒ Task[User] = id ⇒ Task(User(id, "Sherlock"))
    val getAddressF: User ⇒ Task[Address] = id ⇒ Task(Address())
    val logF: String ⇒ Task[Unit] = r ⇒ Task(logger.info(r))

    "have run with flow" in {
      flow[Task](getUserF, getAddressF, logF)(99l).run should be equalTo Address()
    }

    "have run with kleisliFlow" in {
      kleisliFlow[Task](getUserF, getAddressF, logF).run(99l).run should be equalTo Address()
    }
  }

  "Scalar or Vector response with rx.Observable" should {
    import rx.lang.scala.{ Observable ⇒ RxObservable }
    import rx.lang.scala.Notification.{ OnError, OnCompleted, OnNext }

    val executor = newFixedThreadPool(2, new NamedThreadFactory("observable-flow"))
    val RxScheduler = ExecutionContextScheduler(ExecutionContext.fromExecutor(executor))
    val addresses = List(Address("Baker street 1"), Address("Baker street 2"))

    val logF: String ⇒ RxObservable[Unit] =
      r ⇒ RxObservable.just(logger.info(r))

    val getUserById: Long ⇒ RxObservable[User] =
      id ⇒ RxObservable.defer {
        logF(s"Observable-Producer fetch user by $id")
        RxObservable.just(User(id, "Sherlock"))
      }.subscribeOn(RxScheduler)

    val getAddressByUser: User ⇒ RxObservable[Address] =
      user ⇒ RxObservable.defer {
        logF(s"Observable-Producer fetch addresses by $user")
        RxObservable.from(addresses)
      }.subscribeOn(RxScheduler)

    implicit val M = new Monad[RxObservable]() {
      override def point[A](a: ⇒ A): RxObservable[A] = RxObservable.just(a)
      override def bind[A, B](fa: RxObservable[A])(f: (A) ⇒ RxObservable[B]): RxObservable[B] = fa flatMap f
    }

    "have run with flow" in {
      val register = new AtomicRegister(List[Address]())
      val latch = new CountDownLatch(1)

      flow[RxObservable](getUserById, getAddressByUser, logF)(99l)
        .observeOn(RxScheduler)
        .materialize.subscribe { n ⇒
          n match {
            case OnNext(v) ⇒
              logF(s"Observable-Consumer receives $v")
              register.transact {
                v :: _
              }
            case OnCompleted  ⇒ latch.countDown
            case OnError(err) ⇒ println("Error: " + err.getMessage)
          }
        }
      latch.await(5, TimeUnit.SECONDS) === true
      register.get.reverse should be equalTo addresses
    }

    "have run with kleisliFlow" in {
      val register = new AtomicRegister(List[Address]())
      val latch = new CountDownLatch(1)
      kleisliFlow[RxObservable](getUserById, getAddressByUser, logF).run(98l)
        .observeOn(RxScheduler)
        .materialize.subscribe { n ⇒
          n match {
            case OnNext(v) ⇒
              logF(s"Observable-Consumer receives $v")
              register.transact {
                v :: _
              }
            case OnCompleted  ⇒ latch.countDown
            case OnError(err) ⇒ println("Error: " + err.getMessage)
          }
        }
      latch.await(5, TimeUnit.SECONDS) === true
      register.get.reverse should be equalTo addresses
    }
  }

  "Scalar or Vector response with scalaz.Process" should {
    import scalaz.stream.Process
    type PTask[X] = Process[Task, X]

    val Limit = 100

    //val Prog = flow[({ type λ[x] = Process[Task, x] })#λ](getUserF, getUserAddressF, logF)(99l)
    "have run with flow and mergeN" in {
      val logger = Logger.getLogger("process-flow-mergeN")
      val parLevel = Runtime.getRuntime.availableProcessors() / 2
      val ioE = newFixedThreadPool(parLevel, new NamedThreadFactory("flow-mergeN-thread"))
      val LoggerS = scalaz.stream.sink.lift[Task, Address] { a ⇒ Task.delay(logger.info(a)) }

      val logF: String ⇒ Process[Task, Unit] =
        r ⇒ P.eval(Task.delay(logger.info(r)))

      def getUserF: Long ⇒ Process[Task, User] =
        id ⇒ P.eval(Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
          User(id, "Sherlock", List.range(0, Limit))
        }(ioE))

      def addresses(i: Int): Process[Task, Address] =
        P.eval(Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate blocking call
          Address("Baker street " + i)
        }(ioE))

      def source(ids: Seq[Int]): Process[Task, Process[Task, Address]] =
        emitAll(ids) map { addresses _ }

      def getUserAddressF: User ⇒ Process[Task, Address] =
        user ⇒
          scalaz.stream.merge.mergeN(parLevel)(source(user.ids))(Strategy.Executor(ioE))

      val buf = Buffer.empty[Address]
      val prog = flow[PTask](getUserF, getUserAddressF, logF)(99l)
      val f = (prog observe LoggerS to io.fillBuffer(buf)).run.attemptRun
      f.isRight === true
      buf.size === Limit
    }

    "have run with kleisliFlow and mergeN" in {
      val logger = Logger.getLogger("process-kleisliFlow-mergeN")
      val parLevel = Runtime.getRuntime.availableProcessors() / 2
      val ioE = newFixedThreadPool(parLevel, new NamedThreadFactory("kleisliFlow-mergeN-thread"))
      val LoggerS = scalaz.stream.sink.lift[Task, Address] { a ⇒ Task.delay(logger.info(a)) }

      val logF: String ⇒ Process[Task, Unit] =
        r ⇒ P.eval(Task.delay(logger.info(r)))

      def getUserF: Long ⇒ Process[Task, User] =
        id ⇒ P.eval(Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
          User(id, "Sherlock", List.range(0, Limit))
        }(ioE))

      def addresses(i: Int): Process[Task, Address] =
        P.eval(Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate remote call
          Address("Baker street " + i)
        }(ioE))

      def source(ids: Seq[Int]): Process[Task, Process[Task, Address]] =
        emitAll(ids) map { addresses _ }

      def getUserAddressF: User ⇒ Process[Task, Address] =
        user ⇒
          scalaz.stream.merge.mergeN(parLevel)(source(user.ids))(Strategy.Executor(ioE))

      val buf = Buffer.empty[Address]
      val prog = kleisliFlow[PTask](getUserF, getUserAddressF, logF).run(99l)
      val f = (prog observe LoggerS to io.fillBuffer(buf)).run.attemptRun
      f.isRight === true
      buf.size === Limit
    }

    "have run flow using gather for unordered address" in {
      val logger = Logger.getLogger("process-gather")
      val parLevel = Runtime.getRuntime.availableProcessors() / 2
      val ioE = newFixedThreadPool(parLevel, new NamedThreadFactory("gather-thread"))
      val LoggerS = scalaz.stream.sink.lift[Task, Address] { a ⇒ Task.delay(logger.info(a)) }

      val logF: String ⇒ Process[Task, Unit] =
        r ⇒ P.eval(Task.delay(logger.info(r)))

      def getUserF: Long ⇒ Process[Task, User] =
        id ⇒ P.eval(Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
          User(id, "Sherlock", List.range(0, Limit))
        }(ioE))

      def addresses(i: Int): Task[Address] =
        Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate remote call
          Address("Baker street " + i)
        }(ioE)

      def source(ids: Seq[Int]): Process[Task, Task[Address]] =
        emitAll(ids) map { addresses _ }

      def getUnorderedAddress: User ⇒ Process[Task, Address] =
        user ⇒ source(user.ids) gather parLevel

      val buf = Buffer.empty[Address]
      val prog = flow[PTask](getUserF, getUnorderedAddress, logF)(99l)
      val f = (prog observe LoggerS to io.fillBuffer(buf)).run.attemptRun
      f.isRight === true
      buf.size === Limit
    }

    "have run flow using sequence for ordered address" in {
      val logger = Logger.getLogger("process-sequence")
      val parLevel = Runtime.getRuntime.availableProcessors() / 2
      val ioE = newFixedThreadPool(parLevel, new NamedThreadFactory("gather-sequence"))
      val LoggerS = scalaz.stream.sink.lift[Task, Address] { a ⇒ Task.delay(logger.info(a)) }

      val logF: String ⇒ Process[Task, Unit] =
        r ⇒ P.eval(Task.delay(logger.info(r)))

      def getUserF: Long ⇒ Process[Task, User] =
        id ⇒ P.eval(Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
          User(id, "Sherlock", List.range(0, Limit))
        }(ioE))

      def addresses(i: Int): Task[Address] =
        Task {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate remote call
          Address("Baker street " + i)
        }(ioE)

      def source(ids: Seq[Int]): Process[Task, Task[Address]] =
        emitAll(ids) map { addresses _ }

      def getOrderedAddress: User ⇒ Process[Task, Address] =
        user ⇒ source(user.ids) sequence parLevel

      val buf = Buffer.empty[Address]
      val prog = flow[PTask](getUserF, getOrderedAddress, logF)(99l)
      val f = (prog observe LoggerS to io.fillBuffer(buf)).run.attemptRun
      f.isRight === true
      buf.size === Limit
    }
  }

  "Writer monad for testing" in {
    val id = 199l
    type W[X] = Writer[Map[String, String], X]

    val getUserF: Long ⇒ W[User] =
      id ⇒ {
        val u = User(id, "Sherlock")
        u.set(Map(id.toString -> u.name))
      }

    val getAddressF: User ⇒ W[Address] =
      user ⇒ {
        val a = Address()
        a.set(Map("address" -> a.street))
      }

    val logF: String ⇒ W[Unit] =
      r ⇒ logger.info(r).point[W]

    val rMap = flow[W](getUserF, getAddressF, logF)(id).run._1
    //val rMap = program[({ type λ[x] = Writer[Map[String, String], x] })#λ](getUserF, getAddressF, logF)(id).run._1

    rMap.get(id.toString) should be equalTo Some("Sherlock")
    rMap.get("address") should be equalTo Some(Address().street)
  }
}