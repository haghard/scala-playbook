package scalaz

import java.util.concurrent.Executors._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ThreadFactory, Executors, CountDownLatch, TimeUnit }

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
import monifu.reactive.Ack.{ Cancel, Continue }
import rx.lang.scala.schedulers.NewThreadScheduler
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.{ ExecutionContext, Future }
import scalaz.concurrent.{ Strategy, Task }
import java.util.concurrent.atomic.{ AtomicReference ⇒ JavaAtomicReference }

class ComposableEffects extends Specification {
  val P = scalaz.stream.Process
  val logger = Logger.getLogger("effects")

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
  case class User(id: Long, name: String)
  case class Address(street: String = "Baker Street 221B")

  /*getUserF(id) flatMap { user ⇒
      getUserAddressF(user) flatMap { a ⇒
        logF(s"[${t.runtimeClass.getName}] fetch address $a for user $user").map(_ ⇒ a)
      }
  }*/
  //Functional composition of effects
  def flow[M[_]: Monad](getUserF: Long ⇒ M[User],
                        getUserAddressF: User ⇒ M[Address],
                        logF: String ⇒ M[Unit])(id: Long)(implicit t: ClassTag[M[_]]): M[Address] = {
    val cName = t.runtimeClass.getName
    for {
      _ ← logF(s"1.[$cName] Searching user for id $id")
      user ← getUserF(id)
      _ ← logF(s"2.[$cName] Searching address for user $user")
      address ← getUserAddressF(user)
      //_ ← logF(s"[$cName] fetched address $address for user $user")
    } yield address
  }

  def kleisliFlow[M[_]: Monad](getUserF: Long ⇒ M[User],
                               getUserAddressF: User ⇒ M[Address],
                               logF: String ⇒ M[Unit])(implicit t: ClassTag[M[_]]): Kleisli[M, Long, Address] = {
    import scalaz.Kleisli._
    val cName = t.runtimeClass.getName
    kleisli(getUserF.compose { id: Long ⇒ logger.info(s"1.[$cName] Searching user for id $id"); id }) >=>
      kleisli(getUserAddressF.compose { u: User ⇒ logger.info(s"2.[$cName] Searching address for user $u"); u })
  }

  /*"Id monad effect" in {
    val getUserF: Long ⇒ Id[User] = id ⇒ User(id, "Sherlock")
    val getAddressF: User ⇒ Id[Address] = user ⇒ Address()
    val logF: String ⇒ Id[Unit] = r ⇒ logger.info(r)

    flow[Id](getUserF, getAddressF, logF)(99l) should be equalTo Address()
  }*/

  "Absent/Present value effect with Option" in {
    val getUserF: Long ⇒ Option[User] = id ⇒ Option(User(id, "Sherlock"))
    val getAddressF: User ⇒ Option[Address] = user ⇒ Some(Address())
    val logF: String ⇒ Option[Unit] = r ⇒ Option(logger.info(r))

    flow[Option](getUserF, getAddressF, logF)(99l) should be equalTo Some(Address())
  }

  "Absent/Present value effect with Kleisli and Option" in {
    val getUserF: Long ⇒ Option[User] = id ⇒ Option(User(id, "Sherlock"))
    val getAddressF: User ⇒ Option[Address] = user ⇒ Some(Address())
    val logF: String ⇒ Option[Unit] = r ⇒ Option(logger.info(r))

    kleisliFlow[Option](getUserF, getAddressF, logF).run(99l) should be equalTo Some(Address())
  }

  /*
  "Latency effect with Future" in {
    val getUserF: Long ⇒ Future[User] = id ⇒ Future(User(id, "Sherlock"))
    val getAddressF: User ⇒ Future[Address] = id ⇒ Future(Address())
    val logF: String ⇒ Future[Unit] = r ⇒ Future(logger.info(r))

    import scala.concurrent.Await
    Await.result(flow[Future](getUserF, getAddressF, logF)(99l),
      new FiniteDuration(1, TimeUnit.SECONDS)) should be equalTo Address()
  }

  "Concurrency effect with Task" in {
    val getUserF: Long ⇒ Task[User] = id ⇒ Task.now(User(id, "Sherlock"))
    val getAddressF: User ⇒ Task[Address] = id ⇒ Task.now(Address())
    val logF: String ⇒ Task[Unit] = r ⇒ Task.now(logger.info(r))

    flow[Task](getUserF, getAddressF, logF)(99l).run should be equalTo Address()
  }

  "Scalar or Vector response with monifu.Observable" in {
    import monifu.reactive._
    import monifu.concurrent.Scheduler

    val P = Scheduler.computation(2)
    implicit val C = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, namedTF("consumer")))
    val seq = List(Address(), Address(), Address(), Address())

    val latch = new CountDownLatch(1)
    val register = new AtomicRegister(List[Address]())

    implicit val monifuM = new Monad[Observable]() {
      override def point[A](a: ⇒ A): Observable[A] = Observable.unit(a)
      override def bind[A, B](fa: Observable[A])(f: (A) ⇒ Observable[B]): Observable[B] = fa flatMap f
    }

    val getUserF: Long ⇒ Observable[User] = id ⇒ Observable.unit(User(id, "Sherlock")).subscribeOn(P)
    val getAddressByUser: User ⇒ Observable[Address] = u ⇒ Observable.fromIterable(seq).subscribeOn(P)
    val logF: String ⇒ Observable[Unit] = r ⇒ Observable.unit(logger.info(r))

    flow[Observable](getUserF, getAddressByUser, logF)(261l)
      .subscribe(new Observer[Address] {
        def onNext(elem: Address) = Future {
          logF(s"Observer consumed $elem")
          val results = register.transactAndGet { elem :: _ }
          if (results.size == 4) {
            latch.countDown()
            Cancel
          } else Continue
        }(C)

        def onError(ex: scala.Throwable): scala.Unit = {
          logF("Error: " + ex.getMessage)
          latch.countDown()
        }
        def onComplete(): scala.Unit = {}
      })(P)

    latch.await()
    register.get.reverse should be equalTo seq
  }

  "Scalar or Vector response with rx.Observable" in {
    import rx.lang.scala.{ Observable ⇒ RxObservable }
    import rx.lang.scala.Notification.{ OnError, OnCompleted, OnNext }

    val P = NewThreadScheduler()
    val register = new AtomicRegister(List[Address]())
    val latch = new CountDownLatch(1)

    val logF: String ⇒ RxObservable[Unit] =
      r ⇒ RxObservable.just(logger.info(r))

    val getUserById: Long ⇒ RxObservable[User] =
      id ⇒ RxObservable.defer {
        logF("RxObservable producer getUserById")
        RxObservable.just(User(id, "Sherlock"))
      }.subscribeOn(P)

    val getAddressByUser: User ⇒ RxObservable[Address] =
      user ⇒ RxObservable.defer {
        logF("RxObservable producer getAddressByUser")
        RxObservable.from(Seq(Address("Baker street 1"), Address("Baker street 2")))
      }.subscribeOn(P)

    implicit val M = new Monad[RxObservable]() {
      override def point[A](a: ⇒ A): RxObservable[A] = RxObservable.just(a)
      override def bind[A, B](fa: RxObservable[A])(f: (A) ⇒ RxObservable[B]): RxObservable[B] = fa flatMap f
    }

    flow[RxObservable](getUserById, getAddressByUser, logF)(99l)
      .observeOn(rx.lang.scala.schedulers.ComputationScheduler())
      .materialize.subscribe { n ⇒
        n match {
          case OnNext(v) ⇒
            logF("RxObserver consume Address")
            register.transact { v :: _ }
          case OnCompleted  ⇒ latch.countDown
          case OnError(err) ⇒ println("Error: " + err.getMessage)
        }
      }

    latch.await()
    register.get.size should be equalTo 2
  }

  "Scalar or Vector response with scalaz.Process" in {
    import scalaz.stream.Process

    type PTask[X] = Process[Task, X]

    val seq = (1 to 100).toSeq
    val buf = Buffer.empty[Address]

    val parallelism = Runtime.getRuntime.availableProcessors() / 2
    val ioE = newFixedThreadPool(parallelism, new NamedThreadFactory("remote-process"))

    def resource(i: Int): Process[Task, Address] = P.eval(Task {
      Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate blocking call
      Address("Baker street " + i)
    }(ioE))

    def resource2(i: Int): Task[Address] = Task {
      Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate blocking call
      Address("Baker street" + i)
    }(ioE)

    val source: Process[Task, Process[Task, Address]] = emitAll(seq) map { resource _ }

    val source2: Process[Task, Task[Address]] = emitAll(seq) map { resource2 _ }

    val getUserF: (Long ⇒ Process[Task, User]) =
      id ⇒ P.eval(Task {
        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
        User(id, "Sherlock")
      }(ioE))

    val getUserAddressF: (User ⇒ Process[Task, Address]) =
      user ⇒
        scalaz.stream.merge.mergeN(parallelism * 2)(source)(Strategy.Executor(ioE))

    val getUnOrderedAddress2: (User ⇒ Process[Task, Address]) =
      user ⇒
        source2 gather parallelism

    val getOrderedAddress3: (User ⇒ Process[Task, Address]) =
      user ⇒
        source2 sequence parallelism

    val logF: String ⇒ Process[Task, Unit] =
      r ⇒ P.eval(Task.delay(logger.info(r)))

    val prog = flow[PTask](getUserF, getUserAddressF, logF)(99l)
    //val Prog = flow[({ type λ[x] = Process[Task, x] })#λ](getUserF, getUserAddressF, logF)(99l)

    val r = (prog to io.fillBuffer(buf)).run.attemptRun

    r should be equalTo \/-(())
    seq.size should be equalTo buf.size
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
  }*/
}