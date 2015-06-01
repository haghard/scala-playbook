package scalaz

import java.util.concurrent.Executors._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ThreadFactory, Executors, CountDownLatch, TimeUnit }

import mongo.MongoProgram.NamedThreadFactory
import monifu.reactive.Ack.{ Cancel, Continue }
import org.specs2.mutable.Specification
import rx.lang.scala.schedulers.NewThreadScheduler
import scala.collection.mutable.Buffer
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scalaz.Scalaz._
import scalaz.concurrent.{ Strategy, Task }
import scala.language.higherKinds
import scalaz.stream.Process._
import scalaz.stream.{ process1, io }

class GenericEffects extends Specification {

  import java.util.concurrent.atomic.{ AtomicReference ⇒ JavaAtomicReference }

  final class AtomicRegister[A](init: A) extends JavaAtomicReference[A](init) {
    final def transact(f: A ⇒ A): Unit = {
      @annotation.tailrec
      def attempt: A = {
        val current = get
        val updated = f(current)
        if (compareAndSet(current, updated)) updated else attempt
      }
      attempt
    }

    final def transactAndGet(f: A ⇒ A): A = {
      @annotation.tailrec
      def attempt: A = {
        val current = get
        val updated = f(get)
        if (compareAndSet(current, updated)) updated else attempt
      }
      attempt
    }
  }

  def namedTF(name: String) = new ThreadFactory {
    val num = new AtomicInteger(1)
    def newThread(runnable: Runnable) = new Thread(runnable, s"$name - ${num.incrementAndGet}")
  }

  //Domain
  case class User(id: Long, name: String)
  case class Address(street: String)

  def fetch[M[_]: Monad](userById: Long ⇒ M[User],
                         addressByUser: User ⇒ M[Address])(id: Long): M[Address] =
    for {
      user ← userById(id)
      address ← addressByUser(user)
    } yield address

  "Id monad effect" in {
    val getUserById: Long ⇒ Id[User] =
      id ⇒ User(id, "Sherlock")

    val gerAddressByUser: User ⇒ Id[Address] =
      id ⇒ Address("Baker street")

    fetch[Id](getUserById, gerAddressByUser)(99l) should be equalTo Address("Baker street")
  }

  "Absent/Present value effect with Option" in {
    (fetch[Option](
      { id ⇒ Some(User(id, "Sherlock")) },
      { user ⇒ Some(Address("Baker street")) })(99l)) should be equalTo Some(Address("Baker street"))
  }

  "Success/Error value effect with Disjunction" in {
    (fetch[({ type λ[x] = String \/ x })#λ](
      { id ⇒ \/-(User(id, "Sherlock")) },
      { user ⇒ -\/(s"Can't find street for user ${user.id}") })(99l)) should be equalTo -\/("Can't find street for user 99")
  }

  "Latency effect with Future" in {
    val getUserById: Long ⇒ Future[User] =
      id ⇒ Future(User(id, "Sherlock"))

    val gerAddressByUser: User ⇒ Future[Address] =
      id ⇒ Future(Address("Baker street"))

    import scala.concurrent.Await
    Await.result(fetch[Future](getUserById, gerAddressByUser)(99l),
      new FiniteDuration(1, TimeUnit.SECONDS)) should be equalTo Address("Baker street")
  }

  "Concurrency effect with Task" in {
    val getUserById: Long ⇒ Task[User] =
      id ⇒ Task.now(User(id, "Sherlock"))

    val gerAddressByUser: User ⇒ Task[Address] =
      id ⇒ Task.now(Address("Baker street"))

    (fetch[Task](getUserById, gerAddressByUser)(99l))
      .run should be equalTo Address("Baker street")
  }

  "Scalar or Vector response with monifu.Observable" in {
    import monifu.reactive._
    import monifu.concurrent.Scheduler

    val P = Scheduler.computation(2)
    implicit val C = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, namedTF("consumers")))
    val seq = List(Address("Baker street 1"), Address("Baker street 2"), Address("Baker street 3"), Address("Baker street 4"))

    val latch = new CountDownLatch(1)
    val register = new AtomicRegister(List[Address]())

    implicit val monifuM = new Monad[Observable]() {
      override def point[A](a: ⇒ A): Observable[A] = Observable.unit(a)
      override def bind[A, B](fa: Observable[A])(f: (A) ⇒ Observable[B]): Observable[B] = fa flatMap f
    }

    val getUserById: Long ⇒ Observable[User] = {
      id ⇒
        Observable.unit({
          println(Thread.currentThread().getName + ": Observable producer User")
          User(id, "Sherlock")
        }).subscribeOn(P)
    }

    val getAddressByUser: User ⇒ Observable[Address] = {
      u ⇒
        Observable.fromIterable(seq).subscribeOn(P)
    }

    (fetch[Observable](getUserById, getAddressByUser)(261l))
      .subscribe(new Observer[Address] {
        def onNext(elem: Address) = Future {
          println(Thread.currentThread().getName + s": Observer consume $elem")
          val results = register.transactAndGet { elem :: _ }
          if (results.size == 4) {
            latch.countDown()
            Cancel
          } else Continue
        }(C)

        def onError(ex: scala.Throwable): scala.Unit = {
          println("Error: " + ex.getMessage)
          latch.countDown()
        }
        def onComplete(): scala.Unit = println("onComplete")
      })(P)

    latch.await()
    register.get.reverse should be equalTo seq
  }

  "Scalar or Vector response with rx.Observable" in {
    import rx.lang.scala.{ Observable ⇒ RxObservable }

    val P = NewThreadScheduler()
    val register = new AtomicRegister(List[Address]())
    val latch = new CountDownLatch(1)

    val getUserById: Long ⇒ RxObservable[User] =
      id ⇒ RxObservable.defer {
        println(Thread.currentThread().getName + ": RxObservable producer getUserById")
        Thread.sleep(1000)
        RxObservable.just(User(id, "Sherlock"))
      }.subscribeOn(P)

    val getAddressByUser: User ⇒ RxObservable[Address] =
      user ⇒ RxObservable.defer {
        println(Thread.currentThread().getName + ": RxObservable producer getAddressByUser")
        RxObservable.from(Seq(Address("Baker street 1"), Address("Baker street 2")))
      }.subscribeOn(P)

    implicit val M = new Monad[RxObservable]() {
      override def point[A](a: ⇒ A): RxObservable[A] = RxObservable.just(a)
      override def bind[A, B](fa: RxObservable[A])(f: (A) ⇒ RxObservable[B]): RxObservable[B] = fa flatMap f
    }

    import rx.lang.scala.Notification.{ OnError, OnCompleted, OnNext }

    (fetch[RxObservable](getUserById, getAddressByUser)(99l))
      .observeOn(rx.lang.scala.schedulers.ComputationScheduler())
      .materialize.subscribe { n ⇒
        n match {
          case OnNext(v) ⇒
            println(Thread.currentThread().getName + ": RxObserver consume Address")
            register.transact { v :: _ }
          case OnCompleted  ⇒ latch.countDown
          case OnError(err) ⇒ println("Error: " + err.getMessage)
        }
      }

    latch.await()
    register.get.reverse should be equalTo Address("Baker street 1") :: Address("Baker street 2") :: Nil
  }

  "Scalar or Vector response with scalaz.Process" in {
    import scalaz.stream.Process
    val P = scalaz.stream.Process
    val parallelism = Runtime.getRuntime.availableProcessors() / 2
    val ioE = newFixedThreadPool(parallelism, new NamedThreadFactory("remote-process"))

    val buf = Buffer.empty[Address]
    val seq = (1 to 100).toSeq

    def resource(i: Int): Process[Task, Address] = P.eval(Task {
      Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate blocking call
      println(s"${Thread.currentThread().getName} - Process fetch address for $i")
      Address("Baker street " + i)
    }(ioE))

    def resource2(i: Int): Task[Address] = Task {
      Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate blocking call
      println(s"${Thread.currentThread().getName} - Process fetch address for $i")
      Address("Baker street" + i)
    }(ioE)

    val source: Process[Task, Process[Task, Address]] = emitAll(seq) map { resource _ }

    val source2: Process[Task, Task[Address]] = emitAll(seq) map { resource2 _ }

    val getUserById: (Long ⇒ Process[Task, User]) =
      id ⇒ P.eval(Task {
        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
        println(Thread.currentThread().getName + ": Process produce user")
        User(id, "Sherlock")
      }(ioE))

    val getAddressByUser: (User ⇒ Process[Task, Address]) =
      user ⇒
        scalaz.stream.merge.mergeN(parallelism * 2)(source)(Strategy.Executor(ioE))

    val getUnOrderedAddressByUser2: (User ⇒ Process[Task, Address]) =
      user ⇒
        source2 gather parallelism

    val getOrderedAddressByUser3: (User ⇒ Process[Task, Address]) =
      user ⇒
        source2 sequence parallelism

    val Proc = fetch[({ type λ[x] = Process[Task, x] })#λ](getUserById, getAddressByUser)(99l)

    val r = (Proc to io.fillBuffer(buf)).run.attemptRun

    println(buf)

    r should be equalTo \/-(())
    seq.size should be equalTo buf.size
  }
}