package scalaz

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ThreadFactory, Executors, CountDownLatch, TimeUnit }

import monifu.reactive.Ack.{ Cancel, Continue }
import org.specs2.mutable.Specification
import rx.lang.scala.schedulers.NewThreadScheduler
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scalaz.Scalaz._
import scalaz.concurrent.Task
import scala.language.higherKinds

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
      u ⇒ Observable.fromIterable(seq).subscribeOn(P)
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

    def addresses = {
      def go(n: String): Process[Task, Address] =
        P.await(Task.delay(n))(i ⇒ P.emit(Address(i)) ++ go(i + " !"))
      go("Baker street")
    }

    val getUserById: Long ⇒ Process[Task, User] =
      id ⇒ P.emit(User(id, "Sherlock"))

    val gerAddressByUser: User ⇒ Process[Task, Address] =
      id ⇒ addresses.take(3)

    (fetch[({ type λ[x] = Process[Task, x] })#λ](getUserById, gerAddressByUser)(99l))
      .runLog.run should be equalTo Vector(Address("Baker street"),
        Address("Baker street !"), Address("Baker street ! !"))
  }
}