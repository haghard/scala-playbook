package scalaz

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import org.specs2.mutable.Specification
import rx.lang.scala.schedulers.{ NewThreadScheduler, ComputationScheduler }
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scalaz.Scalaz._
import scalaz.concurrent.Task
import scala.language.higherKinds

class GenericEffects extends Specification {

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

  "Scalar or Vector response with rx.Observable" in {
    import rx.lang.scala._

    val getUserById: Long ⇒ Observable[User] =
      id ⇒ Observable.defer {
        println(Thread.currentThread().getName + ": producer getUserById")
        Observable.just(User(id, "Sherlock"))
      }.subscribeOn(NewThreadScheduler())

    val getAddressByUser: User ⇒ Observable[Address] =
      user ⇒ Observable.defer {
        println(Thread.currentThread().getName + ": producer getAddressByUser")
        Observable.from(Seq(Address("Baker street 1"), Address("Baker street 2")))
      }.subscribeOn(NewThreadScheduler())

    implicit val M = new Monad[Observable]() {
      override def point[A](a: ⇒ A): Observable[A] = Observable.just(a)
      override def bind[A, B](fa: Observable[A])(f: (A) ⇒ Observable[B]): Observable[B] = fa flatMap f
    }

    import rx.lang.scala.Notification.{ OnError, OnCompleted, OnNext }

    final class AtomicRegister[A](init: A) extends java.util.concurrent.atomic.AtomicReference[A](init) {
      final def transact(f: A ⇒ A): Unit = {
        @annotation.tailrec
        def attempt(): A = {
          val a = get
          val next = f(a)
          if (compareAndSet(a, next)) next else attempt
        }
        attempt
      }
    }

    val register = new AtomicRegister(List[Address]())
    val latch = new CountDownLatch(1)

    (fetch[Observable](getUserById, getAddressByUser)(99l))
      .observeOn(ComputationScheduler())
      .materialize.subscribe { n ⇒
        n match {
          case OnNext(v)    ⇒ register.transact { list: List[Address] ⇒ v :: list }
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