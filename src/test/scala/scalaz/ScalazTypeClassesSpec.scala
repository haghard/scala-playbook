package scalaz

import java.util.concurrent.TimeUnit
import org.specs2.mutable.Specification
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scalaz._
import Scalaz._
import scala.language.higherKinds
import scalaz.concurrent.Task

object ScalazTypeClassesSpec {

  implicit class NumericAlgebraOps[T](val v: T) extends AnyVal {
    def plus[F[_]](ctx: F[T])(implicit f: Functor[F], num: Numeric[T]): F[T] =
      f.map(ctx)(num.plus(_, v))

    def minis[F[_]](ctx: F[T])(implicit f: Functor[F], num: Numeric[T]): F[T] =
      f.map(ctx)(num.minus(_, v))

    def multiply[F[_]](ctx: F[T])(implicit f: Functor[F], num: Numeric[T]): F[T] =
      f.map(ctx)(num.times(_, v))
  }
}

class ScalazTypeClassesSpec extends Specification {

  "Equal" should {
    "run" in {
      class Foo(val a: Int, val b: Int)
      implicit val eqFoo = new Equal[Foo] {
        override def equal(a1: Foo, a2: Foo) = a1.a === a2.a
      }

      assert(new Foo(1, 2) === new Foo(1, 6))
      "Hello" === "olleH".reverse
    }
  }

  "Functor" should {
    "run" in {
      "hello".some.map(_.length).get === 5

      import ScalazTypeClassesSpec._
      67.plus(10.some) === Some(77)
      1.minis(List(7, 2)) === List(6, 1)
    }
  }

  "Monoid" should {
    "foldMap using append" in {
      val origin = "Helloworld"
      val (l, r) = origin.splitAt(6)
      val actual = List(l, r).foldMap(i ⇒ i)
      actual === origin

      List(10, 21, 12).foldMap(i ⇒ i) should be equalTo 43
      List("abcd", "efghi").foldMap(_.length) should be equalTo 9
    }

    "semigroup pos" in {
      1 |+| 2 |+| 6 should be equalTo 9
      "Hello".some |+| None |+| "world".some should be equalTo Some("Helloworld")
    }
  }

  "Monoid" should {
    "for map modification" in {

      val m1 = Map(1 -> List("a", "b"), 2 -> List("aa", "bb"))
      val m2 = Map(1 -> List("c", "d"), 3 -> List("cc", "dd"))
      m1 |+| m2 should be equalTo Map(1 -> List("a", "b", "c", "d"), 2 -> List("aa", "bb"), 3 -> List("cc", "dd"))

      val m3 = Map("a" -> 1, "b" -> 1)
      val m4 = Map("a" -> 1, "c" -> 1)
      m3 |+| m4 should be equalTo Map("a" -> 2, "b" -> 1, "c" -> 1)

      List("a", "b", "b", "b", "a", "c")
        .foldMap(i ⇒ Map(i -> 1)) should be equalTo Map("a" -> 2, "b" -> 3, "c" -> 1)
      //OR
      import scala.collection._
      (List("a", "b", "b", "b", "a", "c").groupBy(x ⇒ x)
        .foldLeft(mutable.Map[String, Int]().withDefaultValue(0))((acc, item) ⇒
          acc.+=(item._1 -> item._2.size)
        )) should be equalTo mutable.Map("a" -> 2, "b" -> 3, "c" -> 1)
    }
  }

  "Monad" should {
    "bind" in {
      val M = Monad[Option]
      def func(m: Int): Option[Int] = Some(m * 5)
      (M.point(12) >>= func) should be equalTo Some(60)
    }

    case class User(id: Long, name: String)
    case class Address(street: String)

    def addressFromUserId[M[_]: Monad](getUserById: Long ⇒ M[User],
                                       getAddressByUser: User ⇒ M[Address])(id: Long): M[Address] = {
      for {
        user ← getUserById(id)
        address ← getAddressByUser(user)
      } yield address
    }

    "abstract result aspect effect with Option" in {
      (addressFromUserId[Option](
        { id ⇒ Some(User(id, "Sherlock")) },
        { user ⇒ Some(Address("Baker street")) })(99l)) should be equalTo Some(Address("Baker street"))
    }

    "abstract result aspect effect with Id" in {
      val getUserById: Long ⇒ Id[User] =
        id ⇒ User(id, "Sherlock")

      val gerAddressByUser: User ⇒ Id[Address] =
        id ⇒ Address("Baker street")

      addressFromUserId[Id](getUserById, gerAddressByUser)(99l) should be equalTo Address("Baker street")
    }

    "abstract latency effect with Future" in {
      val getUserById: Long ⇒ Future[User] =
        id ⇒ Future(User(id, "Sherlock"))

      val gerAddressByUser: User ⇒ Future[Address] =
        id ⇒ Future(Address("Baker street"))

      import scala.concurrent.Await
      Await.result(addressFromUserId[Future](getUserById, gerAddressByUser)(99l),
        new FiniteDuration(1, TimeUnit.SECONDS)) should be equalTo Address("Baker street")
    }

    "abstract latency effect context Task" in {
      val getUserById: Long ⇒ Task[User] =
        id ⇒ Task.now(User(id, "Sherlock"))

      val gerAddressByUser: User ⇒ Task[Address] =
        id ⇒ Task.now(Address("Baker street"))

      (addressFromUserId[Task](getUserById, gerAddressByUser)(99l))
        .run should be equalTo Address("Baker street")
    }

    "abstract Process" in {
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

      (addressFromUserId[({ type f[x] = Process[Task, x] })#f](getUserById, gerAddressByUser)(99l))
        .runLog.run should be equalTo Vector(Address("Baker street"),
          Address("Baker street !"), Address("Baker street ! !"))
    }
  }
}