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

    def plus[F[_]](M: F[T])(implicit f: Functor[F], num: Numeric[T]): F[T] =
      f.map(M)(num.plus(_, v))

    def minis[F[_]](M: F[T])(implicit f: Functor[F], num: Numeric[T]): F[T] =
      f.map(M)(num.minus(_, v))

    def multiply[F[_]](M: F[T])(implicit f: Functor[F], num: Numeric[T]): F[T] =
      f.map(M)(num.times(_, v))

    def plusM[F[_]: Monad](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.plus(_, v))

    def minisM[F[_]: Monad](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.minus(_, v))

    def multiplyM[F[_]: Monad](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.times(_, v))
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
      import ScalazTypeClassesSpec._

      "hello".some.map(_.length).get === 5

      67.plus(10.some) === Some(77)
      1.minis(List(7, 2)) === List(6, 1)

      67.plusM(10.some) === Some(77)
      1.minisM(List(7, 2)) === List(6, 1)
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
  }
}