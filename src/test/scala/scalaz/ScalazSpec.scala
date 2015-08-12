package scalaz

import scalaz._
import Scalaz._
import scala.language.higherKinds
import org.specs2.mutable.Specification

import scalaz.concurrent.Task

object ScalazSpec {

  implicit class NumericAlgebraOps[T](val v: T) extends AnyVal {

    def plus[F[_]: Functor](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.plus(_, v))

    def minis[F[_]: Functor](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.minus(_, v))

    def multiply[F[_]: Functor](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.times(_, v))

    def plusM[F[_]: Monad](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.plus(_, v))

    def minisM[F[_]: Monad](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.minus(_, v))

    def multiplyM[F[_]: Monad](M: F[T])(implicit num: Numeric[T]): F[T] =
      M.map(num.times(_, v))
  }
}

class ScalazSpec extends Specification {

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
    import ScalazSpec._

    "digit compose with Functor" in {
      "hello".some.map(_.length).get === 5

      67.plus(10.some) === Some(77)
      1.minis(List(7, 2)) === List(6, 1)

      67.plusM(10.some) === Some(77)
      1.minisM(List(7, 2)) === List(6, 1)
    }

    "Functors composition" in {
      val F = Functor[List] compose Functor[Option]
      F.map(List(Some(1), None, Some(2)))(_ + 1) === List(Some(2), None, Some(3))
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

    "Semigroup ops" in {
      1 |+| 2 |+| 6 should be equalTo 9
      "Hello".some |+| None |+| "World".some should be equalTo "HelloWorld".some
    }

    "Semigroup(Monoid) and ApplicativeBuilder relations" in {
      (1.some |+| 2.some) === (1.some |@| 2.some)(_ + _)
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

    "state traverseU" in {
      val src = List(1, 2, 4, 5)
      src.traverseU(x ⇒ scalaz.State { y: Int ⇒
        (y + x, x)
      }).run(0)._1 should be equalTo 12
    }

    "state traverseS" in {
      val src = List(1, 2, 4, 5)
      src.traverseS(x ⇒ scalaz.State { y: Int ⇒ (y + x, x) })
        .run(0)._1 should be equalTo 12
    }
  }

  "Option as Apply" should {
    "Apply  <*>, *>, <* operator" in {
      //Usefull when you want return error when it occurs

      val s: scala.Option[Int] = scalaz.Scalaz.none
      1.point[Option].map(_ + 2) === Some(3)

      //<* take left hand side and discard right if they both successes
      1.some <* 2.some === Some(1)

      //*> take right hand side and discard left if they both successes
      1.some *> 2.some === Some(2)

      //take left
      scalaz.Scalaz.none[Int] <* 1.some === None

      //take left if they are both successes, but they are not
      1.some <* scalaz.Scalaz.none[Int] === None
      //*> take right if they are both successes, but they are not
      scalaz.Scalaz.none[Int] *> 1.some === None

      //take left if they are both successes, but they are not
      scalaz.Scalaz.none[Int] *> 2.some == Some(1)

      val f = { x: Int ⇒ x + 3 }.point[Option]
      val f1 = 9.some <*> ((x: Int, y: Int) ⇒ x + y).curried.some

      9.some.<*>(f) === Some(12)
      3.some.<*>(f1) === Some(12)
    }

    "Applicative builders" in {
      ^(3.some, 5.some)(_ + _) === Some(8)
      //or
      (3.some |@| 5.some)(_ + _) === Some(8)

      ^(3.some, scalaz.Scalaz.none[Int])(_ + _) === None

      (List("hi", "hello") |@| List("?", "!"))(_ + _) === List("hi?", "hi!", "hello?", "hello!")
    }

    "Curried Type Parameters" in {
      //Construct Applicative/Monad with type and value
      final class WrapHelper[F[_]] {
        def apply[A](a: A)(implicit ev: Applicative[F]): F[A] =
          ev.point(a)
      }

      /*
      It works fine, but you must supply both type arguments even
      though the second is inferable.
      def wrap[F[_]: Applicative, A](a: A): F[A] =
        Applicative[F].point(a)
        */

      def wrap[F[_]] = new WrapHelper[F]

      import scala.language.reflectiveCalls
      def wrapA[F[_]] = new {
        def apply[A](a: A)(implicit ev: Applicative[F]): F[A] =
          ev.point(a)
      }

      def wrapM[F[_]] = new {
        def apply[A](a: A)(implicit ev: Monad[F]): F[A] =
          ev.point(a)
      }

      wrap[List](1) === List(1)
      wrap[Option](1) === Some(1)

      wrapA[List](1) === List(1)
      wrapA[Option](1) === Some(1)

      wrapM[List](1) === List(1)
      wrapM[Option](1) === Some(1)

    }

    "TraverseUsage" should {

      "sequence" in {
        //given a Traverse[F] and Applicative[G] turns F[G[A]] into G[F[A]]
        val list: List[Option[Int]] = List(1.some, 2.some, 3.some, 4.some)
        Traverse[List].sequence(list) === Some(List(1, 2, 3, 4))

        //In the case of the Option applicative, if any of the values are None the result of the entire computation is None
        val list2: List[Option[Int]] = List(1.some, 2.some, scalaz.Scalaz.none[Int], 4.some)
        Traverse[List].sequence(list2) === None
      }

      "traverse" in {
        List(1.some, 2.some).traverse(_.map(_ * 2)) === Some(List(2, 4))
      }

      "sequence traverse relation" in {
        val step: Int ⇒ Option[Int] = (x ⇒ if (x < 5) (x * 2).some else scalaz.Scalaz.none[Int])
        val l = List(1, 2, 3, 4)
        l.traverse(step) === l.map(step).sequence

        //the entire computation is None
        val l0 = l :+ 5
        l0.traverse(step) === l0.map(step).sequence

      }

      "sequenceU for collect errors" in {
        val validations: Vector[ValidationNel[String, Int]] = Vector(1.success, "failure2".failureNel, 3.success, "failure4".failureNel)
        val result = validations.sequenceU
        result === NonEmptyList("failure2", "failure4").failure[Vector[Int]]
      }
    }

    "TraverseS" in {
      val r = List(1, 2).traverseS { x: Int ⇒
        State { y: Int ⇒ ((x * y), x) }
      }

      val ans = r.run(5)
      ans._1 === 10 //(1*5)*2
      ans._2 === List(1, 2)
    }
  }

  "Free monad with Evaluator" should {

    sealed trait Instruction[+T]
    case class PrintLine(prompt: String) extends Instruction[String]
    case class ReadLine(msg: String) extends Instruction[String]

    trait Evaluator[F[_]] { self ⇒

      def evaluate[T](given: F[T]): T

      def ~>[G[_]: Monad]: (F ~> G) =
        Evaluator.nat(self, scalaz.Monad[G])
    }

    object Evaluator {
      def apply[F[_]: Evaluator] = implicitly[Evaluator[F]]

      private[Evaluator] def nat[F[_], G[_], E <: Evaluator[F]](implicit E: E, G: Monad[G]) = new (F ~> G) {
        override def apply[A](given: F[A]): G[A] =
          G.pure(E.evaluate(given))
      }
    }

    object EvaluatorLogic extends Evaluator[Instruction] {
      override def evaluate[T](given: Instruction[T]): T = given match {
        case PrintLine(text) ⇒ { println(text); "" }
        case ReadLine(msg)   ⇒ s"${msg}99" //StdIn.readLine(m)
      }
    }

    def request = Free.liftFC(PrintLine("Please enter the size: "))
    def respond(m: String) = Free.liftFC(ReadLine(m))

    "run" in {
      val expected = "size:99"
      val program = for {
        d ← request
        size ← respond("size:")
      } yield size

      val sId = Free.runFC(program)(EvaluatorLogic.~>[Id])
      val sTask = Free.runFC(program)(EvaluatorLogic.~>[Task]).run

      sId === expected
      sTask === expected
    }
  }
}