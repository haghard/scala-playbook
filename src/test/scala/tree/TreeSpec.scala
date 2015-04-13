package tree

import java.util.concurrent.Executors

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scalaz.{ Nondeterminism, Monoid }
import scalaz.concurrent.Task
import scalaz._
import Scalaz._

class TreeSpec extends Specification {
  import TreeF._

  implicit val M = scalaz.Monoid[Int]

  "tree" should {
    "foldMap" in {

      val b = Branch(
        Branch(Leaf(1), Branch(Leaf(4), Leaf(8))),
        Branch(Leaf(3), Leaf(4))
      )

      foldMap(b)(identity)(M) should be equalTo 20
    }
  }

  "tree" should {
    "folds" in {

      val b = Branch(
        Branch(Leaf(1), Branch(Leaf(4), Leaf(8))),
        Branch(Leaf(3), Leaf(4)))

      foldLeft(b)(0)(_ + _) should be equalTo 20
      foldRight(b)(0)(_ + _) should be equalTo 20
    }
  }

  "tree" should {
    "foldMapTask" in {
      implicit val M0 = monoidT(M)

      val m: (Int) ⇒ Int =
        x ⇒ {
          Thread.sleep(10)
          x
        }

      val b = Branch(
        Branch(Leaf(1), Branch(Leaf(4), Leaf(8))),
        Branch(Leaf(3), Leaf(4)))

      foldMapTask(b)(m).run should be equalTo 20
    }
  }
}

sealed trait Tree[+T]
case class Leaf[T](value: T) extends Tree[T]
case class Branch[T](left: Tree[T], right: Tree[T]) extends Tree[T]

object TreeF extends Foldable0[Tree] {
  private val logger = Logger.getLogger("tree")

  override def foldMap[A, B](as: Tree[A])(f: A ⇒ B)(implicit mb: Monoid[B]): B =
    as match {
      case Leaf(a)      ⇒ f(a)
      case Branch(l, r) ⇒ mb.append(foldMap(l)(f)(mb), foldMap(r)(f)(mb))
    }

  override def foldLeft[A, B](as: Tree[A])(z: B)(f: (B, A) ⇒ B): B = as match {
    case Leaf(a)      ⇒ f(z, a)
    case Branch(l, r) ⇒ foldLeft(r)(foldLeft(l)(z)(f))(f)
  }

  override def foldRight[A, B](as: Tree[A])(z: B)(f: (A, B) ⇒ B): B = as match {
    case Leaf(a)      ⇒ f(a, z)
    case Branch(l, r) ⇒ foldRight(l)(foldRight(r)(z)(f))(f)
  }

  override def foldMapTask[A, B](fa: Tree[A])(f: (A) ⇒ B)(implicit m: Monoid[B]): Task[B] = {
    Task delay (fa) flatMap { tree ⇒
      foldMap(tree)(element ⇒ Task.delay {
        f(element)
      })(monoidT(m))
    }
  }

  val executor = Executors.newFixedThreadPool(2, new NamedThreadFactory("tree-folder"))

  implicit def monoidT[A](m: Monoid[A]): Monoid[Task[A]] = new Monoid[Task[A]] {

    private val ND = Nondeterminism[Task]

    override def zero = Task.delay(m.zero)

    override def append(a: Task[A], b: ⇒ Task[A]): Task[A] =
      for {
        r ← ND.nmap2(Task.fork(a)(executor), Task.fork(b)(executor)) { (l, r) ⇒
          logger.info(s" op($l,$r)")
          m.append(l, r)
        }
      } yield r
  }

  /**
   *
   *
   */
  override def foldMap2[T, A](fa: Option[Tree[T]])(f: (T) ⇒ A)(implicit m: Monoid[A]): A = ???
}