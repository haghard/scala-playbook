package tree

import java.util.concurrent.{ ExecutorService, Executor, Executors }

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.{ Nondeterminism, Monoid }
import scalaz.concurrent.Task
import scalaz._
import Scalaz._

class BTreeSpec2 extends Specification {
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
    "foldMapPar" in {
      implicit val M0 = monoidPar(M)

      val m: (Int) ⇒ Int =
        x ⇒ {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 300))
          x
        }

      val b = Branch(
        Branch(leaf(1), Branch(leaf(2), leaf(3))),
        Branch(leaf(4), Branch(leaf(5), Branch(leaf(6), leaf(7)))))

      /*
                    |
             _______|_______
            |               |
         +--+--+        +---+-----+
         |     |        |         |
         1  +--+--+     4         5
            |     |               |
            2     3            +--+--+
                               |     |
                               6     7
    */

      foldMapPar(b)(m).run should be equalTo (1 + 2 + 3 + 4 + 5 + 6 + 7)
    }
  }
}

sealed trait Tree[+T]
case class Leaf[T](value: T) extends Tree[T]
case class Branch[T](left: Tree[T], right: Tree[T]) extends Tree[T]

object TreeF extends Foldable0[Tree] {
  private val logger = Logger.getLogger("tree")
  implicit val S = Executors.newFixedThreadPool(2, new NamedThreadFactory("tree-folder"))

  def leaf[T](value: T) = Leaf[T](value)

  override def foldMap[A, B](as: Tree[A])(f: A ⇒ B)(implicit M: Monoid[B]): B =
    as match {
      case Leaf(a)      ⇒ f(a)
      case Branch(l, r) ⇒ (M append (foldMap(l)(f)(M), foldMap(r)(f)(M)))
    }

  override def foldLeft[A, B](as: Tree[A])(z: B)(f: (B, A) ⇒ B): B = as match {
    case Leaf(a)      ⇒ f(z, a)
    case Branch(l, r) ⇒ foldLeft(r)(foldLeft(l)(z)(f))(f)
  }

  override def foldRight[A, B](as: Tree[A])(z: B)(f: (A, B) ⇒ B): B = as match {
    case Leaf(a)      ⇒ f(a, z)
    case Branch(l, r) ⇒ foldRight(l)(foldRight(r)(z)(f))(f)
  }

  override def foldMapPar[A, B](fa: Tree[A])(f: (A) ⇒ B)(implicit M: Monoid[B]): Task[B] =
    Task.now(fa).flatMap(foldMap(_)(value ⇒ Task.delay(f(value)))(monoidPar(M)))

  implicit def monoidPar[A](M: Monoid[A]): Monoid[Task[A]] = new Monoid[Task[A]] {
    private val ND = Nondeterminism[Task]

    override val zero = Task.delay(M.zero)

    override def append(a: Task[A], b: ⇒ Task[A]): Task[A] =
      for {
        r ← ND.nmap2(Task.fork(a)(S), Task.fork(b)(S)) { (l, r) ⇒
          logger.info(s" op($l,$r)")
          M.append(l, r)
        }
      } yield r
  }

  /**
   *
   *
   */
  override def foldMap2[T, A](fa: Option[Tree[T]])(f: (T) ⇒ A)(implicit m: Monoid[A]): A = ???
}
